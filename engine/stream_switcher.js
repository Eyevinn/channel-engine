const debug = require("debug")("engine-stream-switcher");
const crypto = require("crypto");
const fetch = require("node-fetch");
const { AbortController } = require("abort-controller");

const SwitcherState = Object.freeze({
  V2L_TO_LIVE: 1,
  V2L_TO_VOD: 2,
  LIVE_TO_V2L: 3,
  LIVE_TO_LIVE: 4,
  LIVE_TO_VOD: 5,
});
const StreamType = Object.freeze({
  LIVE: 1,
  VOD: 2,
});

const FAIL_TIMEOUT = 3000;
const MAX_FAILS = 3;

class StreamSwitcher {
  constructor(config) {
    this.sessionId = crypto.randomBytes(20).toString("hex");
    this.useDemuxedAudio = false;
    this.cloudWatchLogging = false;
    this.streamTypeLive = false;
    this.streamSwitchManager = null;
    this.eventId = null;
    this.working = false;
    this.timeDiff = null;
    this.abortTimeStamp = null;

    if (config) {
      if (config.sessionId) {
        this.sessionId = config.sessionId;
      }
      if (config.useDemuxedAudio) {
        this.useDemuxedAudio = true;
      }
      if (config.cloudWatchMetrics) {
        this.cloudWatchLogging = true;
      }
      if (config.streamSwitchManager) {
        this.streamSwitchManager = config.streamSwitchManager;
      }
    }
  }

  getEventId() {
    return this.eventId;
  }

  /**
   *
   * @param {Session} session The VOD2Live Session object.
   * @param {SessionLive} sessionLive The Live Session object.
   * @returns A bool, true if streamSwitchManager contains current Live event to be played else false.
   */
  async streamSwitcher(session, sessionLive) {
    let status = null;
    if (!this.streamSwitchManager) {
      debug(`[${this.sessionId}]: No streamSwitchManager available`);
      return false;
    }
    if (this.working) {
      debug(`[${this.sessionId}]: streamSwitcher is currently busy`);
      return null;
    }
    // Handle Complete Storage Reset
    let sessionState = await session._sessionState.get("state");
    if (sessionState === 1 || !sessionState) {
      this.working = true;
      sessionLive.waitForPlayhead = false;
      sessionLive.allowedToSet = false; // only have effect on leader
      await sessionLive.resetSession();
      await sessionLive.resetLiveStoreAsync(0);
      this.working = false;
      this.abortTimeStamp = Date.now() + 20 * 1000; // 30 second V2L->LIVE timeout
      if (this.streamTypeLive) {
        debug(`[${this.sessionId}]: [ Ending LIVE Abruptly, Going to -> V2L ]`);
        this.streamTypeLive = false;
      }
      debug(`[${this.sessionId}]: StreamSwitcher reacting to Full Store Reset`);
      return false; // Go to V2L feed
    }

    // Filter out schedule objects from the past
    const tsNow = Date.now();
    const strmSchedule = await this.streamSwitchManager.getSchedule(this.sessionId);
    const schedule = strmSchedule.filter((obj) => obj.end_time > tsNow);
    if (schedule.length === 0 && this.streamTypeLive) {
      status = await this._initSwitching(
        SwitcherState.LIVE_TO_V2L,
        session,
        sessionLive,
        null
      );
      return status;
    }
    if (schedule.length === 0) {
      this.eventId = null;
      return false;
    }
    const scheduleObj = schedule[0];
    this.timeDiff = scheduleObj;
    if (tsNow < scheduleObj.start_time) {
      if (this.streamTypeLive) {
        status = await this._initSwitching(
          SwitcherState.LIVE_TO_V2L,
          session,
          sessionLive,
          null
        );
        return status;
      }
      this.eventId = null;
      return false;
    }

    let tries = 0;
    let validURI = false;
    while (!validURI && tries < MAX_FAILS) {
      debug(`[${this.sessionId}]: Switcher is validating Master URI... (tries left=${MAX_FAILS - tries})`);
      validURI = await this._validURI(scheduleObj.uri);
      tries++;
    }
    if (!validURI) {
      debug(`[${this.sessionId}]: Unreachable URI: [${scheduleObj.uri}]`);
      if (this.streamTypeLive) {
        debug(`[${this.sessionId}]: Abort Live Stream! Switching back to VOD2Live due to unreachable URI`);
        this.abortTimeStamp = Date.now();
        status = await this._initSwitching(SwitcherState.LIVE_TO_V2L, session, sessionLive, null);
        return status;
      }
      return false;
    }
    debug(`[${this.sessionId}]: ....Master URI -> VALID`);

    if (this.abortTimeStamp && tsNow - this.abortTimeStamp <= 10000) {
      // If we have a valid URI and no more than 10 seconds have passed since switching from Live->V2L.
      // Stay on V2L to give live sessionLive some time to prepare before switching back to live.
      debug(`[${this.sessionId}]: Waiting [${10000 - (tsNow - this.abortTimeStamp)}ms] before switching back to Live due to unreachable URI`);
      return false;
    }
    this.abortTimeStamp = null;

    if (this.streamTypeLive) {
      if (
        tsNow >= scheduleObj.start_time &&
        this.eventId !== scheduleObj.eventId
      ) {
        if (scheduleObj.type === StreamType.LIVE) {
          status = await this._initSwitching(
            SwitcherState.LIVE_TO_LIVE,
            session,
            sessionLive,
            scheduleObj
          );
          return status;
        }
        status = await this._initSwitching(
          SwitcherState.LIVE_TO_VOD,
          session,
          sessionLive,
          scheduleObj
        );
        return status;
      }
    }
    if (
      tsNow >= scheduleObj.start_time &&
      tsNow < scheduleObj.end_time &&
      scheduleObj.end_time - tsNow > 10000
    ) {
      if (scheduleObj.type === StreamType.LIVE) {
        if (!this.streamTypeLive) {
          status = await this._initSwitching(
            SwitcherState.V2L_TO_LIVE,
            session,
            sessionLive,
            scheduleObj
          );
          return status;
        }
        return true;
      }
      if (!this.streamTypeLive) {
        if (!scheduleObj.duration) {
          debug(`[${this.sessionId}]: Cannot switch VOD no duration specified for schedule item: [${scheduleObj.assetId}]`);
          return false;
        }
        if (this.eventId !== scheduleObj.eventId) {
          status = await this._initSwitching(
            SwitcherState.V2L_TO_VOD,
            session,
            sessionLive,
            scheduleObj
          );
          return status;
        }
        return false;
      }
    }
  }

  async _initSwitching(state, session, sessionLive, scheduleObj) {
    this.working = true;
    const RESET_DELAY = 5000;
    let liveCounts = 0;
    let liveSegments = null;
    let currVodCounts = 0;
    let currLiveCounts = 0;
    let currVodSegments = null;
    let eventSegments = null;
    let liveUri = null;

    switch (state) {
      case SwitcherState.V2L_TO_LIVE:
        try {
          debug(`[${this.sessionId}]: [ INIT Switching from V2L->LIVE ]`);
          this.eventId = scheduleObj.eventId;
          currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
          currVodSegments = await session.getCurrentMediaSequenceSegments();

          // In risk that the SL-playhead might have updated some data after
          // we reset last time... we should Reset SessionLive before sending new data.
          await sessionLive.resetLiveStoreAsync(0);
          await sessionLive.setCurrentMediaAndDiscSequenceCount(
            currVodCounts.mediaSeq,
            currVodCounts.discSeq
          );
          await sessionLive.setCurrentMediaSequenceSegments(currVodSegments);
          liveUri = await sessionLive.setLiveUri(scheduleObj.uri);

          if (!liveUri) {
            debug(`[${this.sessionId}]: [ ERROR Switching from V2L->LIVE ]`);
            this.working = false;
            this.eventId = null;
            return false;
          }

          this.working = false;
          this.streamTypeLive = true;
          debug(`[${this.sessionId}]: [ Switched from V2L->LIVE ]`);
          return true;
        } catch (err) {
          debug(`[${this.sessionId}]: [ ERROR Switching from V2L->LIVE ]`);
          throw new Error(err);
        }

      case SwitcherState.V2L_TO_VOD:
        try {
          debug(`[${this.sessionId}]: [ INIT Switching from V2L->VOD ]`);
          this.eventId = scheduleObj.eventId;
          currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
          eventSegments = await session.getTruncatedVodSegments(
            scheduleObj.uri,
            scheduleObj.duration / 1000
          );

          if (!eventSegments) {
            debug(`[${this.sessionId}]: [ ERROR Switching from V2L->VOD ]`);
            this.working = false;
            this.eventId = null;
            return false;
          }

          await session.setCurrentMediaAndDiscSequenceCount(
            currVodCounts.mediaSeq,
            currVodCounts.discSeq
          );
          await session.setCurrentMediaSequenceSegments(eventSegments, 0, true);

          this.working = false;
          debug(`[${this.sessionId}]: [ Switched from V2L->VOD ]`);
          return false;
        } catch (err) {
          debug(`[${this.sessionId}]: [ ERROR Switching from V2L->VOD ]`);
          throw new Error(err);
        }
      case SwitcherState.LIVE_TO_V2L:
        try {
          debug(`[${this.sessionId}]: [ INIT Switching from LIVE->V2L ]`);
          this.eventId = null;
          liveSegments = await sessionLive.getCurrentMediaSequenceSegments();
          liveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();

          await sessionLive.resetSession();
          sessionLive.resetLiveStoreAsync(RESET_DELAY); // In parallel

          if (scheduleObj && !scheduleObj.duration) {
            debug(`[${this.sessionId}]: Cannot switch VOD. No duration specified for schedule item: [${scheduleObj.assetId}]`);
          }

          if (this._isEmpty(liveSegments.currMseqSegs)) {
            this.working = false;
            this.streamTypeLive = false;
            debug(`[${this.sessionId}]: [ Switched from LIVE->V2L ]`);
            return false;
          }

          await session.setCurrentMediaSequenceSegments(
            liveSegments.currMseqSegs,
            liveSegments.segCount
          );
          await session.setCurrentMediaAndDiscSequenceCount(
            liveCounts.mediaSeq,
            liveCounts.discSeq
          );

          this.working = false;
          this.streamTypeLive = false;
          debug(`[${this.sessionId}]: [ Switched from LIVE->V2L ]`);
          return false;
        } catch (err) {
          debug(`[${this.sessionId}]: [ ERROR Switching from LIVE->V2L ]`);
          throw new Error(err);
        }
      case SwitcherState.LIVE_TO_VOD:
        try {
          debug(`[${this.sessionId}]: INIT Switching from LIVE->VOD`);
          // TODO: Not yet fully tested/supported
          this.eventId = scheduleObj.eventId;
          liveSegments = await sessionLive.getCurrentMediaSequenceSegments();
          liveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
          await sessionLive.resetSession();
          sessionLive.resetLiveStoreAsync(RESET_DELAY); // In parallel

          eventSegments = await session.getTruncatedVodSegments(
            scheduleObj.uri,
            scheduleObj.duration / 1000
          );
          if (!eventSegments) {
            debug(`[${this.sessionId}]: [ ERROR Switching from LIVE->VOD ]`);
            this.streamTypeLive = false;
            this.working = false;
            this.eventId = null;
            return false;
          }

          await session.setCurrentMediaAndDiscSequenceCount(
            liveCounts.mediaSeq - 1,
            liveCounts.discSeq - 1
          );
          await session.setCurrentMediaSequenceSegments(
            liveSegments.currMseqSegs,
            liveSegments.segCount
          );
          await session.setCurrentMediaSequenceSegments(eventSegments, 0, true);

          this.working = false;
          this.streamTypeLive = false;
          debug(`[${this.sessionId}]: Switched from LIVE->VOD`);
          return false;
        } catch (err) {
          debug(`[${this.sessionId}]: [ ERROR Switching from LIVE->VOD ]`);
          throw new Error(err);
        }
      case SwitcherState.LIVE_TO_LIVE:
        try {
          debug(`[${this.sessionId}]: INIT Switching from LIVE->LIVE`);
          // TODO: Not yet fully tested/supported
          this.eventId = scheduleObj.eventId;
          eventSegments = await sessionLive.getCurrentMediaSequenceSegments();
          currLiveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();

          await sessionLive.resetSession();
          await sessionLive.resetLiveStoreAsync(0);

          await sessionLive.setCurrentMediaAndDiscSequenceCount(
            currLiveCounts.mediaSeq,
            currLiveCounts.discSeq
          );
          await sessionLive.setCurrentMediaSequenceSegments(
            eventSegments.currMseqSegs
          );
          liveUri = await sessionLive.setLiveUri(scheduleObj.uri);

          if (!liveUri) {
            debug(`[${this.sessionId}]: [ ERROR Switching from LIVE->LIVE ]`);
            this.streamTypeLive = false;
            this.working = false;
            this.eventId = null;
            return false;
          }

          this.working = false;
          debug(`[${this.sessionId}]: Switched from LIVE->LIVE`);
          return true;
        } catch (err) {
          debug(`[${this.sessionId}]: [ ERROR Switching from LIVE->LIVE ]`);
          throw new Error(err);
        }
      default:
        debug(`[${this.sessionId}]: SwitcherState [${state}] not implemented`);
        this.streamTypeLive = false;
        this.working = false;
        this.eventId = null;
        return false;
    }
  }

  _isEmpty(obj) {
    if (!obj) {
      return true;
    }
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        return false;
      }
    }
    return true;
  }

  async _validURI(uri) {
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      debug(`[${this.sessionId}]: Request Timeout @ ${uri}`);
      controller.abort();
    }, FAIL_TIMEOUT);
    try {
      const online = await fetch(uri, { signal: controller.signal });

      if (online.status >= 200 && online.status < 300) {
        return true;
      }
      debug(`[${this.sessionId}]: Failed to validate URI: ${uri}\nERROR! Returned Status Code: ${online.status}`);
      return false;
    } catch (err) {
      debug(`[${this.sessionId}]: Failed to validate URI: ${uri}\nERROR! ${err}`);
      return false;
    } finally {
      clearTimeout(timeout);
    }
  }
}

module.exports = StreamSwitcher;
