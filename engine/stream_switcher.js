const debug = require('debug')('engine-streamSwitcher');
const crypto = require('crypto');
const fetch = require('node-fetch');
const { AbortController } = require('abort-controller');

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

class StreamSwitcher {
  constructor(config) {
    this.sessionId =  crypto.randomBytes(20).toString('hex');
    this.useDemuxedAudio = false;
    this.cloudWatchLogging = false;
    this.streamTypeLive = false;
    this.streamSwitchManager = null;
    this.eventId = null;
    this.working = false;

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
    if (!this.streamSwitchManager) {
      debug(`[${this.sessionId}]: No streamSwitchManager available`);
      return false;
    }
    if (this.working) {
      debug(`[${this.sessionId}]: streamSwitcher is currently busy`);
      return null;
    }

    // Filter out schedule objects from the past
    const tsNow = Date.now();
    const strmSchedule = this.streamSwitchManager.getSchedule();
    const schedule = strmSchedule.filter((obj) => obj.end_time > tsNow);
    if (schedule.length === 0 && this.streamTypeLive) {
      await this._initSwitching(
        SwitcherState.LIVE_TO_V2L,
        session,
        sessionLive,
        null
      );
      return false;
    }
    if (schedule.length === 0) {
      this.eventId = null;
      return false;
    }
    const scheduleObj = schedule[0];
    if (tsNow < scheduleObj.start_time) {
      if (this.streamTypeLive) {
        await this._initSwitching(
          SwitcherState.LIVE_TO_V2L,
          session,
          sessionLive,
          null
        );
      }
      this.eventId = null;
      return false;
    }
    const validURI = await this._validURI(scheduleObj.uri);
    if (!validURI) {
      debug(`[${this.sessionId}]: Unreachable URI`);
      if (this.streamTypeLive) {
        debug(`[${this.sessionId}]: Switching back to VOD2Live due to unreachable URI`);
        await this._initSwitching(
          SwitcherState.LIVE_TO_V2L,
          session,
          sessionLive,
          null
        );
        return false;
      }
      return false;
    }
    if (this.streamTypeLive) {
      if (tsNow >= scheduleObj.start_time && this.eventId !== scheduleObj.eventId) {
        if (scheduleObj.type === StreamType.LIVE) {
          await this._initSwitching(
            SwitcherState.LIVE_TO_LIVE,
            session,
            sessionLive,
            scheduleObj
          );
          return true;
        }
        await this._initSwitching(
          SwitcherState.LIVE_TO_VOD,
          session,
          sessionLive,
          scheduleObj
        );
        return false;
      }
    }
    if (tsNow >= scheduleObj.start_time && tsNow < scheduleObj.end_time) {
      if (scheduleObj.type === StreamType.LIVE) {
        if (!this.streamTypeLive) {
          await this._initSwitching(
            SwitcherState.V2L_TO_LIVE,
            session,
            sessionLive,
            scheduleObj
          );
          return true;
        }
        return true;
      }
      if (!this.streamTypeLive) {
        if(!scheduleObj.duration) {
          debug(`[${this.sessionId}]: Cannot switch VOD no duration specified for schedule item: [${scheduleObj.assetId}]`);
          return false;
        }
        if (this.eventId !== scheduleObj.eventId) {
          await this._initSwitching(
            SwitcherState.V2L_TO_VOD,
            session,
            sessionLive,
            scheduleObj
          );
        }
        return false;
      }
    }
  }

  async _initSwitching(state, session, sessionLive, scheduleObj) {
    let liveCounts = 0;
    let liveSegments = null;
    let currVodCounts = 0;
    let currLiveCounts = 0;
    let currVodSegments = null;
    let eventSegments = null;

    switch (state) {
      case SwitcherState.V2L_TO_LIVE:
        this.working = true;
        this.streamTypeLive = true;
        this.eventId = scheduleObj.eventId;
        currVodSegments = await session.getCurrentMediaSequenceSegments();
        currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();

        await sessionLive.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
        await sessionLive.setCurrentMediaSequenceSegments(currVodSegments);
        await sessionLive.setLiveUri(scheduleObj.uri);
        this.working = false;
        debug(`[${this.sessionId}]: [ Switching from V2L->LIVE ]`);
        break;
      case SwitcherState.V2L_TO_VOD:
        this.working = true;
        this.eventId = scheduleObj.eventId;
        currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
        eventSegments = await session.getTruncatedVodSegments(scheduleObj.uri, (scheduleObj.duration / 1000));

        await session.setCurrentMediaAndDiscSequenceCount((currVodCounts.mediaSeq - 1), currVodCounts.discSeq);
        await session.setCurrentMediaSequenceSegments(eventSegments, true);
        this.working = false;
        debug(`[${this.sessionId}]: [ Switching from V2L->VOD ]`);
        break;
      case SwitcherState.LIVE_TO_V2L:
        this.working = true;
        this.eventId = null;
        this.streamTypeLive = false;
        liveSegments = await sessionLive.getCurrentMediaSequenceSegments();
        liveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
        sessionLive.resetSession();

        if(scheduleObj && !scheduleObj.duration) {
          debug(`[${this.sessionId}]: Cannot switch VOD no duration specified for schedule item: [${scheduleObj.assetId}]`);
        }
        await session.setCurrentMediaAndDiscSequenceCount(liveCounts.mediaSeq, liveCounts.discSeq);
        await session.setCurrentMediaSequenceSegments(liveSegments);
        this.working = false;
        debug(`[${this.sessionId}]: Switching from LIVE->V2L`);
        break;
      case SwitcherState.LIVE_TO_VOD:
        this.working = true;
        this.streamTypeLive = false;
        liveSegments = await sessionLive.getCurrentMediaSequenceSegments();
        liveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
        sessionLive.resetSession();

        this.eventId = scheduleObj.eventId;
        eventSegments = await session.getTruncatedVodSegments(scheduleObj.uri, (scheduleObj.duration / 1000));

        await session.setCurrentMediaAndDiscSequenceCount(liveCounts.mediaSeq, liveCounts.discSeq);
        await session.setCurrentMediaSequenceSegments(liveSegments);
        await session.setCurrentMediaSequenceSegments(eventSegments, true);
        this.working = false;
        debug(`[${this.sessionId}]: Switching from LIVE->VOD`);
        break;
      case SwitcherState.LIVE_TO_LIVE:
        this.working = true;
        this.eventId = scheduleObj.eventId;
        eventSegments = await sessionLive.getCurrentMediaSequenceSegments();
        currLiveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
        sessionLive.resetSession();

        await sessionLive.setCurrentMediaAndDiscSequenceCount((currLiveCounts.mediaSeq + 1), currLiveCounts.discSeq);
        await sessionLive.setCurrentMediaSequenceSegments(eventSegments);
        await sessionLive.setLiveUri(scheduleObj.uri);
        this.working = false;
        debug(`[${this.sessionId}]: Switching from LIVE->LIVE`);
        break;
      default:
        debug(`[${this.sessionId}]: SwitcherState [${state}] not implemented`);
        this.working = false;
        this.streamTypeLive = false;
        break;
    }
  }

  async _validURI(uri) {
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort();
    }, 3000);
    try {
      const online = await fetch(uri, {signal: controller.signal });
      if (online.status >= 200 && online.status < 300) {
        return true;
      }
      return false;
    } catch (err) {
      return false;
    } finally {
      clearTimeout(timeout);
    }
  }
}

module.exports = StreamSwitcher;
