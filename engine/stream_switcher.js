const debug = require('debug')('engine-streamSwitcher');
const crypto = require('crypto');
const fetch = require('node-fetch');

const SwitcherState = Object.freeze({
  LIVE_TO_LIVE: 1,
  LIVE_TO_VOD: 2,
  VOD_TO_LIVE: 3,
  VOD_TO_VOD: 4,
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
    // Filter out schedule objects from the past
    const tsNow = Date.now();
    const strmSchedule = this.streamSwitchManager.getSchedule();
    const schedule = strmSchedule.filter((obj) => obj.end_time > tsNow);
    // If no more live streams, and streamType is live switch back to vod2live
    if (schedule.length === 0 && this.streamTypeLive) {
      await this._initSwitching(
        SwitcherState.LIVE_TO_VOD,
        session,
        sessionLive,
        null
      );
      return false;
    }
    if (schedule.length === 0) {
      return false;
    }
    const scheduleObj = schedule[0];
    if (tsNow < scheduleObj.start_time) {
      // We are past the end point for scheduled content
      if (this.streamTypeLive) {
        await this._initSwitching(
          SwitcherState.LIVE_TO_VOD,
          session,
          sessionLive,
          null
        );
      }
      return false;
    }
    // Check if Live URI is ok
    const validURI = await this._validURI(scheduleObj.uri);
    if (!validURI) {
      debug(`[${this.sessionId}]: Unreachable URI`);
      if (this.streamTypeLive) {
        debug(`[${this.sessionId}]: Switching back to VOD2Live due to unreachable URI`);
        await this._initSwitching(
          SwitcherState.LIVE_TO_VOD,
          session,
          sessionLive,
          null
        );
      }
      return false;
    }
    // Case: Back-to-Back events
    if (this.streamTypeLive) {
      if (tsNow >= scheduleObj.start_time && this.eventId !== scheduleObj.eventId) {
        if (scheduleObj.type === StreamType.LIVE) {
          // Live->Live
          await this._initSwitching(
            SwitcherState.LIVE_TO_LIVE,
            session,
            sessionLive,
            scheduleObj
          );
          return true;
        }
        // Live->*VOD
        await this._initSwitching(
          SwitcherState.LIVE_TO_VOD,
          session,
          sessionLive,
          scheduleObj
        );
        return false;
      }
    }
    // Case: We want to run scheduled content
    if (tsNow >= scheduleObj.start_time && tsNow < scheduleObj.end_time) {
      if (scheduleObj.type === StreamType.LIVE) {
        if (!this.streamTypeLive) {
          await this._initSwitching(
            SwitcherState.VOD_TO_LIVE,
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
            SwitcherState.VOD_TO_VOD,
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
    let currVodCounts = 0;
    let currLiveCounts = 0;
    let currVodSegments = null;
    let eventSegments = null;

    switch (state) {
      case SwitcherState.VOD_TO_LIVE:
        this.streamTypeLive = true;
        this.eventId = scheduleObj.eventId;
        currVodSegments = await session.getCurrentMediaSequenceSegments();
        currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();

        await sessionLive.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
        await sessionLive.setCurrentMediaSequenceSegments(currVodSegments);
        await sessionLive.setLiveUri(scheduleObj.uri);
        debug(`[${this.sessionId}]: [ Switching from V2L->LIVE ]`);
        break;
      case SwitcherState.VOD_TO_VOD:
        this.eventId = scheduleObj.eventId;
        currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
        eventSegments = await session.getTruncatedVodSegments(scheduleObj.uri, (scheduleObj.duration / 1000));

        await session.setCurrentMediaAndDiscSequenceCount((currVodCounts.mediaSeq - 1), currVodCounts.discSeq);
        await session.setCurrentMediaSequenceSegments(eventSegments, true);
        debug(`[${this.sessionId}]: [ Switching from V2L->VOD ]`);
        break;
      case SwitcherState.LIVE_TO_VOD:
        this.streamTypeLive = false;
        const liveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
        const liveSegments = await sessionLive.getCurrentMediaSequenceSegments();
        if (!scheduleObj || !scheduleObj.duration) {
          this.eventId = null;
          if(scheduleObj && !scheduleObj.duration) {
            debug(`[${this.sessionId}]: Cannot switch VOD no duration specified for schedule item: [${scheduleObj.assetId}]`);
          }
          await session.setCurrentMediaAndDiscSequenceCount(liveCounts.mediaSeq, liveCounts.discSeq);
          await session.setCurrentMediaSequenceSegments(liveSegments);
          debug(`[${this.sessionId}]: Switching from LIVE->V2L`);
          break;
        }
        this.eventId = scheduleObj.eventId;
        eventSegments = await session.getTruncatedVodSegments(scheduleObj.uri, (scheduleObj.duration / 1000));
        // Complete the Live->V2L 
        await session.setCurrentMediaAndDiscSequenceCount(liveCounts.mediaSeq, liveCounts.discSeq);
        await session.setCurrentMediaSequenceSegments(liveSegments);
        // Do V2L->VOD
        await session.setCurrentMediaSequenceSegments(eventSegments, true);
        debug(`[${this.sessionId}]: Switching from LIVE->VOD`);
        break;
      case SwitcherState.LIVE_TO_LIVE:
        this.eventId = scheduleObj.eventId;
        currLiveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
        eventSegments = await sessionLive.getCurrentMediaSequenceSegments();

        await sessionLive.setCurrentMediaAndDiscSequenceCount((currLiveCounts.mediaSeq + 1), currLiveCounts.discSeq);
        await sessionLive.setCurrentMediaSequenceSegments(eventSegments);
        await sessionLive.setLiveUri(scheduleObj.uri);
        debug(`[${this.sessionId}]: Switching from LIVE->LIVE`);
        break;
      default:
        debug(`[${this.sessionId}]: SwitcherState [${state}] not implemented`);
        this.streamTypeLive = false;
        break;
    }
  }

  async _validURI(uri) {
    try {
      const online = await fetch(uri);
      if (online.status >= 200 && online.status < 300) {
        return true;
      }
      return false;
    } catch (err) {
      return false;
    }
  }
}

module.exports = StreamSwitcher;
