const debug = require('debug')('engine-streamSwitcher');
const fetch = require('node-fetch');

const SwitcherState = Object.freeze({
  LIVE_TO_LIVE: 1,
  LIVE_TO_VOD: 2,
  VOD_TO_LIVE: 3,
  VOD_TO_VOD: 4 // Not Implemented
});

class StreamSwitcher {
  constructor(config) {
    this.useDemuxedAudio = false;
    this.cloudWatchLogging = false;
    this.streamTypeLive = false;
    this.streamSwitchManager = null;
    this.streamID = null;

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
    const schedule = strmSchedule.filter((obj) => obj.estEnd >= tsNow);
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
    // Check if Live URI is ok
    const validURI = await this._validateURI(scheduleObj.uri);
    if (!validURI) {
      debug(`[${this.sessionId}]: Unreachable URI`);
      if (this.streamTypeLive) {
        debug(`[${this.sessionId}]: Switching back to vod2live due to unreachable URI`);
        await this._initSwitching(
          SwitcherState.LIVE_TO_VOD,
          session,
          sessionLive,
          null
        );
      }
      return false;
    }
    // Case: Live->Live
    if (schedule.length > 0 && this.streamTypeLive) {
      if (tsNow >= scheduleObj.start && this.streamID !== scheduleObj.id) {
        await this._initSwitching(
          SwitcherState.LIVE_TO_LIVE,
          session,
          sessionLive,
          scheduleObj
        );
        return true;
      }
    }
    // Case: We want to be live
    if (tsNow >= scheduleObj.start && tsNow < scheduleObj.estEnd) {
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
    // GO BACK TO V2L? Then:
    if (tsNow < scheduleObj.start && this.streamTypeLive) {
      // We are past the end point for the scheduled Live stream
      await this._initSwitching(
        SwitcherState.LIVE_TO_VOD,
        session,
        sessionLive,
        null
      );
      return false;
    }
    return false;
  }

  async _initSwitching(state, session, sessionLive, scheduleObj) {
    switch (state) {
      case SwitcherState.VOD_TO_LIVE:
        this.streamTypeLive = true;
        this.streamID = scheduleObj.id;
        const currVodSegments = await session.getCurrentMediaSequenceSegments();
        const currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
        const liveStreamUri = scheduleObj.uri;

        await sessionLive.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
        await sessionLive.setCurrentMediaSequenceSegments(currVodSegments);
        await sessionLive.setLiveUri(liveStreamUri);
        debug(`[${this.sessionId}]: [ Switching from V2L->LIVE ]`);
        break;
      case SwitcherState.LIVE_TO_VOD:
        this.streamTypeLive = false;
        this.streamID = null;
        const currLiveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
        const currLiveSegments = await sessionLive.getCurrentMediaSequenceSegments();

        await session.setCurrentMediaAndDiscSequenceCount(
          currLiveCounts.mediaSeq,
          currLiveCounts.discSeq
        );
        await session.setCurrentMediaSequenceSegments(currLiveSegments);
        debug(`[${this.sessionId}]: Switching from LIVE->V2L`);
        break;
      case SwitcherState.LIVE_TO_LIVE:
        this.streamID = scheduleObj.id;
        const newLiveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
        const newLiveSegments = await sessionLive.getCurrentMediaSequenceSegments();
        const newLiveStreamUri = scheduleObj.uri;

        await sessionLive.setCurrentMediaAndDiscSequenceCount((newLiveCounts.mediaSeq + 1), newLiveCounts.discSeq);
        await sessionLive.setCurrentMediaSequenceSegments(newLiveSegments);
        await sessionLive.setLiveUri(newLiveStreamUri);
        debug(`[${this.sessionId}]: Switching from LIVE->LIVE`);
        break;
      default:
        debug(`[${this.sessionId}]: SwitcherState [${state}] not yet implemented`);
        this.streamTypeLive = false;
        break;
    }
  }

  async _validateURI(liveURI) {
    try {
      const online = await fetch(liveURI);
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
