const debug = require('debug')('engine-streamSwitcher');
const fetch = require('node-fetch');

const SwitcherState = Object.freeze({
  LIVE_NEW_URL: 1,
  LIVE: 2,
  VOD: 3,
});

class StreamSwitcher {
  constructor(config) {
    this.useDemuxedAudio = false;
    this.cloudWatchLogging = false;
    this.streamTypeLive = false;
    this.streamSwitchManager = null;

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
   * @returns A bool true if streamSwitchManager contains current Live event to be played else false.
   */
  async streamSwitcher(session, sessionLive) {
    if (!this.streamSwitchManager) {
      debug('No streamSwitchManager available');
      return false;
    }
    // Filter out schedule objects from the past
    const tsNow = Date.now();
    const strmSchedule = this.streamSwitchManager.getSchedule();
    const schedule = strmSchedule.filter((obj) => obj.estEnd >= tsNow);
    // If no more live streams, and streamType is live switch back to vod2live
    if (schedule.length === 0 && this.streamTypeLive) {
      await this._initSwitching(SwitcherState.VOD, session, sessionLive, null);
      debug(`++++++++++++++++++++++ [ A ] ++++++++++++++++++++++`);
      return false;
    }
    if (schedule.length === 0) {
      debug(`++++++++++++++++++++++ [ B ] ++++++++++++++++++++++`);
      return false;
    }
    const scheduleObj = schedule[0];
    // Check if Live URI is ok
    const validURI = await this._validateURI(scheduleObj.uri);
    if (!validURI) {
      debug(`Unreachable URI`);
      debug(`++++++++++++++++++++++ [ C ] ++++++++++++++++++++++`);
      if (this.streamTypeLive) {
        debug(`Switching back to vod2live`);
        await this._initSwitching(
          SwitcherState.VOD,
          session,
          sessionLive,
          null
        );
      }
      return false;
    }

    // Case: We want to be live
    if (tsNow >= scheduleObj.start && tsNow < scheduleObj.estEnd) {
      // TODO: Check if current streaming url == new streaming url
      if (!this.streamTypeLive) {
        await this._initSwitching(
          SwitcherState.LIVE,
          session,
          sessionLive,
          scheduleObj
        );
        debug(`++++++++++++++++++++++ [ D ] ++++++++++++++++++++++`);
        return true;
      }
      debug(`++++++++++++++++++++++ [ E ] ++++++++++++++++++++++`);
      return true;
    }
    if (schedule.length > 1) {
      const nextScheduleObj = schedule[1];
      if (tsNow >= nextScheduleObj.start) {
        debug(`BEFORE STREAMTYPE LIVE IS ${this.streamTypeLive}`);
        await this._initSwitching(
          SwitcherState.LIVE,
          session,
          sessionLive,
          nextScheduleObj
        );
        debug(`AFTER STREAMTYPE LIVE IS ${this.streamTypeLive}`);
        debug(`++++++++++++++++++++++ [ F ] ++++++++++++++++++++++`);
        return true;
      } else {
        debug(`++++++++++++++++++++++ [ G ] ++++++++++++++++++++++`);
        return false;
      }
    }
    // GO BACK TO V2L? Then:
    if (strmSchedule.length !== schedule.length && this.streamTypeLive) {
      // We are past the end point for the scheduled Live stream
      await this._initSwitching(SwitcherState.VOD, session, sessionLive, null);
      debug(`++++++++++++++++++++++ [ H ] ++++++++++++++++++++++`);
      return false;
    }
    debug(`++++++++++++++++++++++ [ I ] ++++++++++++++++++++++`);
    return false;
  }

  async _initSwitching(state, session, sessionLive, scheduleObj) {
    switch (state) {
      case SwitcherState.LIVE:
        this.streamTypeLive = true;
        // Do the v2l->live version: 1) get current mediaSeq 2) get last media sequence.
        const currVodSegments = await session.getCurrentMediaSequenceSegments();
        const currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
        
        const liveStreamUri = scheduleObj.uri;

        // Necessary data needed for manifest Rewrites!
        await sessionLive.setCurrentMediaAndDiscSequenceCount(
          currVodCounts.mediaSeq,
          currVodCounts.discSeq
        );
        await sessionLive.setCurrentMediaSequenceSegments(currVodSegments);
        await sessionLive.setLiveUri(liveStreamUri);
        debug(
          `+++++++++++++++++++++++ [ Switching from V2L->LIVE ] +++++++++++++++++++++++`
        );
        break;
      case SwitcherState.VOD:
        this.streamTypeLive = false;
        // Do the live->v2l version
        const currLiveCounts =
          await sessionLive.getCurrentMediaAndDiscSequenceCount();
        const currLiveSegments =
          await sessionLive.getCurrentMediaSequenceSegments();
        debug(
          `-------- mseq & dseq from SessionLive -> [${currLiveCounts.mediaSeq}]:[${currLiveCounts.discSeq}]`
        );
        debug(
          `-------- VOD Segments from SessionLive -> [${Object.keys(
            currLiveSegments
          )}_${currLiveSegments[Object.keys(currLiveSegments)[1]].length}]`
        );
        // TODO: Set data in Session
        // Necessary data needed for manifest Rewrites!
        await session.setCurrentMediaAndDiscSequenceCount(
          currLiveCounts.mediaSeq,
          currLiveCounts.discSeq
        );
        await session.setCurrentMediaSequenceSegments(currLiveSegments);
        debug(
          `+++++++++++++++++++++++ [ Switching from LIVE->V2L ] +++++++++++++++++++++++`
        );
        break;
      case SwitcherState.LIVE_NEW_URL:
        // Do the live->live version
        // TODO: const currMediaAndDicSeq = await sessionLIVE.getCurrentMediaAndDiscSequenceCount();
        // TODO: const currVodSegments = await sessionLIVE.getCurrentMediaSequenceSegments();
        // TODO: Also send ScheduleObj.uri to sessionLIVE
        debug(
          `+++++++++++++++++++++++ [ Switching from LIVE->LIVE ] +++++++++++++++++++++++`
        );
        break;
      default:
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
