const crypto = require('crypto');
const debug = require('debug')('engine-session');
const HLSVod = require('@eyevinn/hls-vodtolive');
const m3u8 = require('@eyevinn/m3u8');
const HLSRepeatVod = require('@eyevinn/hls-repeat');
const HLSTruncateVod = require('@eyevinn/hls-truncate');
const Readable = require('stream').Readable;

const { SessionState } = require('./session_state.js');
const { PlayheadState } = require('./playhead_state.js');

const { applyFilter, cloudWatchLog, m3u8Header, logerror } = require('./util.js');
const ChaosMonkey = require('./chaos_monkey.js');

const AVERAGE_SEGMENT_DURATION = 3000;
const DEFAULT_PLAYHEAD_DIFF_THRESHOLD = 1000;
const DEFAULT_MAX_TICK_INTERVAL = 10000;

const timer = ms => new Promise(res => setTimeout(res, ms));

class Session {
  /**
   *
   * config: {
   *   startWithId,
   * }
   *
   */
  constructor(assetManager, config, sessionStore) {
    this._assetManager = assetManager;
    this._sessionId = crypto.randomBytes(20).toString('hex');

    this._sessionStateStore = sessionStore.sessionStateStore;
    this._playheadStateStore = sessionStore.playheadStateStore;
    this._instanceId = sessionStore.instanceId;

    //this.currentVod;
    this.currentMetadata = {};
    this._events = [];
    this.averageSegmentDuration = AVERAGE_SEGMENT_DURATION;
    this.use_demuxed_audio = false;
    this.cloudWatchLogging = false;
    this.playheadDiffThreshold = DEFAULT_PLAYHEAD_DIFF_THRESHOLD;
    this.maxTickInterval = DEFAULT_MAX_TICK_INTERVAL;
    this.diffCompensation = null;
    this.prevVodMediaSeq = {
      video: null,
      audio: null
    }
    this.waitingForNextVod = false;
    this.leaderIsSettingNextVod = false;
    this.isSwitchingBackToV2L = false;
    this.switchDataForSession = {
      mediaSeq: null,
      discSeq: null,
      mediaSeqOffset: null,
      transitionSegments: null,
      reloadBehind: null,
    }

    if (config) {
      if (config.sessionId) {
        this._sessionId = config.sessionId;
      }

      if (config.category) {
        this._category = config.category;
      }
      if (config.averageSegmentDuration) {
        this.averageSegmentDuration = config.averageSegmentDuration;
      }
      if (config.useDemuxedAudio) {
        this.use_demuxed_audio = true;
      }
      if (config.startWithId) {
        this.startWithId = config.startWithId;
      }
      if (config.profile) {
        this._sessionProfile = config.profile;
      }
      if (config.audioTracks) {
        this._audioTracks = config.audioTracks;
      }
      if (config.closedCaptions) {
        this._closedCaptions = config.closedCaptions;
      }
      if (config.slateUri) {
        this.slateUri = config.slateUri;
        this.slateRepetitions = config.slateRepetitions || 10;
        this.slateDuration = config.slateDuration || 4000;
        debug(`Will use slate URI ${this.slateUri} (${this.slateRepetitions} ${this.slateDuration}ms)`);
      }
      if (config.cloudWatchMetrics) {
        this.cloudWatchLogging = true;
      }
      if (config.playheadDiffThreshold) {
        this.playheadDiffThreshold = config.playheadDiffThreshold;
      }
      if (config.maxTickInterval) {
        this.maxTickInterval = config.maxTickInterval;
      }
      if (config.disabledPlayhead) {
        this.disabledPlayhead = true;
      }
      if (config.targetDurationPadding) {
        this.targetDurationPadding = config.targetDurationPadding;
      }
      if (config.forceTargetDuration) {
        this.forceTargetDuration = config.forceTargetDuration;
      }
    }
  }

  async initAsync() {
    this._sessionState = await this._sessionStateStore.create(this._sessionId, this._instanceId);
    this._playheadState = await this._playheadStateStore.create(this._sessionId);

    if (this.startWithId) {
      await this._sessionState.set("state", SessionState.VOD_INIT_BY_ID);
      await this._sessionState.set("assetId", this.startWithId);
    }
  }

  get sessionId() {
    return this._sessionId;
  }

  async startPlayheadAsync() {
    debug(`[${this._sessionId}]: Playhead consumer started:`);
    debug(`[${this._sessionId}]: diffThreshold=${this.playheadDiffThreshold}`);
    debug(`[${this._sessionId}]: maxTickInterval=${this.maxTickInterval}`);
    debug(`[${this._sessionId}]: averageSegmentDuration=${this.averageSegmentDuration}`);

    this.disabledPlayhead = false;

    let playheadState = await this._playheadState.getValues(["state"]);
    let state = await this._playheadState.setState(PlayheadState.RUNNING);
    while (state !== PlayheadState.CRASHED) {
      try {
        const tsIncrementBegin = Date.now();
        const manifest = await this.incrementAsync();
        if (!manifest) {
          debug(`[${this._sessionId}]: No manifest available yet, will try again after 1000ms`);
          await timer(1000);
          continue;
        }
        const tsIncrementEnd = Date.now();
        const sessionState = await this._sessionState.getValues(["state"]);
        playheadState = await this._playheadState.getValues(["tickInterval", "playheadRef", "tickMs"]);
        state = await this._playheadState.getState();

        const isLeader = await this._sessionStateStore.isLeader(this._instanceId);
        if (isLeader && 
          [
            SessionState.VOD_NEXT_INIT,
            SessionState.VOD_NEXT_INITIATING,
            SessionState.VOD_RELOAD_INIT,
            SessionState.VOD_RELOAD_INITIATING
          ].indexOf(sessionState.state) !== -1) {

          const firstDuration = await this._getFirstDuration(manifest);
          const tickInterval = firstDuration < 2 ? 2 : firstDuration;
          debug(`[${this._sessionId}]: I am the leader and updated tick interval to ${tickInterval} sec`);
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'tickIntervalUpdated', channel: this._sessionId, tickIntervalSec: tickInterval });
          this._playheadState.set("tickInterval", tickInterval);
        } else if (state == PlayheadState.STOPPED) {
          debug(`[${this._sessionId}]: Stopping playhead`);
          return;
        } else {
          const reqTickInterval = playheadState.tickInterval;
          const timeSpentInIncrement = (tsIncrementEnd - tsIncrementBegin) / 1000;
          let tickInterval = reqTickInterval - timeSpentInIncrement;
          const delta = await this._getCurrentDeltaTime();
          if (delta != 0) {
            debug(`[${this._sessionId}]: Delta time is != 0 need will adjust ${delta}sec to tick interval`);
            tickInterval += delta;
          }
          const position = (await this._getCurrentPlayheadPosition()) * 1000;
          const timePosition = Date.now() - playheadState.playheadRef;
          const diff = position - timePosition;
          debug(`[${this._sessionId}]: ${timePosition}:${position}:${diff > 0 ? '+' : ''}${diff}ms`);
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'playheadDiff', channel: this._sessionId, diffMs: diff });
          if (diff > this.playheadDiffThreshold) {
            tickInterval += ((diff / 1000)) - (this.playheadDiffThreshold / 1000);
          } else if (diff < -this.playheadDiffThreshold) {
            tickInterval += ((diff / 1000)) + (this.playheadDiffThreshold / 1000);
          }
          if (this.diffCompensation && this.diffCompensation > 0) {
            const DIFF_COMPENSATION = 2000;
            debug(`[${this._sessionId}]: Adding ${DIFF_COMPENSATION}msec to tickInterval to compensate for schedule diff (current=${this.diffCompensation}msec)`);
            tickInterval += (DIFF_COMPENSATION / 1000);
            this.diffCompensation -= DIFF_COMPENSATION;
          }
          debug(`[${this._sessionId}]: Requested tickInterval=${tickInterval}s (max=${this.maxTickInterval / 1000}s, diffThreshold=${this.playheadDiffThreshold}msec)`);
          if (tickInterval <= 0) {
            tickInterval = 0.5;
          } else if (tickInterval > (this.maxTickInterval / 1000)) {
            tickInterval = this.maxTickInterval / 1000;
          }
          debug(`[${this._sessionId}]: (${(new Date()).toISOString()}) ${timeSpentInIncrement}sec in increment. Next tick in ${tickInterval} seconds`)
          await timer((tickInterval * 1000) - 50);
          const tsTickEnd = Date.now();
          await this._playheadState.set("tickMs", (tsTickEnd - tsIncrementBegin));
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'tickInterval', channel: this._sessionId, tickTimeMs: (tsTickEnd - tsIncrementBegin) });
        }
      } catch (err) {
        debug(`[${this._sessionId}]: Playhead consumer crashed (1)`);
        console.error(`[${this._sessionId}]: ${err.message}`);
        cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
          { event: 'error', on: 'playhead', channel: this._sessionId, err: err });
        debug(err);
        state = await this._playheadState.setState(PlayheadState.CRASHED);
      }
    }
  }

  async restartPlayheadAsync() {
    await this._sessionState.set("state", SessionState.VOD_NEXT_INIT);
    debug(`[${this._sessionId}]: Restarting playhead consumer`);
    await this.startPlayheadAsync();
  }

  async stopPlayheadAsync() {
    debug(`[${this._sessionId}]: Stopping playhead consumer`);
    await this._playheadState.set("state", PlayheadState.STOPPED);
  }

  async getStatusAsync() {
    if (this.disabledPlayhead) {
      return {
        sessionId: this._sessionId,
      };
    } else {
      const playheadState = await this._playheadState.getValues(["tickMs", "mediaSeq", "vodMediaSeqVideo"]);
      const state = await this._playheadState.getState();
      const sessionState = await this._sessionState.getValues(["slateCount"]);
      const playheadStateMap = {};
      playheadStateMap[PlayheadState.IDLE] = 'idle';
      playheadStateMap[PlayheadState.RUNNING] = 'running';
      playheadStateMap[PlayheadState.CRASHED] = 'crashed';
      playheadStateMap[PlayheadState.STOPPED] = 'stopped';

      const status = {
        sessionId: this._sessionId,
        playhead: {
          state: playheadStateMap[state],
          tickMs: playheadState.tickMs,
          mediaSeq: playheadState.mediaSeq + playheadState.vodMediaSeqVideo,
        },
        slateInserted: sessionState.slateCount,
      };
      return status;
    }
  }

  async resetAsync() {
    await this._sessionStateStore.reset(this._sessionId);
    await this._playheadStateStore.reset(this._sessionId);
  }

  async getSessionState() {
    const state = await this._sessionState.get("state");
    return state;
  }

  async getTruncatedVodSegments(vodUri, duration) {
    try {
      const hlsVod = await this._truncateSlate(null, duration, vodUri);
      let vodSegments = hlsVod.getMediaSegments();
      Object.keys(vodSegments).forEach((bw) => vodSegments[bw].unshift({ discontinuity: true, cue: { in: true } }));
      return vodSegments;
    } catch (exc) {
      debug(`[${this._sessionId}]: Failed to generate truncated VOD!`);
      return null;
    }
  }

  async setCurrentMediaSequenceSegments(segments, mSeqOffset, reloadBehind) {
    if (!this._sessionState) {
      throw new Error("Session not ready");
    }
    this.isSwitchingBackToV2L = true;

    this.switchDataForSession.reloadBehind = reloadBehind;
    this.switchDataForSession.transitionSegments = segments;
    this.switchDataForSession.mediaSeqOffset = mSeqOffset;

    let isLeader = await this._sessionStateStore.isLeader(this._instanceId);
    if (!isLeader) {
      debug(`[${this._sessionId}]: FOLLOWER: Invalidate cache to ensure having the correct VOD!`);
      await this._sessionState.clearCurrentVodCache();

      let vodReloaded = await this._sessionState.get("vodReloaded");
      let attempts = 9;
      while (!isLeader && !vodReloaded && attempts > 0) {
        debug(`[${this._sessionId}]: FOLLOWER: I arrived before LEADER. Waiting (1000ms) for LEADER to reload currentVod in store! (tries left=${attempts})`);
        await timer(1000);
        await this._sessionStateStore.clearLeaderCache();
        isLeader = await this._sessionStateStore.isLeader(this._instanceId);
        vodReloaded = await this._sessionState.get("vodReloaded");
        attempts--;
      }

      if (attempts === 0) {
        debug(`[${this._sessionId}]: FOLLOWER: WARNING! Attempts=0 - Risk of using wrong currentVod`);
      }
      if (!isLeader || vodReloaded) {
        debug(`[${this._sessionId}]: FOLLOWER: leader is alive, and has presumably updated currentVod. Clearing the cache now`);
        await this._sessionState.clearCurrentVodCache();
        return;
      }
      debug(`[${this._sessionId}]: NEW LEADER: Setting state=VOD_RELOAD_INIT`);
      this.isSwitchingBackToV2L = true;
      await this._sessionState.set("state", SessionState.VOD_RELOAD_INIT)

    } else {
      let vodReloaded = await this._sessionState.get("vodReloaded");
      let attempts = 9;
      while (!vodReloaded && attempts > 0) {
        debug(`[${this._sessionId}]: LEADER: Waiting (1000ms) to buy some time reloading vod and adding it to store! (tries left=${attempts})`);
        await timer(1000);
        vodReloaded = await this._sessionState.get("vodReloaded");
        attempts--;
      }
      if (attempts === 0) {
        debug(`[${this._sessionId}]: LEADER: WARNING! Vod was never Reloaded!`);
        return; 
      }
    }
  }

  async getCurrentMediaSequenceSegments() {
    if (!this._sessionState) {
      throw new Error('Session not ready');
    }
    const isLeader = await this._sessionStateStore.isLeader(this._instanceId);
    if (isLeader) {
      await this._sessionState.set("vodReloaded", 0);
    }

    // Only read data from store if state is VOD_PLAYING
    let state = await this.getSessionState();
    let tries = 12;
    while (state !== SessionState.VOD_PLAYING && tries > 0) {
    const waitTimeMs = 500; 
    debug(`[${this._sessionId}]: state=${state} - Waiting ${waitTimeMs}ms_${tries} until Leader has finished loading next vod.`);
      await timer(waitTimeMs);
      tries--;
      state = await this.getSessionState();
    }

    const playheadState = await this._playheadState.getValues(["mediaSeq", "vodMediaSeqVideo"]);
    if (playheadState.vodMediaSeqVideo === 0) {
      if (!isLeader) {
        debug(`[${this._sessionId}]: Not a leader and first media sequence in a VOD is requested. Invalidate cache to ensure having the correct VOD.`);
        await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
      }
    }
    const currentVod = await this._sessionState.getCurrentVod();
    if (currentVod) {
      try {
        const mediaSegments = currentVod.getLiveMediaSequenceSegments(playheadState.vodMediaSeqVideo);
        debug(`[${this._sessionId}]: Requesting all segments from Media Sequence: ${playheadState.vodMediaSeqVideo}_${currentVod.getLiveMediaSequencesCount()}`);
        return mediaSegments;
      } catch (err) {
        logerror(this._sessionId, err);
        await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
        throw new Error("Failed to get all current Media segments: " + JSON.stringify(playheadState));
      }
    } else {
      throw new Error("Engine not ready");
    }
  }

  async setCurrentMediaAndDiscSequenceCount(_mediaSeq, _discSeq) {
    if (!this._sessionState) {
      throw new Error("Session not ready");
    }

    this.isSwitchingBackToV2L = true;

    this.switchDataForSession.mediaSeq = _mediaSeq;
    this.switchDataForSession.discSeq = _discSeq;
  }

  async getCurrentMediaAndDiscSequenceCount() {
    if (!this._sessionState) {
      throw new Error('Session not ready');
    }

    // Only read data from store if state is VOD_PLAYING
    let state = await this.getSessionState();
    let tries = 12;
    while (state !== SessionState.VOD_PLAYING && tries > 0) {
      const waitTimeMs = 500; 
      debug(`[${this._sessionId}]: state=${state} - Waiting ${waitTimeMs}ms_${tries} until Leader has finished loading next vod.`);
      await timer(waitTimeMs);
      tries--;
      state = await this.getSessionState();
    }
    
    const playheadState = await this._playheadState.getValues(["mediaSeq", "vodMediaSeqVideo"]);
    const discSeqOffset = await this._sessionState.get("discSeq");
    if (playheadState.vodMediaSeqVideo === 0) {
      const isLeader = await this._sessionStateStore.isLeader(this._instanceId);
      if (!isLeader) {
        debug(`[${this._sessionId}]: Not a leader and first media sequence in a VOD is requested. Invalidate cache to ensure having the correct VOD.`);
        await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
      }
    }
    const currentVod = await this._sessionState.getCurrentVod();
    if (currentVod) {
      try {
        const discSeqCount = discSeqOffset + currentVod.discontinuities[playheadState.vodMediaSeqVideo];
        debug(`[${this._sessionId}]: MediaSeq: (${(playheadState.mediaSeq + playheadState.vodMediaSeqVideo)}) and DiscSeq: (${discSeqCount}) requested `);
        return {
          'mediaSeq': (playheadState.mediaSeq + playheadState.vodMediaSeqVideo),
          'discSeq': discSeqCount,
        };
      } catch (err) {
        logerror(this._sessionId, err);
        await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
        throw new Error("Failed to get states: " + JSON.stringify(playheadState));
      }
    } else {
      throw new Error("Engine not ready");
    }
  }

  async getCurrentMediaManifestAsync(bw, playbackSessionId) {
    if (!this._sessionState) {
      throw new Error('Session not ready');
    }
    // Be sure that the leader is not in the middle of setting new vod data in store.
    // Followers will never run this part...
    let tries = 12;
    while (tries > 0 && this.leaderIsSettingNextVod) {
      debug(`[${this._sessionId}]: Leader is setting the next vod. Waiting 500ms_${tries}`);
      await timer(500);
      tries--;
    }

    const sessionState = await this._sessionState.getValues(["vodMediaSeqVideo", "discSeq"]);
    const playheadState = await this._playheadState.getValues(["mediaSeq", "vodMediaSeqVideo"]);

    if (!this.prevVodMediaSeq.video) {
      this.prevVodMediaSeq.video = playheadState.vodMediaSeqVideo;
    }

    if (playheadState.vodMediaSeqVideo > sessionState.vodMediaSeqVideo) {
      debug(`[${this._sessionId}]: Recently Loaded Next Vod. PlayheadState not up-to-date. Return the last generated m3u8`);
      const m3u8 = await this._playheadState.getLastM3u8();
      if (m3u8) {
        this.prevVodMediaSeq.video = playheadState.vodMediaSeqVideo;
        return m3u8;
      } else {
        debug(`[${this._sessionId}]: We don't have any previously generated m3u8`);
      }
    }

    if (playheadState.vodMediaSeqVideo < 2 || playheadState.vodMediaSeqVideo < this.prevVodMediaSeq.video) { 
      debug(`[${this._sessionId}]: current[${playheadState.vodMediaSeqVideo}]_prev[${this.prevVodMediaSeq.video}]`);
      const isLeader = await this._sessionStateStore.isLeader(this._instanceId);
      if (!isLeader) {
        debug(`[${this._sessionId}]: Not a leader and first|second media sequence in a VOD is requested. Invalidate cache to ensure having the correct VOD.`);
        await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
      }
    }

    const currentVod = await this._sessionState.getCurrentVod();
    if (currentVod) {
      try {
        debug(`[${this._sessionId}]: [${playheadState.vodMediaSeqVideo}]_[${currentVod.getLiveMediaSequencesCount()}]`);
        const m3u8 = currentVod.getLiveMediaSequences(playheadState.mediaSeq, bw, playheadState.vodMediaSeqVideo, sessionState.discSeq, this.targetDurationPadding, this.forceTargetDuration);
        debug(`[${this._sessionId}]: [${playheadState.mediaSeq + playheadState.vodMediaSeqVideo}][${sessionState.discSeq}][+${this.targetDurationPadding || 0}] Current media manifest for ${bw} requested`);
        this.prevVodMediaSeq.video = playheadState.vodMediaSeqVideo;
        return m3u8;
      } catch (err) {
        logerror(this._sessionId, err);
        await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
        throw new Error("Failed to generate manifest: " + JSON.stringify(playheadState));
      }
    } else {
      throw new Error("Engine not ready");
    }
  }

  async getCurrentAudioManifestAsync(audioGroupId, audioLanguage, playbackSessionId) {
    if (!this._sessionState) {
      throw new Error('Session not ready');
    }

    const sessionState = await this._sessionState.getValues(["discSeq"]);
    const playheadState = await this._playheadState.getValues(["mediaSeq", "vodMediaSeqAudio"]);
    const currentVod = await this._sessionState.getCurrentVod();
    if (currentVod) {
      try {
        const m3u8 = currentVod.getLiveMediaAudioSequences(playheadState.mediaSeq, audioGroupId, audioLanguage, playheadState.vodMediaSeqAudio, sessionState.discSeq, this.targetDurationPadding, this.forceTargetDuration);
        // # Case: current VOD does not have the selected track.
        if (!m3u8) {
          debug(`[${this._sessionId}]: [${playheadState.mediaSeq + playheadState.vodMediaSeqAudio}] Request Failed for current audio manifest for ${audioGroupId}-${audioLanguage}`);
        }
        debug(`[${this._sessionId}]: [${playheadState.mediaSeq + playheadState.vodMediaSeqAudio}] Current audio manifest for ${audioGroupId}-${audioLanguage} requested`);
        return m3u8;
      } catch (err) {
        logerror(this._sessionId, err);
        await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
        throw new Error("Failed to generate audio manifest: " + JSON.stringify(playheadState));
      }
    } else {
      throw new Error("Engine not ready");
    }
  }

  async incrementAsync() {
    await this._tickAsync(); 
    const isLeader = await this._sessionStateStore.isLeader(this._instanceId);

    let sessionState = await this._sessionState.getValues(
      ["state", "mediaSeq", "discSeq", "vodMediaSeqVideo", "vodMediaSeqAudio"]);
    let playheadState = await this._playheadState.getValues(["mediaSeq", "vodMediaSeqVideo", "vodMediaSeqAudio"]);
    let currentVod = await this._sessionState.getCurrentVod();
    if (!currentVod ||
        sessionState.vodMediaSeqVideo === null ||
        sessionState.vodMediaSeqAudio === null ||
        sessionState.state === null ||
        sessionState.mediaSeq === null ||
        sessionState.discSeq === null) {
      debug(`[${this._sessionId}]: Session is not ready yet`);
      debug(sessionState);
      await this._sessionState.clearCurrentVodCache();
      if (isLeader) {
        debug(`[${this._sessionId}]: I am the leader, trying to initiate the session`);
        sessionState.state = await this._sessionState.set("state", SessionState.VOD_INIT);
      }
      return null;
    }
    if (sessionState.state === SessionState.VOD_NEXT_INITIATING || sessionState.state === SessionState.VOD_RELOAD_INITIATING) {
      if (isLeader) {
        const leaderAction = sessionState.state === SessionState.VOD_NEXT_INITIATING ? "initiated" : "reloaded";
        debug(`[${this._sessionId}]: I am the leader and have just ${leaderAction} next VOD, let's move to VOD_PLAYING`);
        sessionState.state = await this._sessionState.set("state", SessionState.VOD_PLAYING);
      } else {
        debug(`[${this._sessionId}]: Return the last generated m3u8 to give the leader some time`);
        let m3u8 = await this._playheadState.getLastM3u8();
        if (m3u8) {
          return m3u8;
        } else {
          debug(`[${this._sessionId}]: We don't have any previously generated m3u8`);
        }
      }
    } else {
      sessionState.vodMediaSeqVideo = await this._sessionState.increment("vodMediaSeqVideo");
      sessionState.vodMediaSeqAudio = await this._sessionState.increment("vodMediaSeqAudio");
    }
    
    if (sessionState.vodMediaSeqVideo >= currentVod.getLiveMediaSequencesCount() - 1) {
      sessionState.vodMediaSeqVideo = await this._sessionState.set("vodMediaSeqVideo", currentVod.getLiveMediaSequencesCount() - 1);
      sessionState.vodMediaSeqAudio = await this._sessionState.set("vodMediaSeqAudio", currentVod.getLiveMediaSequencesCount() - 1);
      sessionState.state = await this._sessionState.set("state", SessionState.VOD_NEXT_INIT);
    }

    if (this.isSwitchingBackToV2L) {
      sessionState.state = await this._sessionState.set("state", SessionState.VOD_RELOAD_INIT);
      this.isSwitchingBackToV2L = false;
    }

    playheadState.mediaSeq = await this._playheadState.set("mediaSeq", sessionState.mediaSeq);
    playheadState.vodMediaSeqVideo = await this._playheadState.set("vodMediaSeqVideo", sessionState.vodMediaSeqVideo);
    playheadState.vodMediaSeqAudio = await this._playheadState.set("vodMediaSeqAudio", sessionState.vodMediaSeqAudio);
    debug(`[${this._sessionId}]: INCREMENT (mseq=${playheadState.mediaSeq + playheadState.vodMediaSeqVideo}) vodMediaSeq=(${playheadState.vodMediaSeqVideo}_${playheadState.vodMediaSeqAudio} of ${currentVod.getLiveMediaSequencesCount()})`);
    let m3u8 = currentVod.getLiveMediaSequences(playheadState.mediaSeq, 180000, playheadState.vodMediaSeqVideo, sessionState.discSeq);
    await this._playheadState.setLastM3u8(m3u8);

    if (!isLeader && playheadState.vodMediaSeqVideo < 2) {
      debug(`[${this._sessionId}]: Not a leader and have just set playheadState vodMediaSeqVideo to 0|1. Invalidate cache to ensure having the correct VOD.`);
      await this._sessionState.clearCurrentVodCache();
    }

    return m3u8;
  }

  async getMediaManifestAsync(bw, opts) {
    await this._tickAsync();
    const tsLastRequestVideo = await this._sessionState.get("tsLastRequestVideo");
    let timeSinceLastRequest = (tsLastRequestVideo === null) ? 0 : Date.now() - tsLastRequestVideo;
    let sessionState = await this._sessionState.getValues(
      ["state", "vodMediaSeqVideo", "mediaSeq", "discSeq", "lastM3u8"]);

    const currentVod = await this._sessionState.getCurrentVod();
    if (!currentVod) {
      throw new Error('Session not ready');
    }
    if (sessionState.state === SessionState.VOD_NEXT_INITIATING) {
      sessionState.state = await this._sessionState.set("state", SessionState.VOD_PLAYING);
    } else {
      let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);
      sessionState.vodMediaSeqVideo = await this._sessionState.increment("vodMediaSeqVideo", sequencesToIncrement);
    }
    if (sessionState.vodMediaSeqVideo >= currentVod.getLiveMediaSequencesCount() - 1) {
      sessionState.vodMediaSeqVideo = await this._sessionState.set("vodMediaSeqVideo", currentVod.getLiveMediaSequencesCount() - 1);
      sessionState.state = await this._sessionState.set("state", SessionState.VOD_NEXT_INIT);
    }

    debug(`[${this._sessionId}]: VIDEO ${timeSinceLastRequest} (${this.averageSegmentDuration}) bandwidth=${bw} vodMediaSeq=(${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio})`);
    let m3u8;
    try {
      m3u8 = currentVod.getLiveMediaSequences(sessionState.mediaSeq, bw, sessionState.vodMediaSeqVideo, sessionState.discSeq);
    } catch (exc) {
      if (sessionState.lastM3u8[bw]) {
        m3u8 = sessionState.lastM3u8[bw]
      } else {
        logerror(this._sessionId, exc);
        throw new Error('Failed to generate media manifest');
      }
    }
    let lastM3u8 = sessionState.lastM3u8;
    lastM3u8[bw] = m3u8;
    sessionState.lastM3u8 = await this._sessionState.set("lastM3u8", lastM3u8);
    sessionState.lastServedM3u8 = await this._sessionState.set("lastServedM3u8", m3u8);
    sessionState.tsLastRequestVideo = await this._sessionState.set("tsLastRequestVideo", Date.now());

    if (sessionState.state === SessionState.VOD_NEXT_INIT) {
      await this._tickAsync();
      const tsLastRequestVideo = await this._sessionState.get("tsLastRequestVideo");
      let timeSinceLastRequest = (tsLastRequestVideo === null) ? 0 : Date.now() - tsLastRequestVideo;

      let sessionState = await this._sessionState.getValues(
        ["state", "vodMediaSeqVideo", "mediaSeq", "discSeq", "lastM3u8"]);
      if (sessionState.state === SessionState.VOD_NEXT_INITIATING) {
        sessionState.state = await this._sessionState.set("state", SessionState.VOD_PLAYING);
      }
      debug(`[${this._sessionId}]: VIDEO ${timeSinceLastRequest} (${this.averageSegmentDuration}) bandwidth=${bw} vodMediaSeq=(${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio})`);
      try {
        m3u8 = currentVod.getLiveMediaSequences(sessionState.mediaSeq, bw, sessionState.vodMediaSeqVideo, sessionState.discSeq);
      } catch (exc) {
        if (sessionState.lastM3u8[bw]) {
          m3u8 = sessionState.lastM3u8[bw]
        } else {
          logerror(this._sessionId, exc);
          throw new Error('Failed to generate media manifest');
        }
      }
      let lastM3u8 = sessionState.lastM3u8;
      lastM3u8[bw] = m3u8;
      sessionState.lastM3u8 = await this._sessionState.set("lastM3u8", lastM3u8);
      sessionState.lastServedM3u8 = await this._sessionState.set("lastServedM3u8", m3u8);
      sessionState.tsLastRequestVideo = await this._sessionState.set("tsLastRequestVideo", Date.now());
      return m3u8;
    } else {
      return m3u8;
    }
  }

  async getAudioManifestAsync(audioGroupId, audioLanguage, opts) {
    const tsLastRequestAudio = await this._sessionState.get("tsLastRequestAudio");
    let timeSinceLastRequest = (tsLastRequestAudio === null) ? 0 : Date.now() - tsLastRequestAudio;

    let sessionState = await this._sessionState.getValues(
      ["state", "vodMediaSeqVideo", "vodMediaSeqAudio", "mediaSeq", "discSeq", "lastM3u8"]);
    const currentVod = await this._sessionState.getCurrentVod();
    if (sessionState.state !== SessionState.VOD_NEXT_INITIATING) {
      let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);

      if (sessionState.vodMediaSeqAudio < sessionState.vodMediaSeqVideo) {
        sessionState.vodMediaSeqAudio = await this._sessionState.increment("vodMediaSeqAudio", sequencesToIncrement);
        if (sessionState.vodMediaSeqAudio >= currentVod.getLiveMediaSequencesCount() - 1) {
          sessionState.vodMediaSeqAudio = await this._sessionState.set("vodMediaSeqAudio", currentVod.getLiveMediaSequencesCount() - 1);
        }
      }
    }

    debug(`[${this._sessionId}]: AUDIO ${timeSinceLastRequest} (${this.averageSegmentDuration}) audioGroupId=${audioGroupId} audioLanguage=${audioLanguage} vodMediaSeq=(${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio})`);
    let m3u8;
    try {
      m3u8 = currentVod.getLiveMediaAudioSequences(sessionState.mediaSeq, audioGroupId, audioLanguage, sessionState.vodMediaSeqAudio, sessionState.discSeq);
    } catch (exc) {
      if (sessionState.lastM3u8[audioGroupId][audioLanguage]) {
        m3u8 = sessionState.lastM3u8[audioGroupId][audioLanguage];
      } else {
        logerror(this._sessionId, exc);
        throw new Error('Failed to generate audio manifest');
      }
    }
    let lastM3u8 = sessionState.lastM3u8;
    lastM3u8[audioGroupId] = {};
    lastM3u8[audioGroupId][audioLanguage] = m3u8;
    sessionState.lastM3u8 = await this._sessionState.set("lastM3u8", lastM3u8);
    sessionState.tsLastRequestAudio = await this._sessionState.set("tsLastRequestAudio", Date.now());
    return m3u8;
  }

  async getMasterManifestAsync(filter) {
    let m3u8 = "#EXTM3U\n";
    m3u8 += "#EXT-X-VERSION:4\n";
    m3u8 += m3u8Header(this._instanceId);
    m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.session.id",VALUE="${this._sessionId}"\n`;
    m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.eventstream",VALUE="/eventstream/${this._sessionId}"\n`;
    const currentVod = await this._sessionState.getCurrentVod();
    if (!currentVod) {
      throw new Error('Session not ready');
    }
    let audioGroupIds = currentVod.getAudioGroups();
    let defaultAudioGroupId;
    let hasClosedCaptions = this._closedCaptions && this._closedCaptions.length > 0;
    if (hasClosedCaptions) {
      this._closedCaptions.forEach(cc => {
        m3u8 += `#EXT-X-MEDIA:TYPE=CLOSED-CAPTIONS,GROUP-ID="cc",LANGUAGE="${cc.lang}",NAME="${cc.name}",DEFAULT=${cc.default ? "YES" : "NO"},AUTOSELECT=${cc.auto ? "YES" : "NO"},INSTREAM-ID="${cc.id}"\n`;
      });
    }
    if (this.use_demuxed_audio === true && this._audioTracks) {
      if (audioGroupIds.length > 0) {
        m3u8 += "# AUDIO groups\n";
        for (let i = 0; i < audioGroupIds.length; i++) {
          let audioGroupId = audioGroupIds[i];
          for (let j = 0; j < this._audioTracks.length; j++) {
            let audioTrack = this._audioTracks[j];
            // Make default track if set property is true.
            if (audioTrack.default) {
              m3u8 += `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="${audioGroupId}",LANGUAGE="${audioTrack.language}", NAME="${audioTrack.name}",AUTOSELECT=YES,DEFAULT=YES,CHANNELS="2",URI="master-${audioGroupId}_${audioTrack.language}.m3u8;session=${this._sessionId}"\n`;
            } else {
              m3u8 += `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="${audioGroupId}",LANGUAGE="${audioTrack.language}", NAME="${audioTrack.name}",AUTOSELECT=YES,DEFAULT=NO,CHANNELS="2",URI="master-${audioGroupId}_${audioTrack.language}.m3u8;session=${this._sessionId}"\n`;
            }
          }
        }
        // As of now, by default set StreamItem's AUDIO attribute to <first audio group-id>
        defaultAudioGroupId = audioGroupIds[0];
      }
    }
    if (this._sessionProfile) {
      const sessionProfile = filter ? applyFilter(this._sessionProfile, filter) : this._sessionProfile;
      sessionProfile.forEach(profile => {
        m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + ',RESOLUTION=' + profile.resolution[0] + 'x' + profile.resolution[1] + ',CODECS="' + profile.codecs + '"' + (defaultAudioGroupId ? `,AUDIO="${defaultAudioGroupId}"` : '') + (hasClosedCaptions ? ',CLOSED-CAPTIONS="cc"' : '') + '\n';
        m3u8 += "master" + profile.bw + ".m3u8;session=" + this._sessionId + "\n";
      });
    } else {
      currentVod.getUsageProfiles().forEach(profile => {
        m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + ',RESOLUTION=' + profile.resolution + ',CODECS="' + profile.codecs + '"' + (defaultAudioGroupId ? `,AUDIO="${defaultAudioGroupId}"` : '') + (hasClosedCaptions ? ',CLOSED-CAPTIONS="cc"' : '') + '\n';
        m3u8 += "master" + profile.bw + ".m3u8;session=" + this._sessionId + "\n";
      });
    }
    if (this.use_demuxed_audio === true && this._audioTracks) {
      for (let i = 0; i < audioGroupIds.length; i++) {
        let audioGroupId = audioGroupIds[i];
        for (let j = 0; j < this._audioTracks.length; j++) {
          let audioTrack = this._audioTracks[j];
          m3u8 += `#EXT-X-STREAM-INF:BANDWIDTH=97000,CODECS="mp4a.40.2",AUDIO="${audioGroupId}"\n`;
          m3u8 += `master-${audioGroupId}_${audioTrack.language}.m3u8;session=${this._sessionId}\n`;
        }
      }
    }
    this.produceEvent({
      type: 'NOW_PLAYING',
      data: {
        id: this.currentMetadata.id,
        title: this.currentMetadata.title,
      }
    });
    this._sessionState.set("tsLastRequestMaster", Date.now());
    return m3u8;
  }

  async getAudioGroupsAndLangs() {
    const currentVod = await this._sessionState.getCurrentVod();
    if (!currentVod) {
      throw new Error('Session not ready');
    }
    const audioGroupIds = currentVod.getAudioGroups();
    let allAudioGroupsAndTheirLanguages = {};
    audioGroupIds.forEach((groupId) => {
      allAudioGroupsAndTheirLanguages[groupId] =
        currentVod.getAudioLangsForAudioGroup(groupId);
    });
    
    return allAudioGroupsAndTheirLanguages;
  }

  consumeEvent() {
    return this._events.shift();
  }

  produceEvent(event) {
    this._events.push(event);
  }

  hasPlayhead() {
    return !this.disabledPlayhead;
  }

  async _insertSlate(currentVod) {
    if (this.slateUri) {
      console.error(`[${this._sessionId}]: Will insert slate`);
      const slateVod = await this._loadSlate(currentVod);
      debug(`[${this._sessionId}]: slate loaded`);
      const sessionState = await this._sessionState.getValues(["slateCount", "mediaSeq", "discSeq"]);
      let length = 0;
      let lastDiscontinuity = 0;
      if (currentVod) {
        length = currentVod.getLiveMediaSequencesCount();
        lastDiscontinuity = currentVod.getLastDiscontinuity();
      }
      await this._sessionState.set("vodMediaSeqVideo", 0);
      await this._sessionState.set("vodMediaSeqAudio", 0);
      await this._sessionState.set("state", SessionState.VOD_NEXT_INITIATING);
      await this._sessionState.setCurrentVod(slateVod); 
      await this._sessionState.set("mediaSeq", sessionState.mediaSeq + length);
      await this._sessionState.set("discSeq", sessionState.discSeq + lastDiscontinuity);
      await this._sessionState.set("slateCount", sessionState.slateCount + 1);
      await this._playheadState.set("playheadRef", Date.now());

      cloudWatchLog(!this.cloudWatchLogging, 'engine-session', { event: 'slateInserted', channel: this._sessionId });

      return slateVod;
    } else {
      return null;
    }
  }

  async _tickAsync() {
    let newVod;

    let sessionState = await this._sessionState.getValues(
      ["state", "assetId", "vodMediaSeqVideo", "vodMediaSeqAudio", "mediaSeq", "discSeq", "nextVod"]);

    let isLeader = await this._sessionStateStore.isLeader(this._instanceId);

    let currentVod = await this._sessionState.getCurrentVod();
    let vodResponse;

    if (!sessionState.state) {
      sessionState.state = SessionState.VOD_INIT;
    }

    if (!isLeader && this.waitingForNextVod) {
      // By now Leader should have added the next Vod in store
      debug(`[${this._sessionId}]: Not leader! New VOD loaded during last tick. Invalidate current VOD cache`);
      await this._sessionState.clearCurrentVodCache();
      this.waitingForNextVod = false;
    }

    switch (sessionState.state) {
      case SessionState.VOD_INIT:
      case SessionState.VOD_INIT_BY_ID:
        try {
          // Needed if store was reset
          await this._sessionStateStore.clearLeaderCache();
          isLeader = await this._sessionStateStore.isLeader(this._instanceId);

          let nextVodPromise;
          if (sessionState.state === SessionState.VOD_INIT) {
            debug(`[${this._sessionId}]: state=VOD_INIT`);
            nextVodPromise = this._getNextVod();
          } else if (sessionState.state === SessionState.VOD_INIT_BY_ID) {
            debug(`[${this._sessionId}]: state=VOD_INIT_BY_ID ${sessionState.assetId}`);
            nextVodPromise = this._getNextVodById(sessionState.assetId);
          }
          if (isLeader) {
            const nextVodStart = Date.now();
            vodResponse = await nextVodPromise;
            sessionState.nextVod = await this._sessionState.set("nextVod", vodResponse);
            cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
              { event: 'nextVod', channel: this._sessionId, reqTimeMs: Date.now() - nextVodStart });
            let loadPromise;
            if (!vodResponse.type) {
              debug(`[${this._sessionId}]: got first VOD uri=${vodResponse.uri}:${vodResponse.offset || 0}`);
              newVod = new HLSVod(vodResponse.uri, [], null, vodResponse.offset * 1000, m3u8Header(this._instanceId));
              if (vodResponse.timedMetadata) {
                Object.keys(vodResponse.timedMetadata).map(k => {
                  newVod.addMetadata(k, vodResponse.timedMetadata[k]);
                })
              }
              currentVod = newVod;
              loadPromise = currentVod.load();
            } else {
              if (vodResponse.type === 'gap') {
                loadPromise = new Promise((resolve, reject) => {
                  this._fillGap(null, vodResponse.desiredDuration)
                    .then(gapVod => {
                      currentVod = gapVod;
                      resolve(gapVod);
                    }).catch(reject);
                });
              }
            }
            const loadStart = Date.now();
            await loadPromise;
            cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
              { event: 'loadVod', channel: this._sessionId, loadTimeMs: Date.now() - loadStart });
            debug(`[${this._sessionId}]: first VOD loaded`);
            debug(`[${this._sessionId}]: ${currentVod.getDeltaTimes()}`);
            debug(`[${this._sessionId}]: ${currentVod.getPlayheadPositions()}`);
            //debug(newVod);
            sessionState.mediaSeq = await this._sessionState.set("mediaSeq", 0);
            sessionState.discSeq = await this._sessionState.set("discSeq", 0);
            sessionState.vodMediaSeqVideo = await this._sessionState.set("vodMediaSeqVideo", 0);
            sessionState.vodMediaSeqAudio = await this._sessionState.set("vodMediaSeqAudio", 0);
            await this._playheadState.set("playheadRef", Date.now());
            this.produceEvent({
              type: 'NOW_PLAYING',
              data: {
                id: this.currentMetadata.id,
                title: this.currentMetadata.title,
              }
            });
            sessionState.state = await this._sessionState.set("state", SessionState.VOD_PLAYING);
            sessionState.currentVod = await this._sessionState.setCurrentVod(currentVod, { ttl: currentVod.getDuration() * 1000 });
            await this._sessionState.remove("nextVod");
            return;
          } else {
            debug(`[${this._sessionId}]: not a leader so will go directly to state VOD_PLAYING`);
            sessionState.state = await this._sessionState.set("state", SessionState.VOD_PLAYING);
            sessionState.currentVod = await this._sessionState.getCurrentVod();
            return;
          }
        } catch (err) {
          console.error(`[${this._sessionId}]: Failed to init first VOD`);
          if (this._assetManager.handleError) {
            this._assetManager.handleError(new Error("Failed to init first VOD"), vodResponse);
          }
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'error', on: 'firstvod', channel: this._sessionId, err: err, vod: vodResponse });
          debug(err);
          await this._sessionState.remove("nextVod");
          currentVod = await this._insertSlate(currentVod);
          if (!currentVod) {
            debug("No slate to load");
            throw err;
          }
        }
      case SessionState.VOD_PLAYING:
        if (!isLeader) {
          if (sessionState.vodMediaSeqVideo === 0 || this.waitingForNextVod) {
            debug(`[${this._sessionId}]: Not leader! Invalidate current VOD cache and fetch the new one from the leader`);
            await this._sessionState.clearCurrentVodCache();
            currentVod = await this._sessionState.getCurrentVod();
            this.waitingForNextVod = false;
          }
        } else {
          // Handle edge case where store has been reset, but leader has not cleared cache.
          if (!this.prevVodMediaSeq.video) {
            this.prevVodMediaSeq.video = sessionState.vodMediaSeqVideo;
          }
          if (this.prevVodMediaSeq.video < sessionState.vodMediaSeqVideo) {
            debug(`[${this._sessionId}]: current[${sessionState.vodMediaSeqVideo}], prev[${this.prevVodMediaSeq.video}], total[${currentVod.getLiveMediaSequencesCount()}]`);
            await this._sessionState.clearCurrentVodCache();
            currentVod = await this._sessionState.getCurrentVod();
            this.prevVodMediaSeq.video = sessionState.vodMediaSeqVideo;
          }
        }
        debug(`[${this._sessionId}]: state=VOD_PLAYING (${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio}, ${currentVod.getLiveMediaSequencesCount()})`);
        return;
      case SessionState.VOD_NEXT_INITIATING:
        debug(`[${this._sessionId}]: state=VOD_NEXT_INITIATING (${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio}, ${currentVod.getLiveMediaSequencesCount()})`);
        if (!isLeader) {
          debug(`[${this._sessionId}]: not the leader so just waiting for the VOD to be initiated`);
          if (sessionState.vodMediaSeqVideo === 0 || this.waitingForNextVod) {
            debug(`[${this._sessionId}]: First mediasequence in VOD and I am not the leader so invalidate current VOD cache and fetch the new one from the leader`);
            await this._sessionState.clearCurrentVodCache();
          }
          this.waitingForNextVod = true;
        }
        return;
      case SessionState.VOD_NEXT_INIT:
        try {
          debug(`[${this._sessionId}]: state=VOD_NEXT_INIT`);
          if (isLeader) {
            if (!currentVod) {
              debug(`[${this._sessionId}]: no VOD to append to, assume first VOD to init`);
              sessionState.state = await this._sessionState.set("state", SessionState.VOD_INIT);
              return;
            }
            const length = currentVod.getLiveMediaSequencesCount();
            const lastDiscontinuity = currentVod.getLastDiscontinuity();
            sessionState.state = await this._sessionState.set("state", SessionState.VOD_NEXT_INITIATING);
            let vodPromise = this._getNextVod();
            if (length === 1) {
              // Add a grace period for very short VODs before calling nextVod
              const gracePeriod = (this.averageSegmentDuration / 2);
              debug(`[${this._sessionId}]: adding a grace period before calling nextVod: ${gracePeriod}ms`);
              await timer(gracePeriod);
            }
            const nextVodStart = Date.now();
            vodResponse = await vodPromise;
            sessionState.nextVod = await this._sessionState.set("nextVod", vodResponse);
            cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
              { event: 'nextVod', channel: this._sessionId, reqTimeMs: Date.now() - nextVodStart });
            let loadPromise;
            if (!vodResponse.type) {
              debug(`[${this._sessionId}]: got next VOD uri=${vodResponse.uri}:${vodResponse.offset}`);
              newVod = new HLSVod(vodResponse.uri, null, null, vodResponse.offset * 1000, m3u8Header(this._instanceId));
              if (vodResponse.timedMetadata) {
                Object.keys(vodResponse.timedMetadata).map(k => {
                  newVod.addMetadata(k, vodResponse.timedMetadata[k]);
                })
              }
              this.produceEvent({
                type: 'NEXT_VOD_SELECTED',
                data: {
                  id: this.currentMetadata.id,
                  uri: vodResponse.uri,
                  title: this.currentMetadata.title || '',
                }
              });
              loadPromise = newVod.loadAfter(currentVod);
              if (vodResponse.diffMs) {
                this.diffCompensation = vodResponse.diffMs;
              }
            } else {
              loadPromise = new Promise((resolve, reject) => {
                this._fillGap(currentVod, vodResponse.desiredDuration)
                  .then(gapVod => {
                    newVod = gapVod;
                    resolve(newVod);
                  }).catch(reject);
              });
            }
            const loadStart = Date.now();
            await ChaosMonkey.loadVod(loadPromise);
            cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
              { event: 'loadVod', channel: this._sessionId, loadTimeMs: Date.now() - loadStart });
            this.leaderIsSettingNextVod = true;
            debug(`[${this._sessionId}]: next VOD loaded (${newVod.getDeltaTimes()})`);
            debug(`[${this._sessionId}]: ${newVod.getPlayheadPositions()}`);
            currentVod = newVod;
            debug(`[${this._sessionId}]: msequences=${currentVod.getLiveMediaSequencesCount()}`);
            sessionState.vodMediaSeqVideo = await this._sessionState.set("vodMediaSeqVideo", 0);
            sessionState.vodMediaSeqAudio = await this._sessionState.set("vodMediaSeqAudio", 0);
            sessionState.mediaSeq = await this._sessionState.set("mediaSeq", sessionState.mediaSeq + length);
            sessionState.discSeq = await this._sessionState.set("discSeq", sessionState.discSeq + lastDiscontinuity);
            debug(`[${this._sessionId}]: new sequence data set in store [${sessionState.mediaSeq}][${sessionState.discSeq}]`);
            await this._sessionState.remove("nextVod");
            sessionState.currentVod = await this._sessionState.setCurrentVod(currentVod, { ttl: currentVod.getDuration() * 1000 });
            this.leaderIsSettingNextVod = false;
            await this._playheadState.set("playheadRef", Date.now());
            this.produceEvent({
              type: 'NOW_PLAYING',
              data: {
                id: this.currentMetadata.id,
                title: this.currentMetadata.title,
              }
            });
            return;
          } else {
            debug(`[${this._sessionId}]: not a leader so will go directly to state VOD_NEXT_INITIATING`);
            this.waitingForNextVod = true;
            sessionState.state = await this._sessionState.set("state", SessionState.VOD_NEXT_INITIATING);
            sessionState.currentVod = await this._sessionState.getCurrentVod();
          }
        } catch (err) {
          console.error(`[${this._sessionId}]: Failed to init next VOD`);
          debug(`[${this._sessionId}]: ${err}`);
          if (this._assetManager.handleError) {
            this._assetManager.handleError(new Error("Failed to init next VOD"), vodResponse);
          }
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'error', on: 'nextvod', channel: this._sessionId, err: err, vod: vodResponse });
          await this._sessionState.remove("nextVod");
          currentVod = await this._insertSlate(currentVod);
          if (!currentVod) {
            debug("No slate to load");
            throw err;
          }
        }
        break;
        case SessionState.VOD_RELOAD_INIT:
          try {
            debug(`[${this._sessionId}]: state=VOD_RELOAD_INIT`);
            if (isLeader) {

              const startTS = Date.now();

              // 1) To tell Follower that, Leader is working on it!
              sessionState.state = await this._sessionState.set("state", SessionState.VOD_RELOAD_INITIATING);

              // 2) Set new 'offset' sequences, to carry on the continuity from session-live 
              const mSeq = this.switchDataForSession.mediaSeq;
              const dSeq = this.switchDataForSession.discSeq;
              const mSeqOffset = this.switchDataForSession.mediaSeqOffset;
              const reloadBehind = this.switchDataForSession.reloadBehind;
              const segments = this.switchDataForSession.transitionSegments;

              if ([mSeq,dSeq,mSeqOffset,reloadBehind,segments].includes(null)) {
                debug(`[${this._sessionId}]: LEADER: Cannot Reload VOD, missing switch-back data`);
                return;
              }

              await this._sessionState.set("mediaSeq", mSeq);
              await this._playheadState.set("mediaSeq", mSeq);
              await this._sessionState.set("discSeq", dSeq);
              debug(`[${this._sessionId}]: Setting current media and discontinuity count -> [${mSeq}]:[${dSeq}]`);

              // 3) Set new media segments/currentVod, to carry on the continuity from session-live
              debug(`[${this._sessionId}]: LEADER: making changes to current VOD. I will also update currentVod in store.`);
              const playheadState = await this._playheadState.getValues(["vodMediaSeqVideo"]);
              let currentVod = await this._sessionState.getCurrentVod();
              let currentMseq = playheadState.vodMediaSeqVideo + mSeqOffset;
              if (currentMseq > currentVod.getLiveMediaSequencesCount() - 1) {
                currentMseq = currentVod.getLiveMediaSequencesCount() - 1;
              }

              // TODO: Support reloading with audioSegments as well

              await currentVod.reload(currentMseq, segments, null, reloadBehind);
              await this._sessionState.setCurrentVod(currentVod, { ttl: currentVod.getDuration() * 1000 });
              await this._sessionState.set("vodReloaded", 1);
              await this._sessionState.set("vodMediaSeqVideo", 0);
              await this._sessionState.set("vodMediaSeqAudio", 0);
              await this._playheadState.set("vodMediaSeqVideo", 0);
              await this._playheadState.set("vodMediaSeqAudio", 0);
              await this._playheadState.set("playheadRef", Date.now());
              debug(`[${this._sessionId}]: LEADER: Set new Reloaded VOD and vodMediaSeq counts in store.`);
              // 4) emit cloudwatch event object
              cloudWatchLog(!this.cloudWatchLogging, "engine-session", { event: "switchback", channel: this._sessionId, reqTimeMs: Date.now() - startTS });

              return;
            } else {
              debug(`[${this._sessionId}]: not a leader so will go directly to state VOD_RELOAD_INITIATING`);
              sessionState.state = await this._sessionState.set("state", SessionState.VOD_RELOAD_INITIATING);
            }
          } catch (err) {
            debug("Failed to init reload vod");
            throw err;
          } 
        break;
        case SessionState.VOD_RELOAD_INITIATING:
          debug(`[${this._sessionId}]: state=VOD_RELOAD_INITIATING (${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio}, ${currentVod.getLiveMediaSequencesCount()})`);
          if (!isLeader) {
            debug(`[${this._sessionId}]: not the leader so just waiting for the VOD to be reloaded`);
            if (sessionState.vodMediaSeqVideo === 0 || this.waitingForNextVod) {
              debug(`[${this._sessionId}]: First mediasequence in VOD and I am not the leader so invalidate current VOD cache and fetch the new one from the leader`);
              await this._sessionState.clearCurrentVodCache();
            }
            this.waitingForNextVod = true;
          }
          return;
      default:
        throw new Error("Invalid state: " + sessionState.state);
    }
  }

  _getNextVod() {
    return new Promise((resolve, reject) => {
      let nextVodPromise;

      nextVodPromise = this._assetManager.getNextVod({
        sessionId: this._sessionId,
        category: this._category,
        playlistId: this._sessionId
      });

      nextVodPromise.then(nextVod => {
        if (nextVod && nextVod.uri) {
          this.currentMetadata = {
            id: nextVod.id,
            title: nextVod.title || '',
          };
          resolve(nextVod);
        } else if (nextVod && nextVod.type === 'gap') {
          this.currentMetadata = {
            id: 'GAP',
            title: 'GAP of ' + Math.floor(nextVod.desiredDuration) + ' sec',
          };
          resolve(nextVod);
        } else {
          console.error("Invalid VOD:", nextVod);
          reject("Invalid VOD from asset manager")
        }
      })
        .catch(reject);
    });
  }

  _loadSlate(afterVod, reps) {
    return new Promise((resolve, reject) => {
      try {
        const slateVod = new HLSRepeatVod(this.slateUri, reps || this.slateRepetitions);
        let hlsVod;

        slateVod.load()
        .then(() => {
          hlsVod = new HLSVod(this.slateUri, null, null, null, m3u8Header(this._instanceId));
          const timestamp = Date.now();
          hlsVod.addMetadata('id', `slate-${timestamp}`);
          hlsVod.addMetadata('start-date', new Date(timestamp).toISOString());
          hlsVod.addMetadata('planned-duration', ((reps || this.slateRepetitions) * this.slateDuration) / 1000);
          const slateMediaManifestLoader = (bw) => {
            let mediaManifestStream = new Readable();
            mediaManifestStream.push(slateVod.getMediaManifest(bw));
            mediaManifestStream.push(null);
            return mediaManifestStream;
          };
          if (afterVod) {
            return hlsVod.loadAfter(afterVod, null, slateMediaManifestLoader);
          } else {
            return hlsVod.load(null, slateMediaManifestLoader);
          }
        })
        .then(() => {
          resolve(hlsVod);
        })
        .catch(err => {
          debug(err);
          reject(err);
        });
      } catch (err) {
        reject(err);
      }
    });
  }

  _truncateSlate(afterVod, requestedDuration, vodUri) {
    return new Promise((resolve, reject) => {
      let nexVodUri = null;
      try {
        if (vodUri) {
          nexVodUri = vodUri;
        } else {
          nexVodUri = this.slateUri;
        }
        const slateVod = new HLSTruncateVod(nexVodUri, requestedDuration);
        let hlsVod;

        slateVod.load()
        .then(() => {
          hlsVod = new HLSVod(nexVodUri, null, null, null, m3u8Header(this._instanceId));
          const timestamp = Date.now();
          hlsVod.addMetadata('id', `slate-${timestamp}`);
          hlsVod.addMetadata('start-date', new Date(timestamp).toISOString());
          hlsVod.addMetadata('planned-duration', requestedDuration);
          const slateMediaManifestLoader = (bw) => {
            let mediaManifestStream = new Readable();
            mediaManifestStream.push(slateVod.getMediaManifest(bw));
            mediaManifestStream.push(null);
            return mediaManifestStream;
          };
          if (afterVod) {
            return hlsVod.loadAfter(afterVod, null, slateMediaManifestLoader);
          } else {
            return hlsVod.load(null, slateMediaManifestLoader);
          }
        })
        .then(() => {
          resolve(hlsVod);
        })
        .catch(err => {
          debug(err);
          reject(err);
        });
      } catch (err) {
        reject(err);
      }
    });
  }

  _fillGap(afterVod, desiredDuration) {
    return new Promise((resolve, reject) => {
      let loadSlatePromise;
      let durationMs;
      if (desiredDuration > this.slateDuration) {
        const reps = Math.floor(desiredDuration / this.slateDuration);
        debug(`[${this._sessionId}]: Trying to fill a gap of ${desiredDuration} milliseconds (${reps} repetitions)`);
        loadSlatePromise = this._loadSlate(afterVod, reps);
        durationMs = (reps || this.slateRepetitions) * this.slateDuration;
      } else {
        debug(`[${this._sessionId}]: Trying to fill a gap of ${desiredDuration} milliseconds by truncating filler slate (${this.slateDuration})`);
        loadSlatePromise = this._truncateSlate(afterVod, desiredDuration / 1000);
        durationMs = desiredDuration;
      }
      loadSlatePromise.then(hlsVod => {
        cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
          { event: 'filler', channel: this._sessionId, durationMs: durationMs });
        resolve(hlsVod);
      }).catch(reject);
    });
  }

  _getNextVodById(id) {
    return new Promise((resolve, reject) => {
      this._assetManager.getNextVodById(this._sessionId, id)
      .then(nextVod => {
        //debug(nextVod);
        this.currentMetadata = {
          id: nextVod.id,
          title: nextVod.title || '',
        };
        resolve(nextVod);
      })
      .catch(reject);
    });
  }

  _getFirstDuration(manifest) {
    return new Promise((resolve, reject) => {
      try {
        const parser = m3u8.createStream();
        let manifestStream = new Readable();
        manifestStream.push(manifest);
        manifestStream.push(null);

        manifestStream.pipe(parser);
        parser.on('m3u', m3u => {
          if (m3u.items.PlaylistItem[0]) {
            let firstDuration = m3u.items.PlaylistItem[0].get("duration");
            resolve(firstDuration);
          } else {
            console.error("Empty media playlist");
            console.error(manifest);
            reject('Empty media playlist!')
          }
        });
      } catch (err) {
        reject(err);
      }
    });
  }

  async _getCurrentDeltaTime() {
    const sessionState = await this._sessionState.getValues(["vodMediaSeqVideo"]);
    const currentVod = await this._sessionState.getCurrentVod();
    const deltaTimes = currentVod.getDeltaTimes();
    debug(`[${this._sessionId}]: Current delta time (${sessionState.vodMediaSeqVideo}): ${deltaTimes[sessionState.vodMediaSeqVideo]}`);
    if (deltaTimes[sessionState.vodMediaSeqVideo]) {
      return deltaTimes[sessionState.vodMediaSeqVideo];
    }
    return 0;
  }

  async _getCurrentPlayheadPosition() {
    const sessionState = await this._sessionState.getValues(["vodMediaSeqVideo"]);
    const currentVod = await this._sessionState.getCurrentVod();
    const playheadPositions = currentVod.getPlayheadPositions();
    debug(`[${this._sessionId}]: Current playhead position (${sessionState.vodMediaSeqVideo}): ${playheadPositions[sessionState.vodMediaSeqVideo]}`);
    return playheadPositions[sessionState.vodMediaSeqVideo];
  }
}

module.exports = Session;
