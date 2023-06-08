const crypto = require('crypto');
const debug = require('debug')('engine-session');
const HLSVod = require('@eyevinn/hls-vodtolive');
const m3u8 = require('@eyevinn/m3u8');
const HLSRepeatVod = require('@eyevinn/hls-repeat');
const HLSTruncateVod = require('@eyevinn/hls-truncate');
const Readable = require('stream').Readable;

const { SessionState } = require('./session_state.js');
const { PlayheadState } = require('./playhead_state.js');

const { applyFilter, cloudWatchLog, m3u8Header, logerror, codecsFromString } = require('./util.js');
const ChaosMonkey = require('./chaos_monkey.js');

const AVERAGE_SEGMENT_DURATION = 3000;
const DEFAULT_PLAYHEAD_DIFF_THRESHOLD = 1000;
const DEFAULT_MAX_TICK_INTERVAL = 10000;
const DEFAULT_DIFF_COMPENSATION_RATE = 0.5;

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
    this.use_vtt_subtitles = false;
    this.dummySubtitleEndpoint = "";
    this.subtitleSliceEndpoint = "";
    this.cloudWatchLogging = false;
    this.playheadDiffThreshold = DEFAULT_PLAYHEAD_DIFF_THRESHOLD;
    this.maxTickInterval = DEFAULT_MAX_TICK_INTERVAL;
    this.maxTickIntervalIsDefault = true;
    this.diffCompensationRate = DEFAULT_DIFF_COMPENSATION_RATE;
    this.diffCompensation = null;
    this.timePositionOffset = 0;
    this.prevVodMediaSeq = {
      video: null,
      audio: null,
      subtitle: null
    }
    this.prevMediaSeqOffset = {
      video: null,
      audio: null,
      subtitle: null
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
    this.isAllowedToClearVodCache = null;
    this.alwaysNewSegments = null;
    if (config) {
      if (config.alwaysNewSegments) {
        this.alwaysNewSegments = config.alwaysNewSegments;
      }

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
      if (config.dummySubtitleEndpoint) {
        this.dummySubtitleEndpoint = config.dummySubtitleEndpoint;
      }
      if (config.subtitleSliceEndpoint) {
        this.subtitleSliceEndpoint = config.subtitleSliceEndpoint;
      }
      if (config.useVTTSubtitles) {
        this.use_vtt_subtitles = config.useVTTSubtitles;
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
      if (config.subtitleTracks) {
        this._subtitleTracks = config.subtitleTracks;
      }
      if (config.closedCaptions) {
        this._closedCaptions = config.closedCaptions;
      }
      if (config.noSessionDataTags) {
        this._noSessionDataTags = config.noSessionDataTags;
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
        this.maxTickIntervalIsDefault = false;
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
      if (config.diffCompensationRate) {
        this.diffCompensationRate = config.diffCompensationRate;
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
    let numberOfLargeTicks = 0;
    let audioIncrement = 1;
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
          this._playheadState.set("tickInterval", tickInterval, isLeader);
        } else if (state == PlayheadState.STOPPED) {
          debug(`[${this._sessionId}]: Stopping playhead`);
          return;
        } else {
          const reqTickInterval = playheadState.tickInterval;
          const timeSpentInIncrement = (tsIncrementEnd - tsIncrementBegin) / 1000;
          let tickInterval = reqTickInterval - timeSpentInIncrement;
          // Apply HLSVod delta time for current msequence.
          const delta = await this._getCurrentDeltaTime();
          if (delta != 0) {
            tickInterval += delta;
            debug(`[${this._sessionId}]: Delta time is != 0 need will adjust ${delta}sec to tick interval. tick=${tickInterval}`);
          }
          const position = (await this._getCurrentPlayheadPosition()) * 1000;
          let timePosition = Date.now() - playheadState.playheadRef;
          // Apply time position offset if set, only after external diff compensation has concluded.
          if (this.timePositionOffset && this.diffCompensation <= 0 && this.alwaysNewSegments) {
            timePosition -= this.timePositionOffset;
            cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
              { event: 'applyTimePositionOffset', channel: this._sessionId, offsetMs: this.timePositionOffset });
          }
          const diff = position - timePosition;
          debug(`[${this._sessionId}]: ${timePosition}:${position}:${diff > 0 ? '+' : ''}${diff}ms`);
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'playheadDiff', channel: this._sessionId, diffMs: diff });
          if (this.alwaysNewSegments) {
            // Apply Playhead diff compensation, only after external diff compensation has concluded.
            if (this.diffCompensation <= 0) {
              const timeToAdd = this._getPlayheadDiffCompensationValue(diff, this.playheadDiffThreshold);
              tickInterval += timeToAdd;
            }
          } else {
            // Apply Playhead diff compensation, always.
            const timeToAdd = this._getPlayheadDiffCompensationValue(diff, this.playheadDiffThreshold);
            tickInterval += timeToAdd;
          }
          // Apply external diff compensation if available.
          if (this.diffCompensation && this.diffCompensation > 0) {
            const DIFF_COMPENSATION = (reqTickInterval * this.diffCompensationRate).toFixed(2) * 1000;
            debug(`[${this._sessionId}]: Adding ${DIFF_COMPENSATION}msec to tickInterval to compensate for schedule diff (current=${this.diffCompensation}msec)`);
            tickInterval += (DIFF_COMPENSATION / 1000);
            this.diffCompensation -= DIFF_COMPENSATION;
          }
          // Keep tickInterval within upper and lower limits.
          debug(`[${this._sessionId}]: Requested tickInterval=${tickInterval}s (max=${this.maxTickInterval / 1000}s, diffThreshold=${this.playheadDiffThreshold}msec)`);
          if (tickInterval <= 0.5) {
            tickInterval = 0.5;
          } else if (tickInterval > (this.maxTickInterval / 1000)) {
            const changeMaxTick = Math.ceil(Math.abs(tickInterval * 1000 - (this.maxTickInterval))) + 1000;
            if (this.maxTickIntervalIsDefault) {
              if (numberOfLargeTicks > 2) {
                this.maxTickInterval += changeMaxTick;
                numberOfLargeTicks = 0;
              } else {
                numberOfLargeTicks++;
              }
            } else {
              console.warn(`[${this._sessionId}]: Playhead tick interval went over Max tick interval by ${changeMaxTick}ms.
              If the value keeps increasing, consider increasing the 'maxTickInterval' in engineOptions`);
            }
            tickInterval = this.maxTickInterval / 1000;
          }
          debug(`[${this._sessionId}]: (${(new Date()).toISOString()}) ${timeSpentInIncrement}sec in increment. Next tick in ${tickInterval} seconds`)
          await timer((tickInterval * 1000) - 50);
          const tsTickEnd = Date.now();
          await this._playheadState.set("tickMs", (tsTickEnd - tsIncrementBegin), isLeader);
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'tickInterval', channel: this._sessionId, tickTimeMs: (tsTickEnd - tsIncrementBegin) });
          if (this.alwaysNewSegments) {
            // Use dynamic base-tickInterval. Set according to duration of latest segment.
            const lastDuration = await this._getLastDuration(manifest);
            const nextTickInterval = lastDuration < 2 ? 2 : lastDuration;
            await this._playheadState.set("tickInterval", nextTickInterval, isLeader);
            cloudWatchLog(!this.cloudWatchLogging, "engine-session", { event: "tickIntervalUpdated", channel: this._sessionId, tickIntervalSec: nextTickInterval });
          }
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
    const isLeader = await this._sessionStateStore.isLeader(this._instanceId);
    debug(`[${this._sessionId}]: Stopping playhead consumer`);
    await this._playheadState.set("state", PlayheadState.STOPPED, isLeader);
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

    let waitTimeMs = 2000;
    for (let i = segments[Object.keys(segments)[0]].length - 1; 0 < i; i--) {
      if (segments[Object.keys(segments)[0]][i].duration) {
        waitTimeMs = parseInt(1000 * (segments[Object.keys(segments)[0]][i].duration / 3), 10);
        break;
      }
    }

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
      await this._sessionState.set("state", SessionState.VOD_RELOAD_INIT);

    } else {
      let vodReloaded = await this._sessionState.get("vodReloaded");
      let attempts = 12;
      while (!vodReloaded && attempts > 0) {
        debug(`[${this._sessionId}]: LEADER: Waiting (${waitTimeMs}ms) to buy some time reloading vod and adding it to store! (tries left=${attempts})`);
        await timer(waitTimeMs);
        vodReloaded = await this._sessionState.get("vodReloaded");
        attempts--;
      }
      if (attempts === 0) {
        debug(`[${this._sessionId}]: LEADER: WARNING! Vod was never Reloaded!`);
        return;
      }
    }
  }

  async getCurrentMediaSequenceSegments(opts) {
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

    const playheadState = {
      vodMediaSeqVideo: null
    }
    if (opts && opts.targetMseq !== undefined) {
      playheadState.vodMediaSeqVideo = opts.targetMseq;
    } else {
      playheadState.vodMediaSeqVideo = await this._playheadState.get("vodMediaSeqVideo");
    }

    // NOTE: Assume that VOD cache was already cleared in 'getCurrentMediaAndDiscSequenceCount()'
    // and that we now have access to the correct vod cache
    const currentVod = await this._sessionState.getCurrentVod();
    if (currentVod) {
      try {
        const mediaSegments = currentVod.getLiveMediaSequenceSegments(playheadState.vodMediaSeqVideo);

        let mediaSequenceValue = 0;
        if (currentVod.sequenceAlwaysContainNewSegments) {
          mediaSequenceValue = currentVod.mediaSequenceValues[playheadState.vodMediaSeqVideo];
          debug(`[${this._sessionId}]: {${mediaSequenceValue}}_{${currentVod.getLastSequenceMediaSequenceValue()}}`);
        } else {
          mediaSequenceValue = playheadState.vodMediaSeqVideo;
        }

        debug(`[${this._sessionId}]: Requesting all segments from Media Sequence: ${playheadState.vodMediaSeqVideo}(${mediaSequenceValue})_${currentVod.getLiveMediaSequencesCount()}`);
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
    // TODO: support Audio too ^

    // Clear Vod Cache here when Switching to Live just to be safe...
    if (playheadState.vodMediaSeqVideo === 0) {
      const isLeader = await this._sessionStateStore.isLeader(this._instanceId);
      if (!isLeader) {
        debug(`[${this._sessionId}]: Not a leader, about to switch, and first media sequence in a VOD is requested. Invalidate cache to ensure having the correct VOD.`);
        await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
      }
    }
    const currentVod = await this._sessionState.getCurrentVod();
    if (currentVod) {
      try {
        let mediaSequenceValue = 0;
        if (currentVod.sequenceAlwaysContainNewSegments) {
          mediaSequenceValue = currentVod.mediaSequenceValues[playheadState.vodMediaSeqVideo];
          debug(`[${this._sessionId}]: seqIndex=${playheadState.vodMediaSeqVideo}_seqValue=${mediaSequenceValue}`)
        } else {
          mediaSequenceValue = playheadState.vodMediaSeqVideo;
        }
        const discSeqCount = discSeqOffset + currentVod.discontinuities[playheadState.vodMediaSeqVideo];

        debug(`[${this._sessionId}]: MediaSeq: (${playheadState.mediaSeq}+${mediaSequenceValue}=${(playheadState.mediaSeq + mediaSequenceValue)}) and DiscSeq: (${discSeqCount}) requested `);
        return {
          'mediaSeq': (playheadState.mediaSeq + mediaSequenceValue),
          'discSeq': discSeqCount,
          'vodMediaSeqVideo': playheadState.vodMediaSeqVideo,
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
    let playheadState = await this._playheadState.getValues(["mediaSeq", "vodMediaSeqVideo"]);

    if (this.prevVodMediaSeq.video === null) {
      this.prevVodMediaSeq.video = playheadState.vodMediaSeqVideo;
    }
    if (this.prevMediaSeqOffset.video === null) {
      this.prevMediaSeqOffset.video = playheadState.mediaSeq;
    }

    if (playheadState.vodMediaSeqVideo > sessionState.vodMediaSeqVideo || (playheadState.vodMediaSeqVideo < sessionState.vodMediaSeqVideo && playheadState.mediaSeq === this.prevMediaSeqOffset.video)) {
      const state = await this._sessionState.get("state");
      const DELAY_TIME_MS = 1000;
      const ACTION = [SessionState.VOD_RELOAD_INIT, SessionState.VOD_RELOAD_INITIATING].includes(state) ? "Reloaded" : "Loaded Next";
      debug(`[${this._sessionId}]: Recently ${ACTION} Vod. PlayheadState not up-to-date (${playheadState.vodMediaSeqVideo}_${sessionState.vodMediaSeqVideo}). Waiting ${DELAY_TIME_MS}ms before reading from store again`);
      await timer(DELAY_TIME_MS);
      playheadState = await this._playheadState.getValues(["mediaSeq", "vodMediaSeqVideo"]);
    }

    // Force reading up from store, but only once if the condition is right
    if (playheadState.vodMediaSeqVideo < 2 || playheadState.mediaSeq !== this.prevMediaSeqOffset.video) {
      debug(`[${this._sessionId}]: current[${playheadState.vodMediaSeqVideo}]_prev[${this.prevVodMediaSeq.video}]`);
      debug(`[${this._sessionId}]: current-offset[${playheadState.mediaSeq}]_prev-offset[${this.prevMediaSeqOffset.video}]`);
      // If true, then we have not updated the prev-values and not cleared the cache yet.
      if (playheadState.vodMediaSeqVideo < this.prevVodMediaSeq.video || playheadState.mediaSeq !== this.prevMediaSeqOffset.video) {
        this.isAllowedToClearVodCache = true;
      }
      const isLeader = await this._sessionStateStore.isLeader(this._instanceId);
      if (!isLeader && this.isAllowedToClearVodCache) {
        debug(`[${this._sessionId}]: Not a leader and first|second media sequence in a VOD is requested. Invalidate cache to ensure having the correct VOD.`);
        await this._sessionState.clearCurrentVodCache();
        this.isAllowedToClearVodCache = false;
        const diffMs = await this._playheadState.get("diffCompensation");
        if (diffMs) {
          this.diffCompensation = diffMs;
          debug(`[${this._sessionId}]: Setting diffCompensation->${this.diffCompensation}`);
          if (this.diffCompensation) {
            this.timePositionOffset = this.diffCompensation;
            cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
              { event: 'timePositionOffsetUpdated', channel: this._sessionId, offsetMs: this.timePositionOffset });
          } else {
            this.timePositionOffset = 0;
          }
        }
      }
    } else {
      this.isAllowedToClearVodCache = true;
    }

    const currentVod = await this._sessionState.getCurrentVod();
    if (currentVod) {
      try {
        let manifestMseq = playheadState.mediaSeq + playheadState.vodMediaSeqVideo;
        let manifestDseq = sessionState.discSeq + currentVod.discontinuities[playheadState.vodMediaSeqVideo];
        if (currentVod.sequenceAlwaysContainNewSegments) {
          const mediaSequenceValue = currentVod.mediaSequenceValues[playheadState.vodMediaSeqVideo];
          debug(`[${this._sessionId}]: {${mediaSequenceValue}}_{${currentVod.getLastSequenceMediaSequenceValue()}}`);
          manifestMseq = playheadState.mediaSeq + mediaSequenceValue;
        }

        debug(`[${this._sessionId}]: [${playheadState.vodMediaSeqVideo}]_[${currentVod.getLiveMediaSequencesCount()}]`);
        const m3u8 = currentVod.getLiveMediaSequences(playheadState.mediaSeq, bw, playheadState.vodMediaSeqVideo, sessionState.discSeq, this.targetDurationPadding, this.forceTargetDuration);
        debug(`[${this._sessionId}]: [${manifestMseq}][${manifestDseq}][+${this.targetDurationPadding || 0}] Current media manifest for ${bw} requested`);
        this.prevVodMediaSeq.video = playheadState.vodMediaSeqVideo;
        this.prevMediaSeqOffset.video = playheadState.mediaSeq;
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
    let currentVod = null;
    const sessionState = await this._sessionState.getValues(["discSeqAudio", "vodMediaSeqAudio"]);
    let playheadState = await this._playheadState.getValues(["mediaSeqAudio", "vodMediaSeqAudio"]);

    if (playheadState.vodMediaSeqAudio > sessionState.vodMediaSeqAudio || (playheadState.vodMediaSeqAudio < sessionState.vodMediaSeqAudio && playheadState.mediaSeqAudio === this.prevMediaSeqOffset.audio)) {
      const state = await this._sessionState.get("state");
      const DELAY_TIME_MS = 1000;
      const ACTION = [SessionState.VOD_RELOAD_INIT, SessionState.VOD_RELOAD_INITIATING].includes(state) ? "Reloaded" : "Loaded Next";
      debug(`[${this._sessionId}]: Recently ${ACTION} Vod. PlayheadState not up-to-date (${playheadState.vodMediaSeqAudio}_${sessionState.vodMediaSeqAudio}). Waiting ${DELAY_TIME_MS}ms before reading from store again`);
      await timer(DELAY_TIME_MS);
      playheadState = await this._playheadState.getValues(["mediaSeqAudio", "vodMediaSeqAudio"]);
    }
    // local store the prev values
    if (this.prevVodMediaSeq.audio === null) {
      this.prevVodMediaSeq.audio = playheadState.vodMediaSeqAudio;
    }
    if (this.prevMediaSeqOffset.audio === null) {
      this.prevMediaSeqOffset.audio = playheadState.mediaSeqAudio;
    }
    currentVod = await this._sessionState.getCurrentVod();
    if (currentVod) {
      // condition suggesting that a new vod should exist
      if (playheadState.vodMediaSeqAudio < 2 || playheadState.mediaSeqAudio !== this.prevMediaSeqOffset.audio) {
        const AGE_THRESH = this.averageSegmentDuration * 2;
        let cacheAge = null;
        if (this._sessionState.cache && this._sessionState.cache.currentVod.ts) {
          cacheAge = Date.now() - this._sessionState.cache.currentVod.ts;
        }
        if (cacheAge !== null && cacheAge > AGE_THRESH) {
          await timer(500);
          debug(`[${this._sessionId}]: While requesting audio manifest for ${audioGroupId}-${audioLanguage}, (mseq=${playheadState.vodMediaSeqAudio})(vod cache age=${cacheAge})`);
          await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
          currentVod = await this._sessionState.getCurrentVod();
        }
      }
      try {
        let manifestMseq = playheadState.mediaSeqAudio + playheadState.vodMediaSeqAudio;
        let manifestDseq = sessionState.discSeqAudio + currentVod.discontinuitiesAudio[playheadState.vodMediaSeqAudio];
        if (currentVod.sequenceAlwaysContainNewSegments) {
          const mediaSequenceValue = currentVod.mediaSequenceValuesAudio[playheadState.vodMediaSeqAudio];
          debug(`[${this._sessionId}]: {${mediaSequenceValue}}_{${currentVod.getLastSequenceMediaSequenceValueAudio()}} AUDIO`);
          manifestMseq = playheadState.mediaSeqAudio + mediaSequenceValue;
        }
        debug(`[${this._sessionId}]: [${playheadState.vodMediaSeqAudio}]_[${currentVod.getLiveMediaSequencesCount("audio")}] AUDIO (${audioGroupId})`);
        const m3u8 = currentVod.getLiveMediaAudioSequences(
          playheadState.mediaSeqAudio,
          audioGroupId,
          audioLanguage,
          playheadState.vodMediaSeqAudio,
          sessionState.discSeqAudio,
          this.targetDurationPadding,
          this.forceTargetDuration
        );
        // # Case: current VOD does not have the selected track.
        if (!m3u8) {
          debug(`[${this._sessionId}]: [${playheadState.mediaSeqAudio + playheadState.vodMediaSeqAudio}] Request Failed for current audio manifest for ${audioGroupId}-${audioLanguage}`);
        }
        debug(`[${this._sessionId}]: [${manifestMseq}][${manifestDseq}] Current audio manifest for ${audioGroupId}-${audioLanguage} requested`);
        this.prevVodMediaSeq.audio = playheadState.vodMediaSeqAudio;
        this.prevMediaSeqOffset.audio = playheadState.mediaSeqAudio;
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
  
  async getCurrentSubtitleManifestAsync(subtitleGroupId, subtitleLanguage) {
    if (!this._sessionState) {
      throw new Error('Session not ready');
    }
    let currentVod = null;
    const sessionState = await this._sessionState.getValues(["discSeqSubtitle", "vodMediaSeqSubtitle"]);
    let playheadState = await this._playheadState.getValues(["mediaSeqSubtitle", "vodMediaSeqSubtitle"]);

    if (playheadState.vodMediaSeqSubtitle > sessionState.vodMediaSeqSubtitle || (playheadState.vodMediaSeqSubtitle < sessionState.vodMediaSeqSubtitle && playheadState.mediaSeqSubtitle === this.prevMediaSeqOffset.subtitle)) {
      const state = await this._sessionState.get("state");
      const DELAY_TIME_MS = 1000;
      const ACTION = [SessionState.VOD_RELOAD_INIT, SessionState.VOD_RELOAD_INITIATING].includes(state) ? "Reloaded" : "Loaded Next";
      debug(`[${this._sessionId}]: Recently ${ACTION} Vod. PlayheadState not up-to-date (${playheadState.vodMediaSeqSubtitle}_${sessionState.vodMediaSeqSubtitle}). Waiting ${DELAY_TIME_MS}ms before reading from store again`);
      await timer(DELAY_TIME_MS);
      playheadState = await this._playheadState.getValues(["mediaSeqSubtitle", "vodMediaSeqSubtitle"]);
    }
    // local store the prev values
    if (this.prevVodMediaSeq.subtitle === null) {
      this.prevVodMediaSeq.subtitle = playheadState.vodMediaSeqSubtitle;
    }
    if (this.prevMediaSeqOffset.subtitle === null) {
      this.prevMediaSeqOffset.subtitle = playheadState.mediaSeqSubtitle;
    }
    currentVod = await this._sessionState.getCurrentVod();
    if (currentVod) {
      // condition suggesting that a new vod should exist
      if (playheadState.vodMediaSeqSubtitle < 2 || playheadState.mediaSeqSubtitle !== this.prevMediaSeqOffset.subtitle) {
        const AGE_THRESH = this.averageSegmentDuration * 2;
        let cacheAge = null;
        if (this._sessionState.cache && this._sessionState.cache.currentVod.ts) {
          cacheAge = Date.now() - this._sessionState.cache.currentVod.ts;
        }
        if (cacheAge !== null && cacheAge > AGE_THRESH) {
          await timer(500);
          debug(`[${this._sessionId}]: While requesting subtitle manifest for ${subtitleGroupId}-${subtitleLanguage}, (mseq=${playheadState.vodMediaSeqSubtitle})(vod cache age=${cacheAge})`);
          await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
          currentVod = await this._sessionState.getCurrentVod();
        }
      }
      try {
        let manifestMseq = playheadState.mediaSeqSubtitle + playheadState.vodMediaSeqSubtitle;
        let manifestDseq = sessionState.discSeqSubtitle + currentVod.discontinuitiesSubtitle[playheadState.vodMediaSeqSubtitle];
        if (currentVod.sequenceAlwaysContainNewSegments) {
          const mediaSequenceValue = currentVod.mediaSequenceValuesSubtitle[playheadState.vodMediaSeqSubtitle];
          debug(`[${this._sessionId}]: {${mediaSequenceValue}}_{${currentVod.getLastSequenceMediaSequenceValueSubtitle()}} SUBTITLES`);
          manifestMseq = playheadState.mediaSeqSubtitle + mediaSequenceValue;
        }
        debug(`[${this._sessionId}]: [${playheadState.vodMediaSeqSubtitle}]_[${currentVod.getLiveMediaSequencesCount("subtitle")}] SUBTITLES (${subtitleGroupId})`);
        const m3u8 = currentVod.getLiveMediaSubtitleSequences(
          playheadState.mediaSeqSubtitle,
          subtitleGroupId,
          subtitleLanguage,
          playheadState.vodMediaSeqSubtitle,
          sessionState.discSeqSubtitle,
          this.targetDurationPadding,
          this.forceTargetDuration
        );
        // # Case: current VOD does not have the selected track.
        if (!m3u8) {
          debug(`[${this._sessionId}]: [${playheadState.mediaSeqSubtitle + playheadState.vodMediaSeqSubtitle}] Request Failed for current subtitle manifest for ${subtitleGroupId}-${subtitleLanguage}`);
        }
        debug(`[${this._sessionId}]: [${manifestMseq}][${manifestDseq}] Current subtitle manifest for ${subtitleGroupId}-${subtitleLanguage} requested`);
        this.prevVodMediaSeq.subtitle = playheadState.vodMediaSeqSubtitle;
        this.prevMediaSeqOffset.subtitle = playheadState.mediaSeqSubtitle;
        return m3u8;
      } catch (err) {
        logerror(this._sessionId, err);
        await this._sessionState.clearCurrentVodCache(); // force reading up from shared store
        throw new Error("Failed to generate subtitle manifest: " + JSON.stringify(playheadState));
      }
    } else {
      throw new Error("Engine not ready");
    }
  }

  async incrementAsync() {
    await this._tickAsync();
    const isLeader = await this._sessionStateStore.isLeader(this._instanceId);

    let sessionState = await this._sessionState.getValues(
      ["state", "mediaSeq", "mediaSeqAudio", "mediaSeqSubtitle", "discSeq", "discSeqAudio", "discSeqSubtitle", "vodMediaSeqVideo", "vodMediaSeqAudio", "vodMediaSeqSubtitle"]);
    let playheadState = await this._playheadState.getValues(["mediaSeq", "mediaSeqAudio", "mediaSeqSubtitle", "vodMediaSeqVideo", "vodMediaSeqAudio", "vodMediaSubtitle", "playheadRef"]);
    let currentVod = await this._sessionState.getCurrentVod();
    if (!currentVod ||
      sessionState.vodMediaSeqVideo === null ||
      sessionState.vodMediaSeqAudio === null ||
      sessionState.vodMediaSeqSubtitle === null ||
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
        const isOldVod = this._isOldVod(playheadState.playheadRef, currentVod.getDuration());
        let nextState = SessionState.VOD_PLAYING;
        if (isOldVod) {
          nextState = SessionState.VOD_NEXT_INIT;
        }
        const leaderAction = sessionState.state === SessionState.VOD_NEXT_INITIATING ? "initiated" : "reloaded";
        const leaderState = nextState === SessionState.VOD_NEXT_INIT ? "VOD_NEXT_INIT" : "VOD_PLAYING";
        debug(`[${this._sessionId}]: I am the leader and have just ${leaderAction} next VOD, let's move to  ${leaderState}`);
        sessionState.state = await this._sessionState.set("state", nextState);
      } else {
        debug(`[${this._sessionId}]: Return the last generated m3u8 to give the leader some time`);
        let m3u8 = await this._playheadState.getLastM3u8();
        if (m3u8) {
          return m3u8;
        } else {
          debug(`[${this._sessionId}]: We don't have any previously generated m3u8`);
        }
        this.isAllowedToClearVodCache = true;
      }
    } else {
      sessionState.vodMediaSeqVideo = await this._sessionState.increment("vodMediaSeqVideo", 1);
      let audioIncrement;
      if (this.use_demuxed_audio) {
        let positionV = 0;
        let positionA = 0;
        const position = (await this._getCurrentPlayheadPosition()) * 1000;
        positionV = position / 1000;
        let currentVod = await this._sessionState.getCurrentVod();
        const sessionState = await this._sessionState.getValues(["vodMediaSeqAudio"]);
        let posDiff;
        let incrementValue = 1;
        let index = 0;
        const audioSeqLastIdx = currentVod.getLiveMediaSequencesCount("audio") - 1;
        const thresh = 0.5;
        debug(`[${this._sessionId}]: About to determine audio increment`);
        do {
          const audioPosition = (await this._getAudioPlayheadPosition(sessionState.vodMediaSeqAudio + index)) * 1000;
          positionA = audioPosition / 1000;
          posDiff = position - audioPosition;
          if (posDiff <= 0) {
            break;
          }
          if (posDiff > thresh) {
            index++;
            incrementValue++;
          } else {
            index = incrementValue;
            break;
          }
          if (sessionState.vodMediaSeqAudio + index > audioSeqLastIdx) {
            break;
          }
        } while (!(-thresh < posDiff && posDiff < thresh));
        audioIncrement = index;
        debug(`[${this._sessionId}]: Current VOD Playhead Positions are to be: [${positionV.toFixed(3)}][${positionA.toFixed(3)}] (${(positionA-positionV).toFixed(3)})`);
      }
      debug(`[${this._sessionId}]: Will increment audio with ${audioIncrement}`);
      sessionState.vodMediaSeqAudio = await this._sessionState.increment("vodMediaSeqAudio", audioIncrement);

      if (this.use_vtt_subtitles) {
        debug(`[${this._sessionId}]: Will increment subtitle with 1`);
        sessionState.vodMediaSeqSubtitle = await this._sessionState.increment("vodMediaSeqSubtitle", 1);
      }
    }

    if (sessionState.vodMediaSeqVideo >= currentVod.getLiveMediaSequencesCount() - 1) {
      sessionState.vodMediaSeqVideo = await this._sessionState.set("vodMediaSeqVideo", currentVod.getLiveMediaSequencesCount() - 1);
      sessionState.state = await this._sessionState.set("state", SessionState.VOD_NEXT_INIT);
    }

    if (sessionState.vodMediaSeqAudio >= currentVod.getLiveMediaSequencesCount("audio") - 1) {
      sessionState.vodMediaSeqAudio = await this._sessionState.set("vodMediaSeqAudio", currentVod.getLiveMediaSequencesCount("audio") - 1);
    }

    if (sessionState.vodMediaSeqSubtitle >= currentVod.getLiveMediaSequencesCount("subtitle") - 1) {
      sessionState.vodMediaSeqSubtitle = await this._sessionState.set("vodMediaSeqSubtitle", currentVod.getLiveMediaSequencesCount("subtitle") - 1);
    }

    if (this.isSwitchingBackToV2L) {
      sessionState.state = await this._sessionState.set("state", SessionState.VOD_RELOAD_INIT);
      this.isSwitchingBackToV2L = false;
    }

    if (isLeader) {
      debug(`[${this._sessionId}]: I am the leader, updating PlayheadState values`);
    }
    playheadState.mediaSeq = await this._playheadState.set("mediaSeq", sessionState.mediaSeq, isLeader);
    playheadState.mediaSeqAudio = await this._playheadState.set("mediaSeqAudio", sessionState.mediaSeqAudio, isLeader);
    playheadState.mediaSeqSubtitle = await this._playheadState.set("mediaSeqSubtitle", sessionState.mediaSeqSubtitle, isLeader);
    playheadState.vodMediaSeqVideo = await this._playheadState.set("vodMediaSeqVideo", sessionState.vodMediaSeqVideo, isLeader);
    playheadState.vodMediaSeqAudio = await this._playheadState.set("vodMediaSeqAudio", sessionState.vodMediaSeqAudio, isLeader);
    playheadState.vodMediaSeqSubtitle = await this._playheadState.set("vodMediaSeqSubtitle", sessionState.vodMediaSeqSubtitle, isLeader);

    if (currentVod.sequenceAlwaysContainNewSegments) {
      const mediaSequenceValue = currentVod.mediaSequenceValues[playheadState.vodMediaSeqVideo];
      let audioInfo = "";
      if (this.use_demuxed_audio) {
        const mediaSequenceValueAudio = currentVod.mediaSequenceValuesAudio[playheadState.vodMediaSeqAudio];
        audioInfo = ` mseq[A]={${playheadState.mediaSeqAudio + mediaSequenceValueAudio}}`
      }
      debug(`[${this._sessionId}]: Session can now serve mseq[V]={${playheadState.mediaSeq + mediaSequenceValue}}` + audioInfo);
    }

    debug(`[${this._sessionId}]: INCREMENT (mseq=${playheadState.mediaSeq + playheadState.vodMediaSeqVideo}) vodMediaSeq=(${playheadState.vodMediaSeqVideo}_${playheadState.vodMediaSeqAudio} of ${currentVod.getLiveMediaSequencesCount()}_${currentVod.getLiveMediaSequencesCount("audio")})`);

    // As a FOLLOWER, we might need to read up from shared store... 
    if (playheadState.vodMediaSeqVideo < 2 || playheadState.mediaSeq !== this.prevMediaSeqOffset.video) {
      debug(`[${this._sessionId}]: current[${playheadState.vodMediaSeqVideo}]_prev[${this.prevVodMediaSeq.video}]`);
      debug(`[${this._sessionId}]: current-offset[${playheadState.mediaSeq}]_prev-offset[${this.prevMediaSeqOffset.video}]`);
      if (playheadState.vodMediaSeqVideo < this.prevVodMediaSeq.video || playheadState.mediaSeq !== this.prevMediaSeqOffset.video) {
        this.isAllowedToClearVodCache = true;
      }
      if (!isLeader && this.isAllowedToClearVodCache) {
        debug(`[${this._sessionId}]: Not a leader and have just set 'playheadState.vodMediaSeqVideo' to 0|1. Invalidate cache to ensure having the correct VOD.`);
        await this._sessionState.clearCurrentVodCache();
        currentVod = await this._sessionState.getCurrentVod();
        const diffMs = await this._playheadState.get("diffCompensation");
        if (diffMs) {
          this.diffCompensation = diffMs;
          debug(`[${this._sessionId}]: Setting diffCompensation->${this.diffCompensation}`);
          if (this.diffCompensation) {
            this.timePositionOffset = this.diffCompensation;
            cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
              { event: 'timePositionOffsetUpdated', channel: this._sessionId, offsetMs: this.timePositionOffset });
          } else {
            this.timePositionOffset = 0;
          }
        }
        this.isAllowedToClearVodCache = false;
      }
    } else {
      this.isAllowedToClearVodCache = true;
    }
    // Update Value for Previous Sequence
    this.prevVodMediaSeq.video = playheadState.vodMediaSeqVideo;
    this.prevMediaSeqOffset.video = playheadState.mediaSeq;
    if (this.use_demuxed_audio) {
      this.prevVodMediaSeq.audio = playheadState.vodMediaSeqAudio;
      this.prevMediaSeqOffset.audio = playheadState.mediaSeqAudio;
    }

    if (this.use_vtt_subtitles) {
      this.prevVodMediaSeq.subtitle = playheadState.vodMediaSeqSubtitle;
      this.prevMediaSeqOffset.subtitle = playheadState.mediaSeqSubtitle;
    }

    let m3u8 = currentVod.getLiveMediaSequences(playheadState.mediaSeq, 180000, playheadState.vodMediaSeqVideo, sessionState.discSeq);
    await this._playheadState.setLastM3u8(m3u8);
    return m3u8;
  }

  async getMediaManifestAsync(bw, opts) { // this function is no longer used and should be removed comment added 5/5-2023
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

  async getAudioManifestAsync(audioGroupId, audioLanguage, opts) { // this function is no longer used and should be removed comment added 5/5-2023
    const tsLastRequestAudio = await this._sessionState.get("tsLastRequestAudio");
    let timeSinceLastRequest = (tsLastRequestAudio === null) ? 0 : Date.now() - tsLastRequestAudio;

    let sessionState = await this._sessionState.getValues(
      ["state", "vodMediaSeqVideo", "vodMediaSeqAudio", "mediaSeqAudio", "discSeqAudio", "lastM3u8"]);
    const currentVod = await this._sessionState.getCurrentVod();
    if (sessionState.state !== SessionState.VOD_NEXT_INITIATING) {
      let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);

      if (sessionState.vodMediaSeqAudio < sessionState.vodMediaSeqVideo) {
        sessionState.vodMediaSeqAudio = await this._sessionState.increment("vodMediaSeqAudio", sequencesToIncrement);
        if (sessionState.vodMediaSeqAudio >= currentVod.getLiveMediaSequencesCount("audio") - 1) {
          sessionState.vodMediaSeqAudio = await this._sessionState.set("vodMediaSeqAudio", currentVod.getLiveMediaSequencesCount("audio") - 1);
        }
      }
    }

    debug(`[${this._sessionId}]: AUDIO ${timeSinceLastRequest} (${this.averageSegmentDuration}) audioGroupId=${audioGroupId} audioLanguage=${audioLanguage} vodMediaSeq=(${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio})`);
    let m3u8;
    try {
      m3u8 = currentVod.getLiveMediaAudioSequences(sessionState.mediaSeqAudio, audioGroupId, audioLanguage, sessionState.vodMediaSeqAudio, sessionState.discSeqAudio);
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
    sessionState.lastM3u8 = await this._sessionState.set("lastM3u8", lastM3u8); // for audio?
    sessionState.tsLastRequestAudio = await this._sessionState.set("tsLastRequestAudio", Date.now());
    return m3u8;
  }

  async getMasterManifestAsync(filter) {
    let m3u8 = "#EXTM3U\n";
    m3u8 += "#EXT-X-VERSION:4\n";
    m3u8 += m3u8Header(this._instanceId);
    if (!this._noSessionDataTags) {
      m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.session.id",VALUE="${this._sessionId}"\n`;
      m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.eventstream",VALUE="/eventstream/${this._sessionId}"\n`;
    }
    const currentVod = await this._sessionState.getCurrentVod();
    if (!currentVod) {
      throw new Error('Session not ready');
    }
    let audioGroupIds = currentVod.getAudioGroups();
    debug(`[${this._sessionId}]: currentVod.getAudioGroups()=${audioGroupIds.join(",")}`);
    let defaultAudioGroupId;
    let defaultSubtitleGroupId;
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
            const [_, channels] = currentVod.getAudioCodecsAndChannelsForGroupId(audioGroupId);
            // Make default track if set property is true.
            let audioGroupIdFileName = audioGroupId;
            if (audioTrack.enforceAudioGroupId) {
              audioGroupIdFileName = audioTrack.enforceAudioGroupId;
            }
            m3u8 += `#EXT-X-MEDIA:TYPE=AUDIO` +
              `,GROUP-ID="${audioGroupId}"` +
              `,LANGUAGE="${audioTrack.language}"` +
              `,NAME="${audioTrack.name}"` +
              `,AUTOSELECT=YES,DEFAULT=${audioTrack.default ? 'YES' : 'NO'}` +
              `,CHANNELS="${channels ? channels : 2}"` +
              `,URI="master-${audioGroupIdFileName}_${audioTrack.language}.m3u8%3Bsession=${this._sessionId}"` +
              "\n";
          }
        }
        // As of now, by default set StreamItem's AUDIO attribute to <first audio group-id>
        defaultAudioGroupId = audioGroupIds[0];
      }
    }
    if (this.use_vtt_subtitles) {
      let subtitleGroupIds = currentVod.getSubtitleGroups();
      if (subtitleGroupIds.length > 0) {
        m3u8 += "# Subtitle groups\n";
        for (let i = 0; i < subtitleGroupIds.length; i++) {
          let subtitleGroupId = subtitleGroupIds[i];
          for (let j = 0; j < this._subtitleTracks.length; j++) {
            let subtitleTrack = this._subtitleTracks[j];
            // Make default track if set property is true.  TODO add enforce
            m3u8 += `#EXT-X-MEDIA:TYPE=SUBTITLES` +
              `,GROUP-ID="${subtitleGroupId}"` +
              `,LANGUAGE="${subtitleTrack.language}"` +
              `,NAME="${subtitleTrack.name}"` +
              `,AUTOSELECT=YES,DEFAULT=${subtitleTrack.default ? 'YES' : 'NO'}` +
              `,URI="subtitles-${subtitleGroupId}_${subtitleTrack.language}.m3u8%3Bsession=${this._sessionId}"` +
              "\n";
          }
        }
        // As of now, by default set StreamItem's SUBTITLES attribute to <first subtitle group-id>
        defaultSubtitleGroupId = subtitleGroupIds[0];
      }
    }
    if (this._sessionProfile) {
      const sessionProfile = filter ? applyFilter(this._sessionProfile, filter) : this._sessionProfile;
      sessionProfile.forEach(profile => {
        if (this.use_demuxed_audio) {
          // find matching audio group based on codec in stream
          let audioGroupIdToUse;
          const [_, audioCodec] = codecsFromString(profile.codecs);
          if (audioCodec) {
            const profileChannels = profile.channels ? profile.channels : "2";
            audioGroupIdToUse = currentVod.getAudioGroupIdForCodecs(audioCodec, profileChannels);
            if (!audioGroupIds.includes(audioGroupIdToUse)) {
              audioGroupIdToUse = defaultAudioGroupId; 
            }
          }

          debug(`[${this._sessionId}]: audioGroupIdToUse=${audioGroupIdToUse}`);

          // skip stream if no corresponding audio group can be found
          if (audioGroupIdToUse) {
            m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + 
              ',RESOLUTION=' + profile.resolution[0] + 'x' + profile.resolution[1] + 
              ',CODECS="' + profile.codecs + '"' + 
              `,AUDIO="${audioGroupIdToUse}"` + 
              (defaultSubtitleGroupId ? `,SUBTITLES="${defaultSubtitleGroupId}"` : '') + 
              (hasClosedCaptions ? ',CLOSED-CAPTIONS="cc"' : '') + '\n';
            m3u8 += "master" + profile.bw + ".m3u8%3Bsession=" + this._sessionId + "\n";
          }
        } else {
          m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + 
            ',RESOLUTION=' + profile.resolution[0] + 'x' + profile.resolution[1] + 
            ',CODECS="' + profile.codecs + '"' + 
            (defaultAudioGroupId ? `,AUDIO="${defaultAudioGroupId}"` : '') + 
            (defaultSubtitleGroupId ? `,SUBTITLES="${defaultSubtitleGroupId}"` : '') + 
            (hasClosedCaptions ? ',CLOSED-CAPTIONS="cc"' : '') + '\n';
          m3u8 += "master" + profile.bw + ".m3u8%3Bsession=" + this._sessionId + "\n";
        }
      });
    } else {
      currentVod.getUsageProfiles().forEach(profile => {
        m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + 
          ',RESOLUTION=' + profile.resolution + 
          ',CODECS="' + profile.codecs + '"' + 
          (defaultAudioGroupId ? `,AUDIO="${defaultAudioGroupId}"` : '') + 
          (defaultSubtitleGroupId ? `,SUBTITLES="${defaultSubtitleGroupId}"` : '') +
          (hasClosedCaptions ? ',CLOSED-CAPTIONS="cc"' : '') + '\n';
        m3u8 += "master" + profile.bw + ".m3u8%3Bsession=" + this._sessionId + "\n";
      });
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

  async getSubtitleGroupsAndLangs() {
    const currentVod = await this._sessionState.getCurrentVod();
    if (!currentVod) {
      throw new Error('Session not ready');
    }
    const subtitleGroupIds = currentVod.getSubtitleGroups();
    let allSubtitleGroupsAndTheirLanguages = {};
    subtitleGroupIds.forEach((groupId) => {
      allSubtitleGroupsAndTheirLanguages[groupId] =
        currentVod.getSubtitleLangsForSubtitleGroup(groupId);
    });

    return allSubtitleGroupsAndTheirLanguages;
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

  async _insertSlate(currentVod) { // no support for subs
    if (this.slateUri) {
      console.error(`[${this._sessionId}]: Will insert slate`);
      const slateVod = await this._loadSlate(currentVod);
      debug(`[${this._sessionId}]: slate loaded`);
      const sessionState = await this._sessionState.getValues(["slateCount", "mediaSeq", "discSeq", "mediaSeqAudio", "discSeqAudio"]);
      let endValue = 0;
      let endValueAudio = 0;
      let lastDiscontinuity = 0;
      let lastDiscontinuityAudio = 0;
      if (currentVod) {
        if (currentVod.sequenceAlwaysContainNewSegments) {
          endValue = currentVod.getLastSequenceMediaSequenceValue();
          endValueAudio = currentVod.getLastSequenceMediaSequenceValueAudio();
        } else {
          endValue = currentVod.getLiveMediaSequencesCount();
          endValueAudio = currentVod.getLiveMediaSequencesCount("audio");
        }
        lastDiscontinuity = currentVod.getLastDiscontinuity();
        lastDiscontinuityAudio = currentVod.getLastDiscontinuityAudio();
      }
      const isLeader = await this._sessionStateStore.isLeader(this._instanceId);
      await this._sessionState.set("vodMediaSeqVideo", 0);
      await this._sessionState.set("vodMediaSeqAudio", 0);
      await this._sessionState.set("state", SessionState.VOD_NEXT_INITIATING);
      await this._sessionState.setCurrentVod(slateVod);
      await this._sessionState.set("mediaSeq", sessionState.mediaSeq + endValue);
      await this._sessionState.set("mediaSeqAudio", sessionState.mediaSeqAudio + endValueAudio);
      await this._sessionState.set("discSeq", sessionState.discSeq + lastDiscontinuity);
      await this._sessionState.set("discSeqAudio", sessionState.discSeqAudio + lastDiscontinuityAudio);
      await this._sessionState.set("slateCount", sessionState.slateCount + 1);
      await this._playheadState.set("playheadRef", Date.now(), isLeader);

      cloudWatchLog(!this.cloudWatchLogging, 'engine-session', { event: 'slateInserted', channel: this._sessionId });

      return slateVod;
    } else {
      return null;
    }
  }

  async _tickAsync() {
    let newVod;

    let sessionState = await this._sessionState.getValues(
      ["state", "assetId", "vodMediaSeqVideo", "vodMediaSeqAudio", "vodMediaSeqSubtitle", "mediaSeq", "mediaSeqAudio", "mediaSeqSubtitle", "discSeq", "discSeqAudio", "discSeqSubtitle", "nextVod"]);

    let isLeader = await this._sessionStateStore.isLeader(this._instanceId);

    let currentVod = await this._sessionState.getCurrentVod();
    let vodResponse;

    if (!sessionState.state) {
      sessionState.state = SessionState.VOD_INIT;
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
              const hlsOpts = { sequenceAlwaysContainNewSegments: this.alwaysNewSegments, 
                forcedDemuxMode: this.use_demuxed_audio, 
                dummySubtitleEndpoint: this.dummySubtitleEndpoint,
                subtitleSliceEndpoint: this.subtitleSliceEndpoint,
                shouldContainSubtitles: this.use_vtt_subtitles,
                expectedSubtitleTracks: this._subtitleTracks
              };
              newVod = new HLSVod(vodResponse.uri, [], vodResponse.unixTs, vodResponse.offset * 1000, m3u8Header(this._instanceId), hlsOpts);
              if (vodResponse.timedMetadata) {
                Object.keys(vodResponse.timedMetadata).map(k => {
                  newVod.addMetadata(k, vodResponse.timedMetadata[k]);
                })
              }
              currentVod = newVod;
              if (vodResponse.desiredDuration) {
                const { mediaManifestLoader, audioManifestLoader} = await this._truncateVod(vodResponse);
                loadPromise = currentVod.load(null, mediaManifestLoader, audioManifestLoader);
              } else {
                loadPromise = currentVod.load();
              }
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
            debug(`[${this._sessionId}]: playhead positions [V]=${currentVod.getPlayheadPositions("video")}`);
            debug(`[${this._sessionId}]: playhead positions [A]=${currentVod.getPlayheadPositions("audio")}`);
            //debug(newVod);
            sessionState.mediaSeq = await this._sessionState.set("mediaSeq", 0);
            sessionState.mediaSeqAudio = await this._sessionState.set("mediaSeqAudio", 0);
            sessionState.mediaSeqSubtitle = await this._sessionState.set("mediaSeqSubtitle", 0);
            sessionState.discSeq = await this._sessionState.set("discSeq", 0);
            sessionState.discSeqAudio = await this._sessionState.set("discSeqAudio", 0);
            sessionState.discSeqSubtitle = await this._sessionState.set("discSeqSubtitle", 0);
            sessionState.vodMediaSeqVideo = await this._sessionState.set("vodMediaSeqVideo", 0);
            sessionState.vodMediaSeqAudio = await this._sessionState.set("vodMediaSeqAudio", 0);
            sessionState.vodMediaSeqSubtitle = await this._sessionState.set("vodMediaSeqSubtitle", 0);
            await this._playheadState.set("playheadRef", Date.now(), isLeader);
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
        if (isLeader) {
          // Handle edge case where store has been reset, but leader has not cleared cache.
          if (this.prevVodMediaSeq.video === null) {
            this.prevVodMediaSeq.video = sessionState.vodMediaSeqVideo;
          }
          if (this.prevMediaSeqOffset.video === null) {
            this.prevMediaSeqOffset.video = sessionState.mediaSeq;
          }
          if (this.use_demuxed_audio) {
            if (this.prevVodMediaSeq.audio === null) {
              this.prevVodMediaSeq.audio = sessionState.vodMediaSeqAudio;
            }
            if (this.prevMediaSeqOffset.audio === null) {
              this.prevMediaSeqOffset.audio = sessionState.mediaSeqAudio;
            }
          }
          if (this.use_vtt_subtitles) {
            if (this.prevVodMediaSeq.subtitle === null) {
              this.prevVodMediaSeq.subtitle = sessionState.vodMediaSeqSubtitle;
            }
            if (this.prevMediaSeqOffset.audio === null) {
              this.prevMediaSeqOffset.subtitle = sessionState.mediaSeqSubtitle;
            }
          }
          // Clear Cache if prev count is HIGHER than current...
          if (sessionState.vodMediaSeqVideo < this.prevVodMediaSeq.video) {
            debug(`[${this._sessionId}]: state=VOD_PLAYING, current[${sessionState.vodMediaSeqVideo}], prev[${this.prevVodMediaSeq.video}], total[${currentVod.getLiveMediaSequencesCount()}]`);
            await this._sessionState.clearCurrentVodCache();
            currentVod = await this._sessionState.getCurrentVod();
            this.prevVodMediaSeq.video = sessionState.vodMediaSeqVideo;
            this.prevVodMediaSeq.audio = sessionState.vodMediaSeqAudio;
            this.prevVodMediaSeq.subtitle = sessionState.vodMediaSeqSubtitle;
          }
        } else {
          // Handle edge case where Leader loaded next vod but Follower remained in state=VOD_PLAYING
          if ((this.prevMediaSeqOffset.video !== null) & (sessionState.mediaSeq !== this.prevMediaSeqOffset.video)) {
            debug(`[${this._sessionId}]: state=VOD_PLAYING, current[${sessionState.vodMediaSeqVideo}], prev[${this.prevVodMediaSeq.video}], total[${currentVod.getLiveMediaSequencesCount()}]`);
            debug(`[${this._sessionId}]: mediaSeq offsets -> current[${sessionState.vodMediaSeqVideo}], prev[${this.prevVodMediaSeq.video}]`);
            // Allow Follower to clear VodCache...
            this.isAllowedToClearVodCache = true;
          }
        }
        debug(`[${this._sessionId}]: state=VOD_PLAYING (${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio}_${sessionState.vodMediaSeqSubtitle}, ${currentVod.getLiveMediaSequencesCount()}_${currentVod.getLiveMediaSequencesCount("audio")}_${currentVod.getLiveMediaSequencesCount("subtitle")})`);
        return;
      case SessionState.VOD_NEXT_INITIATING:
        debug(`[${this._sessionId}]: state=VOD_NEXT_INITIATING (${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio}_${sessionState.vodMediaSeqSubtitle}, ${currentVod.getLiveMediaSequencesCount()}_${currentVod.getLiveMediaSequencesCount("audio")}_${currentVod.getLiveMediaSequencesCount("subtitle")})`);
        if (!isLeader) {
          debug(`[${this._sessionId}]: not the leader so just waiting for the VOD to be initiated`);
        }
        // Allow Leader|Follower to clear vodCache...
        this.isAllowedToClearVodCache = true;
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
            let endMseqValue;
            let endMseqValueAudio;
            let endMseqValueSubtitle;
            if (currentVod.sequenceAlwaysContainNewSegments) {
              endMseqValue = currentVod.getLastSequenceMediaSequenceValue();
              endMseqValueAudio = currentVod.getLastSequenceMediaSequenceValueAudio();
              endMseqValueSubtitle = currentVod.getLastSequenceMediaSequenceValueSubtitle();
            } else {
              endMseqValue = currentVod.getLiveMediaSequencesCount();
              endMseqValueAudio = currentVod.getLiveMediaSequencesCount("audio");
              endMseqValueSubtitle = currentVod.getLiveMediaSequencesCount("subtitle");
            }
            const lastDiscontinuity = currentVod.getLastDiscontinuity();
            const lastDiscontinuityAudio = currentVod.getLastDiscontinuityAudio();
            const lastDiscontinuitySubtitle = currentVod.getLastDiscontinuitySubtitle();
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
              const hlsOpts = { sequenceAlwaysContainNewSegments: this.alwaysNewSegments,
                forcedDemuxMode: this.use_demuxed_audio, 
                dummySubtitleEndpoint: this.dummySubtitleEndpoint,
                subtitleSliceEndpoint: this.subtitleSliceEndpoint,
                shouldContainSubtitles: this.use_vtt_subtitles,
                expectedSubtitleTracks: this._subtitleTracks
              };
              newVod = new HLSVod(vodResponse.uri, null, vodResponse.unixTs, vodResponse.offset * 1000, m3u8Header(this._instanceId), hlsOpts);
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
              if (vodResponse.desiredDuration) {
                const { mediaManifestLoader, audioManifestLoader} = await this._truncateVod(vodResponse);
                loadPromise = newVod.loadAfter(currentVod, null, mediaManifestLoader, audioManifestLoader);
              } else {
                loadPromise = newVod.loadAfter(currentVod);
              }
              if (vodResponse.diffMs) {
                this.diffCompensation = vodResponse.diffMs;
                if (this.diffCompensation) {
                  this.timePositionOffset = this.diffCompensation;
                  cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
                    { event: 'timePositionOffsetUpdated', channel: this._sessionId, offsetMs: this.timePositionOffset });
                } else {
                  this.timePositionOffset = 0;
                }
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
            debug(`[${this._sessionId}]: playhead positions [V]=${newVod.getPlayheadPositions("video")}`);
            debug(`[${this._sessionId}]: playhead positions [A]=${newVod.getPlayheadPositions("audio")}`);
            currentVod = newVod;
            debug(`[${this._sessionId}]: msequences=${currentVod.getLiveMediaSequencesCount()}; audio msequences=${currentVod.getLiveMediaSequencesCount("audio")}; subtitle msequences=${currentVod.getLiveMediaSequencesCount("subtitle")}`);
            sessionState.vodMediaSeqVideo = await this._sessionState.set("vodMediaSeqVideo", 0);
            sessionState.vodMediaSeqAudio = await this._sessionState.set("vodMediaSeqAudio", 0);
            sessionState.vodMediaSeqSubtitle = await this._sessionState.set("vodMediaSeqSubtitle", 0);
            sessionState.mediaSeq = await this._sessionState.set("mediaSeq", sessionState.mediaSeq + endMseqValue);
            sessionState.mediaSeqAudio = await this._sessionState.set("mediaSeqAudio", sessionState.mediaSeqAudio + endMseqValueAudio);
            sessionState.mediaSeqSubtitle = await this._sessionState.set("mediaSeqSubtitle", sessionState.mediaSeqSubtitle + endMseqValueSubtitle);
            sessionState.discSeq = await this._sessionState.set("discSeq", sessionState.discSeq + lastDiscontinuity);
            sessionState.discSeqAudio = await this._sessionState.set("discSeqAudio", sessionState.discSeqAudio + lastDiscontinuityAudio);
            sessionState.discSeqSubtitle = await this._sessionState.set("discSeqSubtitle", sessionState.discSeqSubtitle + lastDiscontinuitySubtitle);
            debug(`[${this._sessionId}]: new sequence data set in store V[${sessionState.mediaSeq}][${sessionState.discSeq}]_A[${sessionState.mediaSeqAudio}][${sessionState.discSeqAudio}]_S[${sessionState.mediaSeqSubtitle}][${sessionState.discSeqSubtitle}]`);
            await this._sessionState.remove("nextVod");
            sessionState.currentVod = await this._sessionState.setCurrentVod(currentVod, { ttl: currentVod.getDuration() * 1000 });
            this.leaderIsSettingNextVod = false;
            await this._playheadState.set("playheadRef", Date.now(), isLeader);
            await this._playheadState.set("diffCompensation", this.diffCompensation, isLeader);
            debug(`[${this._sessionId}]: sharing durrent vods diffCompensation=${this.diffCompensation}`);
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
            // Allow Leader|Follower to clear vodCache...
            this.isAllowedToClearVodCache = true;
            sessionState.currentVod = await this._sessionState.getCurrentVod();
            const diffMs = await this._playheadState.get("diffCompensation");
            if (diffMs) {
              this.diffCompensation = diffMs;
              debug(`[${this._sessionId}]: Setting diffCompensation=${this.diffCompensation}`);
              if (this.diffCompensation) {
                this.timePositionOffset = this.diffCompensation;
                cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
                  { event: 'timePositionOffsetUpdated', channel: this._sessionId, offsetMs: this.timePositionOffset });
              } else {
                this.timePositionOffset = 0;
              }
            }
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
          // Allow Leader|Follower to clear vodCache...
          this.isAllowedToClearVodCache = true;
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
            let mSeq = this.switchDataForSession.mediaSeq;
            // TODO: support demux^
            let currentVod = await this._sessionState.getCurrentVod();
            if (currentVod.sequenceAlwaysContainNewSegments) {
              // (!) will need to compensate if using this setting on HLSVod Object.
              Object.keys(this.switchDataForSession.transitionSegments).forEach(bw => {
                let shiftedSeg = this.switchDataForSession.transitionSegments[bw].shift();
                if (shiftedSeg && shiftedSeg.discontinuity) {
                  shiftedSeg = this.switchDataForSession.transitionSegments[bw].shift();
                }
              });
            }
            const dSeq = this.switchDataForSession.discSeq;
            const mSeqOffset = this.switchDataForSession.mediaSeqOffset;
            const reloadBehind = this.switchDataForSession.reloadBehind;
            const segments = this.switchDataForSession.transitionSegments;
            if ([mSeq, dSeq, mSeqOffset, reloadBehind, segments].includes(null)) {
              debug(`[${this._sessionId}]: LEADER: Cannot Reload VOD, missing switch-back data`);
              return;
            }
            await this._sessionState.set("mediaSeq", mSeq);
            await this._playheadState.set("mediaSeq", mSeq, isLeader);
            await this._sessionState.set("discSeq", dSeq);
            // TODO: support demux^
            debug(`[${this._sessionId}]: Setting current media and discontinuity count -> [${mSeq}]:[${dSeq}]`);
            // 3) Set new media segments/currentVod, to carry on the continuity from session-live
            debug(`[${this._sessionId}]: LEADER: making changes to current VOD. I will also update currentVod in store.`);
            const playheadState = await this._playheadState.getValues(["vodMediaSeqVideo"]);
            let nextMseq = playheadState.vodMediaSeqVideo + 1;
            if (nextMseq > currentVod.getLiveMediaSequencesCount() - 1) {
              nextMseq = currentVod.getLiveMediaSequencesCount() - 1;
            }

            // ---------------------------------------------------.
            // TODO: Support reloading with audioSegments and SubtitleSegments as well |
            // ---------------------------------------------------'

            await currentVod.reload(nextMseq, segments, null, reloadBehind);
            await this._sessionState.setCurrentVod(currentVod, { ttl: currentVod.getDuration() * 1000 });
            await this._sessionState.set("vodReloaded", 1);
            await this._sessionState.set("vodMediaSeqVideo", 0);
            await this._sessionState.set("vodMediaSeqAudio", 0);
            await this._sessionState.set("vodMediaSeqSubtitle", 0);
            await this._playheadState.set("vodMediaSeqVideo", 0, isLeader);
            await this._playheadState.set("vodMediaSeqAudio", 0, isLeader);
            await this._playheadState.set("vodMediaSeqSubtitle", 0, isLeader);
            await this._playheadState.set("playheadRef", Date.now(), isLeader);
            // 4) Log to debug and cloudwatch
            debug(`[${this._sessionId}]: LEADER: Set new Reloaded VOD and vodMediaSeq counts in store.`);
            debug(`[${this._sessionId}]: next VOD Reloaded (${currentVod.getDeltaTimes()})`);
            debug(`[${this._sessionId}]: ${currentVod.getPlayheadPositions()}`);
            debug(`[${this._sessionId}]: msequences=${currentVod.getLiveMediaSequencesCount()}`);
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
        debug(`[${this._sessionId}]: state=VOD_RELOAD_INITIATING (${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio}_${sessionState.vodMediaSeqSubtitle}, ${currentVod.getLiveMediaSequencesCount()}_${currentVod.getLiveMediaSequencesCount("audio")}_${currentVod.getLiveMediaSequencesCount("subtitle")})`);
        if (!isLeader) {
          debug(`[${this._sessionId}]: not the leader so just waiting for the VOD to be reloaded`);
          if (sessionState.vodMediaSeqVideo === 0 || this.waitingForNextVod) {
            debug(`[${this._sessionId}]: First mediasequence in VOD and I am not the leader so invalidate current VOD cache and fetch the new one from the leader`);
            await this._sessionState.clearCurrentVodCache();
          }
          this.waitingForNextVod = true;
        }
        // Allow Leader|Follower to clear vodCache...
        this.isAllowedToClearVodCache = true;
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
            const hlsOpts = { sequenceAlwaysContainNewSegments: this.alwaysNewSegments, 
              forcedDemuxMode: this.use_demuxed_audio, 
              dummySubtitleEndpoint: this.dummySubtitleEndpoint,
              subtitleSliceEndpoint: this.subtitleSliceEndpoint,
              shouldContainSubtitles: this.use_vtt_subtitles,
              expectedSubtitleTracks: this._subtitleTracks
            };
            const timestamp = Date.now();
            hlsVod = new HLSVod(this.slateUri, null, timestamp, null, m3u8Header(this._instanceId), hlsOpts);
            hlsVod.addMetadata('id', `slate-${timestamp}`);
            hlsVod.addMetadata('start-date', new Date(timestamp).toISOString());
            hlsVod.addMetadata('planned-duration', ((reps || this.slateRepetitions) * this.slateDuration) / 1000);
            const slateMediaManifestLoader = (bw) => {
              let mediaManifestStream = new Readable();
              mediaManifestStream.push(slateVod.getMediaManifest(bw));
              mediaManifestStream.push(null);
              return mediaManifestStream;
            };
            if (this.use_demuxed_audio) {
              const slateAudioManifestLoader = (audioGroupId, audioLanguage) => {
                let mediaManifestStream = new Readable();
                mediaManifestStream.push(slateVod.getAudioManifest(audioGroupId, audioLanguage));
                mediaManifestStream.push(null);
                return mediaManifestStream;
              };
              if (afterVod) {
                return hlsVod.loadAfter(afterVod, null, slateMediaManifestLoader, slateAudioManifestLoader);
              } else {
                return hlsVod.load(null, slateMediaManifestLoader, slateAudioManifestLoader);
              }
            } else {
              if (afterVod) {
                return hlsVod.loadAfter(afterVod, null, slateMediaManifestLoader);
              } else {
                return hlsVod.load(null, slateMediaManifestLoader);
              }
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

  _truncateVod(vodResponse) {
    return new Promise((resolve, reject) => {
      try {
        const truncatedVod = new HLSTruncateVod(vodResponse.uri, vodResponse.desiredDuration / 1000);
        truncatedVod.load()
          .then(() => {
            let audioManifestLoader;
            const mediaManifestLoader = (bw) => {
              let mediaManifestStream = new Readable();
              mediaManifestStream.push(truncatedVod.getMediaManifest(bw));
              mediaManifestStream.push(null);
              return mediaManifestStream;
            };
            if (this.use_demuxed_audio) {
              audioManifestLoader = (audioGroupId, audioLanguage) => {
                let mediaManifestStream = new Readable();
                mediaManifestStream.push(truncatedVod.getAudioManifest(audioGroupId, audioLanguage));
                mediaManifestStream.push(null);
                return mediaManifestStream;
              };
            }
            resolve({ mediaManifestLoader, audioManifestLoader });
          }).catch(err => {
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
            const hlsOpts = { 
              sequenceAlwaysContainNewSegments: this.alwaysNewSegments, 
              forcedDemuxMode: this.use_demuxed_audio, 
              dummySubtitleEndpoint: this.dummySubtitleEndpoint, 
              subtitleSliceEndpoint: this.subtitleSliceEndpoint,
              shouldContainSubtitles: this.use_vtt_subtitles,
              expectedSubtitleTracks: this._subtitleTracks
            };
            const timestamp = Date.now();
            hlsVod = new HLSVod(nexVodUri, null, timestamp, null, m3u8Header(this._instanceId), hlsOpts);
            hlsVod.addMetadata('id', `slate-${timestamp}`);
            hlsVod.addMetadata('start-date', new Date(timestamp).toISOString());
            hlsVod.addMetadata('planned-duration', requestedDuration);
            const slateMediaManifestLoader = (bw) => {
              let mediaManifestStream = new Readable();
              mediaManifestStream.push(slateVod.getMediaManifest(bw));
              mediaManifestStream.push(null);
              return mediaManifestStream;
            };
            if (this.use_demuxed_audio) {
              const slateAudioManifestLoader = (audioGroupId, audioLanguage) => {
                let mediaManifestStream = new Readable();
                mediaManifestStream.push(slateVod.getAudioManifest(audioGroupId, audioLanguage));
                mediaManifestStream.push(null);
                return mediaManifestStream;
              };
              if (afterVod) {
                return hlsVod.loadAfter(afterVod, null, slateMediaManifestLoader, slateAudioManifestLoader);
              } else {
                return hlsVod.load(null, slateMediaManifestLoader, slateAudioManifestLoader);
              }
            } else {
              if (afterVod) {
                return hlsVod.loadAfter(afterVod, null, slateMediaManifestLoader);
              } else {
                return hlsVod.load(null, slateMediaManifestLoader);
              }
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

  async _getAudioPlayheadPosition(seqIdx) {
    const currentVod = await this._sessionState.getCurrentVod();
    const playheadPositions = currentVod.getPlayheadPositions("audio");
    if (seqIdx >= playheadPositions.length - 1) {
      seqIdx = playheadPositions.length - 1
    }
    debug(`[${this._sessionId}]: Current audio playhead position (${seqIdx}): ${playheadPositions[seqIdx]}`);
    return playheadPositions[seqIdx];
  }

  async _getSubtitlePlayheadPosition(seqIdx) {
    const currentVod = await this._sessionState.getCurrentVod();
    const playheadPositions = currentVod.getPlayheadPositions("subtitle");
    if (seqIdx >= playheadPositions.length - 1) {
      seqIdx = playheadPositions.length - 1
    }
    debug(`[${this._sessionId}]: Current subtitle playhead position (${seqIdx}): ${playheadPositions[seqIdx]}`);
    return playheadPositions[seqIdx];
  }

  _getLastDuration(manifest) {
    return new Promise((resolve, reject) => {
      try {
        const parser = m3u8.createStream();
        let manifestStream = new Readable();
        manifestStream.push(manifest);
        manifestStream.push(null);

        manifestStream.pipe(parser);
        parser.on('m3u', m3u => {
          if (m3u.items.PlaylistItem.length > 0) {
            const endIdx = m3u.items.PlaylistItem.length - 1;
            const bottomDuration = m3u.items.PlaylistItem[endIdx].get("duration");
            resolve(bottomDuration);
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

  _getPlayheadDiffCompensationValue(diffMs, thresholdMs) {
    let compensationSec = 0;
    if (diffMs > thresholdMs) {
      compensationSec = (diffMs / 1000) - (thresholdMs / 1000);
      debug(`[${this._sessionId}]: Playhead stepping msequences too early. Need to wait longer. adding ${compensationSec}s`);
      return compensationSec;
    } else if (diffMs < -thresholdMs) {
      compensationSec = (diffMs / 1000) + (thresholdMs / 1000);
      debug(`[${this._sessionId}]: Playhead stepping msequences too LATE. Need to fast-forward. adding ${compensationSec}s`);
      return compensationSec;
    } else {
      return compensationSec;
    }
  }

  _isOldVod(refTs, vodDur) {
    const TIME_PADDING_MS = 20 * 1000 // 20secs
    const VOD_DURATION_MS = vodDur * 1000;
    const TIME_SINCE_VOD_STARTED_MS = (Date.now() - refTs) + TIME_PADDING_MS;
    if (TIME_SINCE_VOD_STARTED_MS > VOD_DURATION_MS) {
      return true;
    }
    return false;
  }
}

module.exports = Session;
