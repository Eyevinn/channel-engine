const crypto = require('crypto');
const debug = require('debug')('engine-session');
const HLSVod = require('@eyevinn/hls-vodtolive');
const m3u8 = require('@eyevinn/m3u8');
const HLSRepeatVod = require('@eyevinn/hls-repeat');
const HLSTruncateVod = require('@eyevinn/hls-truncate');
const Readable = require('stream').Readable;

const { SessionState } = require('./session_state.js');
const { PlayheadState } = require('./playhead_state.js');

const { applyFilter, cloudWatchLog, m3u8Header } = require('./util.js');

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

    //this.currentVod;
    this.currentMetadata = {};
    this._events = [];
    this.averageSegmentDuration = AVERAGE_SEGMENT_DURATION;
    this.use_demuxed_audio = false;
    this.cloudWatchLogging = false;
    this.playheadDiffThreshold = DEFAULT_PLAYHEAD_DIFF_THRESHOLD;
    this.maxTickInterval = DEFAULT_MAX_TICK_INTERVAL;
    this.diffCompensation = null;

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
    }
  }

  async initAsync() {
    await this._sessionStateStore.create(this._sessionId);
    await this._playheadStateStore.create(this._sessionId);

    if (this.startWithId) {
      await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_INIT_BY_ID);
      await this._sessionStateStore.set(this._sessionId, "assetId", this.startWithId);  
    }
  }

  get sessionId() {
    return this._sessionId;
  }

  async getCurrentVod() {
    const sessionState = await this._sessionStateStore.getValues(this._sessionId, ["currentVod"]);
    //debug(`[${this._sessionId}]: ` + sessionState.currentVod ? "currentVod is defined" : "currentVod is null");
    if (sessionState.currentVod) {
      if (this._sessionStateStore.isShared()) {
        let hlsVod = new HLSVod();0
        hlsVod.fromJSON(sessionState.currentVod);
        return hlsVod;
      } else {
        return sessionState.currentVod;
      }
    }
  }

  async setCurrentVod(hlsVod) {
    if (this._sessionStateStore.isShared()) {
      return await this._sessionStateStore.set(this._sessionId, "currentVod", hlsVod.toJSON());
    } else {
      return await this._sessionStateStore.set(this._sessionId, "currentVod", hlsVod);
    }
  }

  async startPlayheadAsync() {
    debug(`[${this._sessionId}]: Playhead consumer started:`); 
    debug(`[${this._sessionId}]:   diffThreshold=${this.playheadDiffThreshold}`);
    debug(`[${this._sessionId}]:   maxTickInterval=${this.maxTickInterval}`);
    debug(`[${this._sessionId}]:   averageSegmentDuration=${this.averageSegmentDuration}`);

    this.disabledPlayhead = false;

    //let playheadState = await this._playheadStateStore.get(this._sessionId);
    let playheadState = await this._playheadStateStore.getValues(this._sessionId, ["state"]);
    playheadState.state = await this._playheadStateStore.set(this._sessionId, "state", PlayheadState.RUNNING);
    while (playheadState.state !== PlayheadState.CRASHED) {
      try {
        const tsIncrementBegin = Date.now();
        const manifest = await this.incrementAsync();
        const tsIncrementEnd = Date.now();
        //const sessionState = await this._sessionStateStore.get(this._sessionId);
        const sessionState = await this._sessionStateStore.getValues(this._sessionId, ["state"]);
        //playheadState = await this._playheadStateStore.get(this._sessionId);
        playheadState = await this._playheadStateStore.getValues(this._sessionId, ["state", "tickInterval", "playheadRef", "tickMs"]);
        if ([SessionState.VOD_NEXT_INIT, SessionState.VOD_NEXT_INITIATING].indexOf(sessionState.state) !== -1) {
          const firstDuration = await this._getFirstDuration(manifest);
          const tickInterval = firstDuration < 2 ? 2 : firstDuration;
          debug(`[${this._sessionId}]: Updated tick interval to ${tickInterval} sec`);
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session', 
            { event: 'tickIntervalUpdated', channel: this._sessionId, tickIntervalSec: tickInterval });
          this._playheadStateStore.set(this._sessionId, "tickInterval", tickInterval);
        } else if (playheadState.state == PlayheadState.STOPPED) {
          debug(`[${this._sessionId}]: Stopping playhead`);
          return;
        } else {
          const reqTickInterval = playheadState.tickInterval;
          const timeSpentInIncrement = (tsIncrementEnd - tsIncrementBegin) / 1000;
          let tickInterval = (reqTickInterval) - timeSpentInIncrement;
          const delta = await this._getCurrentDeltaTime();
          if (delta != 0) {
            debug(`[${this._sessionId}]: Delta time is != 0 need will adjust ${delta}sec to tick interval`);
            tickInterval += delta;
          }
          const position = await this._getCurrentPlayheadPosition() * 1000;
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
          await this._playheadStateStore.set(this._sessionId, "tickMs", (tsTickEnd - tsIncrementBegin));
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session', 
            { event: 'tickInterval', channel: this._sessionId, tickTimeMs: (tsTickEnd - tsIncrementBegin) });
        }
      } catch (err) {
        debug(`[${this._sessionId}]: Playhead consumer crashed (1)`);
        console.error(`[${this._sessionId}]: ${err.message}`);
        cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
          { event: 'error', on: 'playhead', channel: this._sessionId, err: err });
        debug(err);
        playheadState.state = await this._playheadStateStore.set(this._sessionId, "state", PlayheadState.CRASHED);
      }
    }
  }

  async restartPlayheadAsync() {
    await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INIT);
    debug(`[${this._sessionId}]: Restarting playhead consumer`);
    await this.startPlayheadAsync();  
  }

  async stopPlayheadAsync() {
    debug(`[${this._sessionId}]: Stopping playhead consumer`);
    await this._playheadStateStore.set(this._sessionId, "state", PlayheadState.STOPPED);
  }

  async getStatusAsync() {
    if (this.disabledPlayhead) {
      return {
        sessionId: this._sessionId,
      }
    } else {
      //const playheadState = await this._playheadStateStore.get(this._sessionId);
      //const sessionState = await this._sessionStateStore.get(this._sessionId);
      const playheadState = await this._playheadStateStore.getValues(this._sessionId, ["state", "tickMs"]);
      const sessionState = await this._sessionStateStore.getValues(this._sessionId, ["slateCount"]);
      const playheadStateMap = {};
      playheadStateMap[PlayheadState.IDLE] = 'idle';
      playheadStateMap[PlayheadState.RUNNING] = 'running';
      playheadStateMap[PlayheadState.CRASHED] = 'crashed';
      playheadStateMap[PlayheadState.STOPPED] = 'stopped';

      const status = {
        sessionId: this._sessionId,
        playhead: {
          state: playheadStateMap[playheadState.state],
          tickMs: playheadState.tickMs,
        },
        slateInserted: sessionState.slateCount,
      };
      return status;
    }
  }

  async getCurrentMediaManifestAsync(bw, playbackSessionId) {
    //const sessionState = await this._sessionStateStore.get(this._sessionId);
    const sessionState = await this._sessionStateStore.getValues(this._sessionId, ["discSeq"]);
    //const playheadState = await this._playheadStateStore.get(this._sessionId);
    const playheadState = await this._playheadStateStore.getValues(this._sessionId, ["mediaSeq", "vodMediaSeqVideo"]);
    const currentVod = await this.getCurrentVod();
    if (currentVod) {
      const m3u8 = currentVod.getLiveMediaSequences(playheadState.mediaSeq, bw, playheadState.vodMediaSeqVideo, sessionState.discSeq);
      debug(`[${playbackSessionId}]: [${playheadState.mediaSeq + playheadState.vodMediaSeqVideo}][${sessionState.discSeq}] Current media manifest for ${bw} requested`);
      return m3u8;
    } else {
      return "Engine not ready";
    }  
  }

  async getCurrentAudioManifestAsync(audioGroupId, playbackSessionId) {
    //const sessionState = await this._sessionStateStore.get(this._sessionId);
    const sessionState = await this._sessionStateStore.getValues(this._sessionId, ["discSeq"]);
    //const playheadState = await this._playheadStateStore.get(this._sessionId);
    const playheadState = await this._playheadStateStore.getValues(this._sessionId, ["mediaSeq", "vodMediaSeqAudio"]);
    const currentVod = await this.getCurrentVod();
    if (currentVod) {
      const m3u8 = currentVod.getLiveMediaAudioSequences(playheadState.mediaSeq, audioGroupId, playheadState.vodMediaSeqAudio, sessionState.discSeq);
      debug(`[${playbackSessionId}]: [${playheadState.mediaSeq + playheadState.vodMediaSeqAudio}] Current audio manifest for ${audioGroupId} requested`);
      return m3u8;
    } else {
      return "Engine not ready";
    }  
  }

  async incrementAsync() {
    await this._tickAsync();
    //let sessionState = await this._sessionStateStore.get(this._sessionId);
    let sessionState = await this._sessionStateStore.getValues(this._sessionId, ["state", "mediaSeq", "discSeq", "vodMediaSeqVideo", "vodMediaSeqAudio"]);
    //let playheadState = await this._playheadStateStore.get(this._sessionId);
    let playheadState = await this._playheadStateStore.getValues(this._sessionId, ["mediaSeq", "vodMediaSeqVideo", "vodMediaSeqAudio"]);
    const currentVod = await this.getCurrentVod();
    if (sessionState.state === SessionState.VOD_NEXT_INITIATING) {
      sessionState.state = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
    } else {
      sessionState.vodMediaSeqVideo = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", sessionState.vodMediaSeqVideo + 1);
      sessionState.vodMediaSeqAudio = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", sessionState.vodMediaSeqAudio + 1);
    }
    if (sessionState.vodMediaSeqVideo >= currentVod.getLiveMediaSequencesCount() - 1) {
      sessionState.vodMediaSeqVideo = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", currentVod.getLiveMediaSequencesCount() - 1);
      sessionState.vodMediaSeqAudio = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", currentVod.getLiveMediaSequencesCount() - 1);
      sessionState.state = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INIT);
    }
    playheadState.mediaSeq = await this._playheadStateStore.set(this._sessionId, "mediaSeq", sessionState.mediaSeq);
    playheadState.vodMediaSeqVideo = await this._playheadStateStore.set(this._sessionId, "vodMediaSeqVideo", sessionState.vodMediaSeqVideo);
    playheadState.vodMediaSeqAudio = await this._playheadStateStore.set(this._sessionId, "vodMediaSeqAudio", sessionState.vodMediaSeqAudio);
    debug(`[${this._sessionId}]: INCREMENT (mseq=${playheadState.mediaSeq + playheadState.vodMediaSeqVideo}) vodMediaSeq=(${playheadState.vodMediaSeqVideo}_${playheadState.vodMediaSeqAudio} of ${currentVod.getLiveMediaSequencesCount()})`);
    let m3u8 = currentVod.getLiveMediaSequences(playheadState.mediaSeq, 180000, playheadState.vodMediaSeqVideo, sessionState.discSeq);
    return m3u8;
  }

  async getMediaManifestAsync(bw, opts) {
    await this._tickAsync();
    const tsLastRequestVideo = await this._sessionStateStore.get(this._sessionId, "tsLastRequestVideo");
    let timeSinceLastRequest = (tsLastRequestVideo === null) ? 0 : Date.now() - tsLastRequestVideo;
    let sessionState = await this._sessionStateStore.getValues(this._sessionId, 
      ["state", "vodMediaSeqVideo", "mediaSeq", "discSeq", "lastM3u8"]);


    //let sessionState = await this._sessionStateStore.get(this._sessionId);
    const currentVod = await this.getCurrentVod();
    if (!currentVod) {
      throw new Error('Session not ready');
    }
    if (sessionState.state === SessionState.VOD_NEXT_INITIATING) {
      sessionState.state = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
    } else {
      let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);
      sessionState.state = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", sessionState.vodMediaSeqVideo + sequencesToIncrement);
    }
    if (sessionState.vodMediaSeqVideo >= currentVod.getLiveMediaSequencesCount() - 1) {
      sessionState.vodMediaSeqVideo = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", currentVod.getLiveMediaSequencesCount() - 1);
      sessionState.state = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INIT);
    }

    debug(`[${this._sessionId}]: VIDEO ${timeSinceLastRequest} (${this.averageSegmentDuration}) bandwidth=${bw} vodMediaSeq=(${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio})`);
    let m3u8;
    try {
      m3u8 = currentVod.getLiveMediaSequences(sessionState.mediaSeq, bw, sessionState.vodMediaSeqVideo, sessionState.discSeq);
    } catch (exc) {
      if (sessionState.lastM3u8[bw]) {
        m3u8 = sessionState.lastM3u8[bw]
      } else {
        throw new Error('Failed to generate media manifest');
      }
    }
    let lastM3u8 = sessionState.lastM3u8;
    lastM3u8[bw] = m3u8;
    sessionState.lastM3u8 = await this._sessionStateStore.set(this._sessionId, "lastM3u8", lastM3u8);
    sessionState.lastServedM3u8 = await this._sessionStateStore.set(this._sessionId, "lastServedM3u8", m3u8);
    sessionState.tsLastRequestVideo = await this._sessionStateStore.set(this._sessionId, "tsLastRequestVideo", Date.now());

    if (sessionState.state === SessionState.VOD_NEXT_INIT) {
      await this._tickAsync();
      const tsLastRequestVideo = await this._sessionStateStore.get(this._sessionId, "tsLastRequestVideo");
      let timeSinceLastRequest = (tsLastRequestVideo === null) ? 0 : Date.now() - tsLastRequestVideo;

      let sessionState = await this._sessionStateStore.getValues(this._sessionId, 
        ["state", "vodMediaSeqVideo", "mediaSeq", "discSeq", "lastM3u8"]);
      if (sessionState.state === SessionState.VOD_NEXT_INITIATING) {
        sessionState.state = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
      }
      debug(`[${this._sessionId}]: VIDEO ${timeSinceLastRequest} (${this.averageSegmentDuration}) bandwidth=${bw} vodMediaSeq=(${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio})`);
      try {
        m3u8 = currentVod.getLiveMediaSequences(sessionState.mediaSeq, bw, sessionState.vodMediaSeqVideo, sessionState.discSeq);
      } catch (exc) {
        if (sessionState.lastM3u8[bw]) {
          m3u8 = sessionState.lastM3u8[bw]
        } else {
          throw new Error('Failed to generate media manifest');
        }
      }
      let lastM3u8 = sessionState.lastM3u8;
      lastM3u8[bw] = m3u8;
      sessionState.lastM3u8 = await this._sessionStateStore.set(this._sessionId, "lastM3u8", lastM3u8);
      sessionState.lastServedM3u8 = await this._sessionStateStore.set(this._sessionId, "lastServedM3u8", m3u8);
      sessionState.tsLastRequestVideo = await this._sessionStateStore.set(this._sessionId, "tsLastRequestVideo", Date.now());
      return m3u8;
    } else {
      return m3u8;
    }
  }

  async getAudioManifestAsync(audioGroupId, opts) {
    const tsLastRequestAudio = await this._sessionStateStore.get(this._sessionId, "tsLastRequestAudio");
    let timeSinceLastRequest = (tsLastRequestAudio === null) ? 0 : Date.now() - tsLastRequestAudio;

    //let sessionState = await this._sessionStateStore.get(this._sessionId);
    let sessionState = await this._sessionStateStore.getValues(this._sessionId, 
      ["state", "vodMediaSeqVideo", "vodMediaSeqAudio", "mediaSeq", "discSeq", "lastM3u8"]);
    const currentVod = await this.getCurrentVod();
    if (sessionState.state !== SessionState.VOD_NEXT_INITIATING) {
      let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);
  
      if (sessionState.vodMediaSeqAudio < sessionState.vodMediaSeqVideo) {
        sessionState.vodMediaSeqAudio = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", sessionState.vodMediaSeqAudio + sequencesToIncrement);
        if (sessionState.vodMediaSeqAudio >= currentVod.getLiveMediaSequencesCount() - 1) {
          sessionState.vodMediaSeqAudio = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", currentVod.getLiveMediaSequencesCount() - 1);
        }
      }
    }

    debug(`[${this._sessionId}]: AUDIO ${timeSinceLastRequest} (${this.averageSegmentDuration}) audioGroupId=${audioGroupId} vodMediaSeq=(${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio})`);
    let m3u8;
    try {
      m3u8 = currentVod.getLiveMediaAudioSequences(sessionState.mediaSeq, audioGroupId, sessionState.vodMediaSeqAudio, sessionState.discSeq);
    } catch (exc) {
      if (sessionState.lastM3u8[audioGroupId]) {
        m3u8 = sessionState.lastM3u8[audioGroupId];
      } else {
        throw new Error('Failed to generate audio manifest');
      }
    }
    let lastM3u8 = sessionState.lastM3u8;
    lastM3u8[audioGroupId] = m3u8;
    sessionState.lastM3u8 = await this._sessionStateStore.set(this._sessionId, "lastM3u8", lastM3u8);
    sessionState.tsLastRequestAudio = await this._sessionStateStore.set(this._sessionId, "tsLastRequestAudio", Date.now());
    return m3u8;
  }

  async getMasterManifestAsync(filter) {
    let m3u8 = "#EXTM3U\n";
    m3u8 += "#EXT-X-VERSION:4\n";
    m3u8 += m3u8Header();
    m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.session.id",VALUE="${this._sessionId}"\n`;
    m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.eventstream",VALUE="/eventstream/${this._sessionId}"\n`;
    const currentVod = await this.getCurrentVod();
    if (!currentVod) {
      throw new Error('Session not ready');
    }
    let audioGroupIds = currentVod.getAudioGroups();
    let defaultAudioGroupId;
    let hasClosedCaptions = this._closedCaptions && this._closedCaptions.length > 0;
    if (hasClosedCaptions) {
      this._closedCaptions.forEach(cc => {
        m3u8 += `#EXT-X-MEDIA:TYPE=CLOSED-CAPTIONS,GROUP-ID="cc",LANGUAGE="${cc.lang}",NAME="${cc.name}",DEFAULT=${cc.default ? "YES" : "NO" },AUTOSELECT=${cc.auto ? "YES" : "NO" },INSTREAM-ID="${cc.id}"\n`;
      });
    }
    if (this.use_demuxed_audio === true) {
      if (audioGroupIds.length > 0) {
        m3u8 += "# AUDIO groups\n";
        for (let i = 0; i < audioGroupIds.length; i++) {
          let audioGroupId = audioGroupIds[i];
          m3u8 += `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="${audioGroupId}",NAME="audio",AUTOSELECT=YES,DEFAULT=YES,CHANNELS="2",URI="master-${audioGroupId}.m3u8;session=${this._sessionId}"\n`;
        }
        defaultAudioGroupId = audioGroupIds[0];
      }
    }
    if (this._sessionProfile) {
      const sessionProfile = filter ? applyFilter(this._sessionProfile, filter) : this._sessionProfile;
      sessionProfile.forEach(profile => {
        m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + ',RESOLUTION=' + profile.resolution[0] + 'x' + profile.resolution[1] + ',CODECS="' + profile.codecs + '"' + (defaultAudioGroupId ? `,AUDIO="${defaultAudioGroupId}"` : '') + (hasClosedCaptions ? ',CLOSED-CAPTIONS="cc"': '') +'\n';
        m3u8 += "master" + profile.bw + ".m3u8;session=" + this._sessionId + "\n";
      });
    } else {
      currentVod.getUsageProfiles().forEach(profile => {
        m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + ',RESOLUTION=' + profile.resolution + ',CODECS="' + profile.codecs + '"' + (defaultAudioGroupId ? `,AUDIO="${defaultAudioGroupId}"` : '') + (hasClosedCaptions ? ',CLOSED-CAPTIONS="cc"': '') + '\n';
        m3u8 += "master" + profile.bw + ".m3u8;session=" + this._sessionId + "\n";
      });
    }
    if (this.use_demuxed_audio === true) {
      for (let i = 0; i < audioGroupIds.length; i++) {
        let audioGroupId = audioGroupIds[i];
        m3u8 += `#EXT-X-STREAM-INF:BANDWIDTH=97000,CODECS="mp4a.40.2",AUDIO="${audioGroupId}"\n`;
        m3u8 += `master-${audioGroupId}.m3u8;session=${this._sessionId}\n`;
      }
    }
    this.produceEvent({
      type: 'NOW_PLAYING',
      data: {
        id: this.currentMetadata.id,
        title: this.currentMetadata.title,
      }
    });
    this._sessionStateStore.set(this._sessionId, "tsLastRequestMaster", Date.now());
    return m3u8;
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
    if(this.slateUri) {
      console.error(`[${this._sessionId}]: Will insert slate`);
      const slateVod = await this._loadSlate(currentVod);
      debug(`[${this._sessionId}]: slate loaded`);
      //const sessionState = await this._sessionStateStore.get(this._sessionId);
      const sessionState = await this._sessionStateStore.getValues(this._sessionId, ["slateCount"]);
      await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
      await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
      await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
      await this.setCurrentVod(slateVod);
      await this._sessionStateStore.set(this._sessionId, "slateCount", sessionState.slateCount + 1);

      cloudWatchLog(!this.cloudWatchLogging, 'engine-session', { event: 'slateInserted', channel: this._sessionId });

      return slateVod;
    } else {
      return null;
    }
  }

  async _tickAsync() {
    let newVod;

    //let sessionState = await this._sessionStateStore.get(this._sessionId);
    let sessionState = await this._sessionStateStore.getValues(this._sessionId, 
      ["state", "assetId", "vodMediaSeqVideo", "vodMediaSeqAudio", "mediaSeq", "discSeq"]);

    let currentVod = await this.getCurrentVod();
    let vodResponse;

    switch(sessionState.state) {
      case SessionState.VOD_INIT:
      case SessionState.VOD_INIT_BY_ID:
        try {
          let nextVodPromise;
          if (sessionState.state === SessionState.VOD_INIT) {
            debug(`[${this._sessionId}]: state=VOD_INIT`);
            nextVodPromise = this._getNextVod();
          } else if (sessionState.state === SessionState.VOD_INIT_BY_ID) {
            debug(`[${this._sessionId}]: state=VOD_INIT_BY_ID ${sessionState.assetId}`);
            nextVodPromise = this._getNextVodById(sessionState.assetId);
          }
          const nextVodStart = Date.now();
          vodResponse = await nextVodPromise;
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'nextVod', channel: this._sessionId, reqTimeMs: Date.now() - nextVodStart });
          let loadPromise;
          if (!vodResponse.type) {
            debug(`[${this._sessionId}]: got first VOD uri=${vodResponse.uri}:${vodResponse.offset || 0}`);
            newVod = new HLSVod(vodResponse.uri, [], null, vodResponse.offset * 1000, m3u8Header());
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
          sessionState.vodMediaSeqVideo = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
          sessionState.vodMediaSeqAudio = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
          await this._playheadStateStore.set(this._sessionId, "playheadRef", Date.now());
          this.produceEvent({
            type: 'NOW_PLAYING',
            data: {
              id: this.currentMetadata.id,
              title: this.currentMetadata.title,
            }
          });
          sessionState.state = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
          sessionState.currentVod = await this.setCurrentVod(currentVod);
          return;
        } catch (err) {
          console.error(`[${this._sessionId}]: Failed to init first VOD`);
          if (this._assetManager.handleError) {
            this._assetManager.handleError(new Error("Failed to init first VOD"), vodResponse);
          }
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'error', on: 'firstvod', channel: this._sessionId, err: err, vod: vodResponse });
          debug(err);
          currentVod = await this._insertSlate(currentVod);
          if (!currentVod) {
            debug("No slate to load");
            throw err;
          }
        }
      case SessionState.VOD_PLAYING:
        debug(`[${this._sessionId}]: state=VOD_PLAYING (${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio}, ${currentVod.getLiveMediaSequencesCount()})`);
        return;
      case SessionState.VOD_NEXT_INITIATING:
        debug(`[${this._sessionId}]: state=VOD_NEXT_INITIATING`);
        return;
      case SessionState.VOD_NEXT_INIT:
        try {
          debug(`[${this._sessionId}]: state=VOD_NEXT_INIT`);
          if (!currentVod) {
            throw new Error("No VOD to init");
          }
          const length = currentVod.getLiveMediaSequencesCount();
          const lastDiscontinuity = currentVod.getLastDiscontinuity();
          sessionState.state = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INITIATING);
          let vodPromise = this._getNextVod();
          if (length === 1) {
            // Add a grace period for very short VODs before calling nextVod
            const gracePeriod = (this.averageSegmentDuration / 2);
            debug(`[${this._sessionId}]: adding a grace period before calling nextVod: ${gracePeriod}ms`);
            await timer(gracePeriod);
          }
          const nextVodStart = Date.now();
          vodResponse = await vodPromise;
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'nextVod', channel: this._sessionId, reqTimeMs: Date.now() - nextVodStart });
          let loadPromise;
          if (!vodResponse.type) {
            debug(`[${this._sessionId}]: got next VOD uri=${vodResponse.uri}:${vodResponse.offset}`);
            newVod = new HLSVod(vodResponse.uri, null, null, vodResponse.offset * 1000, m3u8Header());
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
          await loadPromise;
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'loadVod', channel: this._sessionId, loadTimeMs: Date.now() - loadStart });
          debug(`[${this._sessionId}]: next VOD loaded (${newVod.getDeltaTimes()})`);
          debug(`[${this._sessionId}]: ${newVod.getPlayheadPositions()}`);
          currentVod = newVod;
          debug(`[${this._sessionId}]: msequences=${currentVod.getLiveMediaSequencesCount()}`);
          sessionState.vodMediaSeqVideo = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
          sessionState.vodMediaSeqAudio = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
          sessionState.mediaSeq = await this._sessionStateStore.set(this._sessionId, "mediaSeq", sessionState.mediaSeq + length);
          sessionState.discSeq = await this._sessionStateStore.set(this._sessionId, "discSeq", sessionState.discSeq + lastDiscontinuity);
          sessionState.currentVod = await this.setCurrentVod(currentVod);
          await this._playheadStateStore.set(this._sessionId, "playheadRef", Date.now());
          this.produceEvent({
            type: 'NOW_PLAYING',
            data: {
              id: this.currentMetadata.id,
              title: this.currentMetadata.title,
            }
          });
          return;
        } catch(err) {
          console.error(`[${this._sessionId}]: Failed to init next VOD`);
          debug(`[${this._sessionId}]: ${err}`);
          if (this._assetManager.handleError) {
            this._assetManager.handleError(new Error("Failed to init next VOD"), vodResponse);
          }
          cloudWatchLog(!this.cloudWatchLogging, 'engine-session',
            { event: 'error', on: 'nextvod', channel: this._sessionId, err: err, vod: vodResponse });
          currentVod = await this._insertSlate(currentVod);
          if (!currentVod) {
            debug("No slate to load");
            throw err;
          }
        }
        break;
      default:
        throw new Error("Invalid state: " + sessionState.state);      
    }
  }

  _getNextVod() {
    return new Promise((resolve, reject) => {
      this._assetManager.getNextVod({ 
        sessionId: this._sessionId, 
        category: this._category, 
        playlistId: this._sessionId
      })
      .then(nextVod => {
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
          hlsVod = new HLSVod(this.slateUri);
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
      } catch(err) {
        reject(err);
      }
    });
  }

  _truncateSlate(afterVod, requestedDuration) {
    return new Promise((resolve, reject) => {
      try {
        const slateVod = new HLSTruncateVod(this.slateUri, requestedDuration);
        let hlsVod;

        slateVod.load()
        .then(() => {
          hlsVod = new HLSVod(this.slateUri);
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
    const sessionState = await this._sessionStateStore.getValues(this._sessionId, ["vodMediaSeqVideo"]);
    const currentVod = await this.getCurrentVod();
    const deltaTimes = currentVod.getDeltaTimes();
    debug(`[${this._sessionId}]: Current delta time (${sessionState.vodMediaSeqVideo}): ${deltaTimes[sessionState.vodMediaSeqVideo]}`);
    if (deltaTimes[sessionState.vodMediaSeqVideo]) {
      return deltaTimes[sessionState.vodMediaSeqVideo];
    }
    return 0;
  }

  async _getCurrentPlayheadPosition() {
    const sessionState = await this._sessionStateStore.getValues(this._sessionId, ["vodMediaSeqVideo"]);
    const currentVod = await this.getCurrentVod();
    const playheadPositions = currentVod.getPlayheadPositions();
    debug(`[${this._sessionId}]: Current playhead position (${sessionState.vodMediaSeqVideo}): ${playheadPositions[sessionState.vodMediaSeqVideo]}`);
    return playheadPositions[sessionState.vodMediaSeqVideo];
  }
}

module.exports = Session;
