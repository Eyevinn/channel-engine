const debug = require("debug")("engine-session-live");
const allSettled = require("promise.allsettled");
const crypto = require("crypto");
const m3u8 = require("@eyevinn/m3u8");
const url = require("url");
const fetch = require("node-fetch");
const { m3u8Header } = require("./util.js");
const { AbortController } = require("abort-controller");

const timer = (ms) => new Promise((res) => setTimeout(res, ms));
const daterangeAttribute = (key, attr) => {
  if (key === "planned-duration" || key === "duration") {
    return key.toUpperCase() + "=" + `${attr.toFixed(3)}`;
  } else {
    return key.toUpperCase() + "=" + `"${attr}"`;
  }
};
const TARGET_PLAYLIST_DURATION_SEC = 60;
const RESET_DELAY = 5000;
const FAIL_TIMEOUT = 4000;
const DEFAULT_PLAYHEAD_INTERVAL_MS = 6 * 1000;
const PlayheadState = Object.freeze({
  RUNNING: 1,
  STOPPED: 2,
  CRASHED: 3,
  IDLE: 4,
});

/**
 * When we implement subtitle support in live-mix we should place it in its own file/or share it with audio
 * we should also remove audio implementation when we implement subtitles from this file so we don't get at 4000 line long file.
 */

class SessionLive {
  constructor(config, sessionLiveStore) {
    this.sessionId = crypto.randomBytes(20).toString("hex");
    this.sessionLiveStateStore = sessionLiveStore.sessionLiveStateStore;
    this.instanceId = sessionLiveStore.instanceId;
    this.mediaSeqCount = 0;
    this.prevMediaSeqCount = 0;
    this.discSeqCount = 0;
    this.prevDiscSeqCount = 0;
    this.audioSeqCount = 0;
    this.prevAudioSeqCount = 0;
    this.audioDiscSeqCount = 0;
    this.prevAudioDiscSeqCount = 0;
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.vodSegments = {};
    this.vodAudioSegments = {};
    this.mediaManifestURIs = {};
    this.audioManifestURIs = {};
    this.liveSegQueue = {};
    this.lastRequestedMediaSeqRaw = null;
    this.liveSourceM3Us = {};
    this.liveAudioSegQueue = {};
    this.lastRequestedAudioSeqRaw = null;
    this.liveAudioSourceM3Us = {};
    this.playheadState = PlayheadState.IDLE;
    this.liveSegsForFollowers = {};
    this.audioLiveSegsForFollowers = {};
    this.timerCompensation = null;
    this.firstTime = true;
    this.firstTimeAudio = true;
    this.allowedToSet = false;
    this.pushAmount = 0;
    this.restAmount = 0;
    this.pushAmountAudio = 0;
    this.restAmountAudio = 0;
    this.waitForPlayhead = true;
    this.blockGenerateManifest = false;

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
      if (config.profile) {
        this.sessionLiveProfile = config.profile;
      }
      if (config.audioTracks) {
        this.sessionAudioTracks = config.audioTracks;
      }
    }
  }

  async initAsync() {
    this.sessionLiveState = await this.sessionLiveStateStore.create(this.sessionId, this.instanceId);
  }

  /**
   *
   * @param {number} resetDelay The amount of time to wait before resetting the session.
   *
   */
  async resetLiveStoreAsync(resetDelay) {
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader) {
      return;
    }
    if (resetDelay === null || resetDelay < 0) {
      resetDelay = RESET_DELAY;
    }
    debug(`[${this.instanceId}][${this.sessionId}]: LEADER: Resetting SessionLive values in Store ${resetDelay === 0 ? "Immediately" : `after a delay=(${resetDelay}ms)`}`);
    await timer(resetDelay);
    await this.sessionLiveState.set("liveSegsForFollowers", null);
    await this.sessionLiveState.set("lastRequestedMediaSeqRaw", null);
    await this.sessionLiveState.set("liveAudioSegsForFollowers", null);
    await this.sessionLiveState.set("lastRequestedAudioSeqRaw", null);
    await this.sessionLiveState.set("transitSegs", null);
    await this.sessionLiveState.set("transitSegsAudio", null);
    await this.sessionLiveState.set("firstCounts", {
      liveSourceMseqCount: null,
      liveSourceAudioMseqCount: null,
      mediaSeqCount: null,
      audioSeqCount: null,
      discSeqCount: null,
      audioDiscSeqCount: null
    });
    debug(`[${this.instanceId}][${this.sessionId}]: LEADER: SessionLive values in Store have now been reset!`);
  }

  async resetSession() {
    /*
     * ISSUE: resetSession can be called at anytime no matter where the playhead is
     * running code. If a reset occurs just before playhead wants to read from this.something,
     * then it will generate a TypeError, depending on function.
     */
    while (this.waitForPlayhead) {
      debug(`[${this.sessionId}]: SessionLive RESET requested. Waiting for Playhead to finish a parse job.`);
      await timer(1000);
    }

    this.mediaSeqCount = 0;
    this.prevMediaSeqCount = 0;
    this.discSeqCount = 0;
    this.audioSeqCount = 0;
    this.prevAudioSeqCount = 0;
    this.audioDiscSeqCount = 0;
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.vodSegments = {};
    this.vodAudioSegments = {};
    this.mediaManifestURIs = {};
    this.audioManifestURIs = {};
    this.liveSegQueue = {};
    this.liveAudioSegQueue = {};
    this.lastRequestedMediaSeqRaw = null;
    this.lastRequestedAudioSeqRaw = null;
    this.liveSourceM3Us = {};
    this.liveAudioSourceM3Us = {};
    this.liveSegsForFollowers = {};
    this.audioLiveSegsForFollowers = {};
    this.timerCompensation = null;
    this.firstTime = true;
    this.firstTimeAudio = true;
    this.pushAmount = 0;
    this.pushAmountAudio = 0;
    this.allowedToSet = false;
    this.waitForPlayhead = true;
    this.blockGenerateManifest = false;

    debug(`[${this.instanceId}][${this.sessionId}]: Resetting all property values in sessionLive`);
  }

  async startPlayheadAsync() {
    debug(`[${this.sessionId}]: SessionLive-Playhead consumer started`);
    this.playheadState = PlayheadState.RUNNING;
    while (this.playheadState !== PlayheadState.CRASHED) {
      try {
        this.timerCompensation = true;
        // Nothing to do if we have no Live Source to probe
        if (!this.masterManifestUri) {
          await timer(3000);
          continue;
        }
        if (this.playheadState === PlayheadState.STOPPED) {
          debug(`[${this.sessionId}]: Playhead has Stopped, clearing local session and store.`);
          this.waitForPlayhead = false;
          await this.resetSession();
          this.resetLiveStoreAsync(RESET_DELAY);
          return;
        }

        // Fetch Live-Source Segments, and get ready for on-the-fly manifest generation
        // And also compensate for processing time

        this.waitForPlayhead = true;
        const tsIncrementBegin = Date.now();
        await this._loadAllMediaManifests();
        await this._loadAllAudioManifests();
        const tsIncrementEnd = Date.now();
        this.waitForPlayhead = false;

        // Let the playhead move at an interval set according to live segment duration
        const liveSegmentDurationMs = this._getAnyFirstSegmentDurationMs() || DEFAULT_PLAYHEAD_INTERVAL_MS;

        // Set the timer
        let timerValueMs = 0;
        if (this.timerCompensation) {
          const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          const incrementDuration = tsIncrementEnd - tsIncrementBegin;
          if (incrementDuration >= liveSegmentDurationMs * 0.5 && isLeader) {
            timerValueMs = liveSegmentDurationMs;
          } else {
            timerValueMs = liveSegmentDurationMs - (tsIncrementEnd - tsIncrementBegin);
          }
        } else {
          // DO NOT compensate if manifest fetching was out-of-sync
          // It means that Live Source and Channel-Engine were awkwardly time-synced
          timerValueMs = liveSegmentDurationMs;
        }
        debug(`[${this.sessionId}]: SessionLive-Playhead going to ping again after ${timerValueMs}ms`);
        await timer(timerValueMs);
      } catch (err) {
        debug(`[${this.sessionId}]: SessionLive-Playhead consumer crashed`);
        console.error(`[${this.sessionId}]: ${err.message}`);
        debug(err);
        this.playheadState = PlayheadState.CRASHED;
      }
    }
  }

  async restartPlayheadAsync() {
    debug(`[${this.sessionId}]: Restarting sessionLive-playhead consumer`);
    await this.startPlayheadAsync();
  }

  async stopPlayheadAsync() {
    debug(`[${this.sessionId}]: Stopping sessionLive-playhead consumer`);
    this.playheadState = PlayheadState.STOPPED;
  }

  /**
   * This function sets the master manifest URI in sessionLive.
   * @param {string} masterManifestUri The master manifest URI.
   * @returns a boolean indicating whether the master manifest URI is reachable or not.
   */
  async setLiveUri(masterManifestUri) {
    if (masterManifestUri === null) {
      debug(`[${this.sessionId}]: No Live URI provided.`);
      return false;
    }
    // Try to set Live URI
    let attempts = 3;
    while (!this.masterManifestUri && attempts > 0) {
      attempts--;
      try {
        debug(`[${this.instanceId}][${this.sessionId}]: Going to fetch Live Master Manifest!`);
        // Load & Parse all Media Manifest URIs from Master
        await this._loadMasterManifest(masterManifestUri);
        this.masterManifestUri = masterManifestUri;
        if (this.sessionLiveProfile) {
          this._filterLiveProfiles();
          debug(`[${this.sessionId}]: Filtered Live profiles! (${Object.keys(this.mediaManifestURIs).length}) profiles left!`);
        }
        if (this.sessionAudioTracks) {
          this._filterLiveAudioTracks();
          debug(`[${this.sessionId}]: Filtered Live audio tracks! (${Object.keys([Object.keys(this.audioManifestURIs)[0]]).length}) profiles left!`);
        }
      } catch (err) {
        this.masterManifestUri = null;
        debug(`[${this.instanceId}][${this.sessionId}]: Failed to fetch Live Master Manifest! ${err}`);
        debug(`[${this.instanceId}][${this.sessionId}]: Will try again in 1000ms! (tries left=${attempts})`);
        await timer(1000);
      }
      // To make sure certain operations only occur once.
      this.firstTime = true;
      this.firstTimeAudio = true;
    }
    // Return whether job was successful or not.
    if (!this.masterManifestUri) {
      return false;
    }
    return true;
  }

  async setCurrentMediaSequenceSegments(segments) {
    if (segments === null) {
      debug(`[${this.sessionId}]: No segments provided.`);
      return false;
    }
    // Make it possible to add & share new segments
    this.allowedToSet = true;
    const allBws = Object.keys(segments);
    if (this._isEmpty(this.vodSegments)) {
      for (let i = 0; i < allBws.length; i++) {
        const bw = allBws[i];
        if (!this.vodSegments[bw]) {
          this.vodSegments[bw] = [];
        }

        if (segments[bw][0].discontinuity) {
          segments[bw].shift();
        }
        let cueInExists = null;
        for (let segIdx = 0; segIdx < segments[bw].length; segIdx++) {
          const v2lSegment = segments[bw][segIdx];
          if (v2lSegment.cue) {
            if (v2lSegment.cue["in"]) {
              cueInExists = true;
            } else {
              cueInExists = false;
            }
          }
          this.vodSegments[bw].push(v2lSegment);
        }

        const endIdx = segments[bw].length - 1;
        if (!segments[bw][endIdx].discontinuity) {
          const finalSegItem = { discontinuity: true };
          if (!cueInExists) {
            finalSegItem["cue"] = { in: true };
          }
          this.vodSegments[bw].push(finalSegItem);
        } else {
          if (!cueInExists) {
            segments[bw][endIdx]["cue"] = { in: true };
          }
        }
      }
    } else {
      debug(`[${this.sessionId}]: 'vodSegments' not empty = Using 'transitSegs'`);
    }
    debug(`[${this.sessionId}]: Setting CurrentMediaSequenceSegments. First seg is: [${this.vodSegments[Object.keys(this.vodSegments)[0]][0].uri}]`);

    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (isLeader) {
      //debug(`[${this.sessionId}]: LEADER: I am adding 'transitSegs'=${JSON.stringify(this.vodSegments)} to Store for future followers`);
      await this.sessionLiveState.set("transitSegs", this.vodSegments);
      debug(`[${this.sessionId}]: LEADER: I am adding 'transitSegs' to Store for future followers`);
    }
  }
  async setCurrentAudioSequenceSegments(segments) {
    if (segments === null) {
      debug(`[${this.sessionId}]: No segments provided.`);
      return false;
    }
    // Make it possible to add & share new segments
    this.allowedToSet = true;
    if (this._isEmpty(this.vodAudioSegments)) {
      const groupIds = Object.keys(segments);
      for (let i = 0; i < groupIds.length; i++) {
        const groupId = groupIds[i];
        const langs = Object.keys(segments[groupId]);
        for (let j = 0; j < langs.length; j++) {
          const lang = langs[j];
          if (!this.vodAudioSegments[groupId]) {
            this.vodAudioSegments[groupId] = {};
          }
          if (!this.vodAudioSegments[groupId][lang]) {
            this.vodAudioSegments[groupId][lang] = [];
          }

          if (segments[groupId][lang][0].discontinuity) {
            segments[groupId][lang].shift();
          }
          let cueInExists = null;
          for (let segIdx = 0; segIdx < segments[groupId][lang].length; segIdx++) {
            const v2lSegment = segments[groupId][lang][segIdx];
            if (v2lSegment.cue) {
              if (v2lSegment.cue["in"]) {
                cueInExists = true;
              } else {
                cueInExists = false;
              }
            }
            this.vodAudioSegments[groupId][lang].push(v2lSegment);
          }

          const endIdx = segments[groupId][lang].length - 1;
          if (!segments[groupId][lang][endIdx].discontinuity) {
            const finalSegItem = { discontinuity: true };
            if (!cueInExists) {
              finalSegItem["cue"] = { in: true };
            }
            this.vodAudioSegments[groupId][lang].push(finalSegItem);
          } else {
            if (!cueInExists) {
              segments[groupId][lang][endIdx]["cue"] = { in: true };
            }
          }
        }
      }
    } else {
      debug(`[${this.sessionId}]: 'vodAudioSegments' not empty = Using 'transitSegs'`);
    }
    debug(`[${this.sessionId}]: Setting CurrentAudioSequenceSegments. First seg is: [${this.vodAudioSegments[Object.keys(this.vodAudioSegments)[0]][Object.keys(this.vodAudioSegments[Object.keys(this.vodAudioSegments)[0]])[0]][0].uri}]`);

    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (isLeader) {
      //debug(`[${this.sessionId}]: LEADER: I am adding 'transitSegs'=${JSON.stringify(this.vodSegments)} to Store for future followers`);
      await this.sessionLiveState.set("transitSegs", this.vodAudioSegments);
      debug(`[${this.sessionId}]: LEADER: I am adding 'transitSegs' to Store for future followers`);
    }
  }

  async setCurrentMediaAndDiscSequenceCount(mediaSeq, discSeq, audioMediaSeq, audioDiscSeq) {
    if (mediaSeq === null || discSeq === null) {
      debug(`[${this.sessionId}]: No media or disc sequence provided`);
      return false;
    }
    if (this.useDemuxedAudio && (audioDiscSeq === null || audioDiscSeq === null)) {
      debug(`[${this.sessionId}]: No media or disc sequence for audio provided`);
      return false;
    }
    debug(`[${this.sessionId}]: Setting mediaSeqCount, discSeqCount, audioSeqCount and audioDiscSeqCount to: [${mediaSeq}]:[${discSeq}], [${audioMediaSeq}]:[${audioDiscSeq}]`);
    this.mediaSeqCount = mediaSeq;
    this.discSeqCount = discSeq;
    this.audioSeqCount = audioMediaSeq;
    this.audioDiscSeqCount = audioDiscSeq;

    // IN CASE: New/Respawned Node Joins the Live Party
    // Don't use what Session gave you. Use the Leaders number if it's available
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    let liveCounts = await this.sessionLiveState.get("firstCounts");
    if (liveCounts === null) {
      liveCounts = {
        liveSourceMseqCount: null,
        mediaSeqCount: null,
        discSeqCount: null,
        liveSourceAudioMseqCount: null,
        audioSeqCount: null,
        audioDiscSeqCount: null,
      };
    }
    if (isLeader) {
      liveCounts.discSeqCount = this.discSeqCount;
      liveCounts.audioDiscSeqCount = this.audioDiscSeqCount;
      await this.sessionLiveState.set("firstCounts", liveCounts);
    } else {
      const leadersMediaSeqCount = liveCounts.mediaSeqCount;
      const leadersDiscSeqCount = liveCounts.discSeqCount;
      const leadersAudioSeqCount = liveCounts.audioSeqCount;
      const leadersAudioDiscSeqCount = liveCounts.audioDiscSeqCount;

      if (leadersMediaSeqCount !== null) {
        this.mediaSeqCount = leadersMediaSeqCount;
        debug(`[${this.sessionId}]: Setting mediaSeqCount to: [${this.mediaSeqCount}]`);
        const transitSegs = await this.sessionLiveState.get("transitSegs");
        if (!this._isEmpty(transitSegs)) {
          debug(`[${this.sessionId}]: Getting and loading 'transitSegs'`);
          this.vodSegments = transitSegs;
        }
      }
      if (leadersAudioSeqCount !== null) {
        this.audioSeqCount = leadersAudioSeqCount;
        debug(`[${this.sessionId}]: Setting mediaSeqCount to: [${this.audioSeqCount}]`);
        const transitAudioSegs = await this.sessionLiveState.get("transitAudioSegs");
        if (!this._isEmpty(transitAudioSegs)) {
          debug(`[${this.sessionId}]: Getting and loading 'transitSegs'`);
          this.vodAudioSegments = transitAudioSegs;
        }
      }
      if (leadersDiscSeqCount !== null) {
        this.discSeqCount = leadersDiscSeqCount;
        debug(`[${this.sessionId}]: Setting discSeqCount to: [${this.discSeqCount}]`);
      }
      if (leadersAudioDiscSeqCount !== null) {
        this.audioDiscSeqCount = leadersAudioDiscSeqCount;
        debug(`[${this.sessionId}]: Setting discSeqCount to: [${this.audioDiscSeqCount}]`);
      }
    }
    return true;
  }

  async getTransitionalSegments() {
    return this.vodSegments;
  }

  async getTransitionalAudioSegments() {
    return this.vodAudioSegments;
  }

  async getCurrentMediaSequenceSegments() {
    /**
     * Might be possible that a follower sends segments to Session
     * BEFORE Leader finished fetching new segs and sending segs himself.
     * As long as Leader sends same segs to session as Follower even though Leader
     * is trying to get new segs, it should be fine!
     **/
    this.allowedToSet = false;
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader) {
      const leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
      if (leadersMediaSeqRaw > this.lastRequestedMediaSeqRaw) {
        this.lastRequestedMediaSeqRaw = leadersMediaSeqRaw;
        this.liveSegsForFollowers = await this.sessionLiveState.get("liveSegsForFollowers");
        this._updateLiveSegQueue();
      }
    }

    let currentMediaSequenceSegments = {};
    let segmentCount = 0;
    let increment = 0;
    for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
      let bw = Object.keys(this.mediaManifestURIs)[i];

      const liveTargetBandwidth = this._findNearestBw(bw, Object.keys(this.mediaManifestURIs));
      const vodTargetBandwidth = this._getNearestBandwidth(bw, Object.keys(this.vodSegments));

      // Remove segments and disc-tag if they are on top
      if (this.vodSegments[vodTargetBandwidth].length > 0 && this.vodSegments[vodTargetBandwidth][0].discontinuity) {
        this.vodSegments[vodTargetBandwidth].shift();
        increment = 1;
      }

      segmentCount = this.vodSegments[vodTargetBandwidth].length;
      currentMediaSequenceSegments[liveTargetBandwidth] = [];
      // In case we switch back before we've depleted all transitional segments
      currentMediaSequenceSegments[liveTargetBandwidth] = this.vodSegments[vodTargetBandwidth].concat(this.liveSegQueue[liveTargetBandwidth]);
      currentMediaSequenceSegments[liveTargetBandwidth].push({ discontinuity: true, cue: { in: true } });
      debug(`[${this.sessionId}]: Getting current media segments for bw=${bw}`);
    }

    this.discSeqCount += increment;
    return {
      currMseqSegs: currentMediaSequenceSegments,
      segCount: segmentCount,
    };
  }

  async getCurrentAudioSequenceSegments() {
    /**
     * Might be possible that a follower sends segments to Session
     * BEFORE Leader finished fetching new segs and sending segs himself.
     * As long as Leader sends same segs to session as Follower even though Leader
     * is trying to get new segs, it should be fine!
     **/
    this.allowedToSet = false;
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader) {
      const leadersAudioSeqRaw = await this.sessionLiveState.get("lastRequestedAudioSeqRaw");
      if (leadersAudioSeqRaw > this.lastRequestedAudioSeqRaw) {
        this.lastRequestedAudioSeqRaw = leadersAudioSeqRaw;
        this.liveAudioSegsForFollowers = await this.sessionLiveState.get("liveAudioSegsForFollowers");
        this._updateAudioLiveSegQueue();
      }
    }

    let currentAudioSequenceSegments = {};
    let segmentCount = 0;
    let increment = 0;
    const groupIds = Object.keys(this.vodAudioSegments);
    for (let i = 0; i < groupIds.length; i++) {
      let groupId = groupIds[i];
      if (!currentAudioSequenceSegments[groupId]) {
        currentAudioSequenceSegments[groupId] = {};
      }
      let langs = Object.keys(this.vodAudioSegments[groupIds[i]]);
      for (let j = 0; j < langs.length; j++) {
        const liveTargetGroupLang = this._findAudioGroupAndLang(groupId, langs[j], this.audioManifestURIs);
        const vodTargetGroupLang = this._findAudioGroupAndLang(groupId, langs[j], this.vodAudioSegments);
        if (!vodTargetGroupLang.audioGroupId || !vodTargetGroupLang.audioLanguage) {
          return null;
        }
        // Remove segments and disc-tag if they are on top
        if (this.vodAudioSegments[vodTargetGroupLang.audioGroupId][vodTargetGroupLang.audioLanguage].length > 0 && this.vodAudioSegments[vodTargetGroupLang.audioGroupId][vodTargetGroupLang.audioLanguage][0].discontinuity) {
          this.vodAudioSegments[vodTargetGroupLang.audioGroupId][vodTargetGroupLang.audioLanguage].shift();
          increment = 1;
        }
        segmentCount = this.vodAudioSegments[vodTargetGroupLang.audioGroupId][vodTargetGroupLang.audioLanguage].length;
        currentAudioSequenceSegments[vodTargetGroupLang.audioGroupId][vodTargetGroupLang.audioLanguage] = [];
        // In case we switch back before we've depleted all transitional segments
        currentAudioSequenceSegments[vodTargetGroupLang.audioGroupId][vodTargetGroupLang.audioLanguage] = this.vodAudioSegments[vodTargetGroupLang.audioGroupId][vodTargetGroupLang.audioLanguage].concat(this.liveAudioSegQueue[liveTargetGroupLang.audioGroupId][liveTargetGroupLang.audioLanguage]);
        currentAudioSequenceSegments[vodTargetGroupLang.audioGroupId][vodTargetGroupLang.audioLanguage].push({ discontinuity: true, cue: { in: true } });
        debug(`[${this.sessionId}]: Getting current audio segments for ${groupId, langs[j]}`);
      }
    }

    this.audioDiscSeqCount += increment;
    return {
      currMseqSegs: currentAudioSequenceSegments,
      segCount: segmentCount,
    };
  }

  async getCurrentMediaAndDiscSequenceCount() {
    return {
      mediaSeq: this.mediaSeqCount,
      discSeq: this.discSeqCount,
      audioSeq: this.audioSeqCount,
      audioDiscSeq: this.audioDiscSeqCount,
    };
  }

  getStatus() {
    const playheadStateMap = {};
    playheadStateMap[PlayheadState.IDLE] = "idle";
    playheadStateMap[PlayheadState.RUNNING] = "running";
    playheadStateMap[PlayheadState.CRASHED] = "crashed";
    playheadStateMap[PlayheadState.STOPPED] = "stopped";
    const status = {
      sessionId: this.sessionId,
      playhead: {
        state: playheadStateMap[this.playheadState],
      },
    };
    return status;
  }

  // Generate manifest to give to client
  async getCurrentMediaManifestAsync(bw) {
    if (!this.sessionLiveState) {
      throw new Error("SessionLive not ready");
    }
    if (bw === null) {
      debug(`[${this.sessionId}]: No bandwidth provided`);
      return null;
    }
    debug(`[${this.sessionId}]: ...Loading the selected Live Media Manifest`);
    let attempts = 10;
    let m3u8 = null;
    while (!m3u8 && attempts > 0) {
      attempts--;
      try {
        m3u8 = await this._GenerateLiveManifest(bw);
        if (!m3u8) {
          debug(`[${this.sessionId}]: No manifest available yet, will try again after 1000ms`);
          await timer(1000);
        }
      } catch (exc) {
        throw new Error(`[${this.instanceId}][${this.sessionId}]: Failed to generate manifest. Live Session might have ended already. \n${exc}`);
      }
    }
    if (!m3u8) {
      throw new Error(`[${this.instanceId}][${this.sessionId}]: Failed to generate manifest after 10000ms`);
    }
    return m3u8;
  }

  async getCurrentAudioManifestAsync(audioGroupId, audioLanguage) {
    if (!this.sessionLiveState) {
      throw new Error("SessionLive not ready");
    }
    if (audioGroupId === null) {
      debug(`[${this.sessionId}]: No audioGroupId provided`);
      return null;
    }
    if (audioLanguage === null) {
      debug(`[${this.sessionId}]: No audioLanguage provided`);
      return null;
    }
    debug(`[${this.sessionId}]: ...Loading the selected Live Audio Manifest`);
    let attempts = 10;
    let m3u8 = null;
    while (!m3u8 && attempts > 0) {
      attempts--;
      try {
        m3u8 = await this._GenerateLiveAudioManifest(audioGroupId, audioLanguage);
        if (!m3u8) {
          debug(`[${this.sessionId}]: No audio manifest available yet, will try again after 1000ms`);
          await timer(1000);
        }
      } catch (exc) {
        throw new Error(`[${this.instanceId}][${this.sessionId}]: Failed to generate audio manifest. Live Session might have ended already. \n${exc}`);
      }
    }
    if (!m3u8) {
      throw new Error(`[${this.instanceId}][${this.sessionId}]: Failed to generate audio manifest after 10000ms`);
    }
    return m3u8;
  }

  async getCurrentSubtitleManifestAsync(subtitleGroupId, subtitleLanguage) {
    debug(`[${this.sessionId}]: getCurrentSubtitleManifestAsync is NOT Implemented`);
    return "Not Implemented";
  }

  /**
   *
   * @param {string} masterManifestURI The master manifest URI.
   * @returns Loads the URIs to the different media playlists from the given master playlist.
   *
   */
  async _loadMasterManifest(masterManifestURI) {
    if (masterManifestURI === null) {
      throw new Error(`[${this.instanceId}][${this.sessionId}]: No masterManifestURI provided`);
    }
    const parser = m3u8.createStream();
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      debug(`[${this.sessionId}]: Request Timeout! Aborting Request to ${masterManifestURI}`);
      controller.abort();
    }, FAIL_TIMEOUT);

    const response = await fetch(masterManifestURI, { signal: controller.signal });
    try {
      response.body.pipe(parser);
    } catch (err) {
      debug(`[${this.sessionId}]: Error when piping response to parser! ${JSON.stringify(err)}`);
      return Promise.reject(err);
    } finally {
      clearTimeout(timeout);
    }

    return new Promise((resolve, reject) => {
      parser.on("m3u", (m3u) => {
        debug(`[${this.sessionId}]: ...Fetched a New Live Master Manifest from:\n${masterManifestURI}`);
        let baseUrl = "";
        const m = masterManifestURI.match(/^(.*)\/.*?$/);
        if (m) {
          baseUrl = m[1] + "/";
        }
        // Get all Profile manifest URIs in the Live Master Manifest
        for (let i = 0; i < m3u.items.StreamItem.length; i++) {
          const streamItem = m3u.items.StreamItem[i];
          const streamItemBW = streamItem.get("bandwidth");
          const mediaManifestUri = url.resolve(baseUrl, streamItem.get("uri"));
          if (!this.mediaManifestURIs[streamItemBW]) {
            this.mediaManifestURIs[streamItemBW] = "";
          }
          this.mediaManifestURIs[streamItemBW] = mediaManifestUri;

          if (streamItem.get("audio") && this.useDemuxedAudio) {
            let audioGroupId = streamItem.get("audio")
            let audioGroupItems = m3u.items.MediaItem.filter((item) => {
              return item.get("type") === "AUDIO" && item.get("group-id") === audioGroupId;
            });
            // # Find all langs amongst the mediaItems that have this group id.
            // # It extracts each mediaItems language attribute value.
            // # ALSO initialize in this.audioSegments a lang. property who's value is an array [{seg1}, {seg2}, ...].
            if (!this.audioManifestURIs[audioGroupId]) {
              this.audioManifestURIs[audioGroupId] = {}
            }

            audioGroupItems.map((item) => {
              let itemLang;
              if (!item.get("language")) {
                itemLang = item.get("name");
              } else {
                itemLang = item.get("language");
              }
              if (!this.audioManifestURIs[audioGroupId][itemLang]) {
                this.audioManifestURIs[audioGroupId][itemLang] = ""
              }
              const audioManifestUri = url.resolve(baseUrl, item.get("uri"))
              this.audioManifestURIs[audioGroupId][itemLang] = audioManifestUri;
            });
          }
        }
        debug(`[${this.sessionId}]: All Live Media Manifest URIs have been collected. (${Object.keys(this.mediaManifestURIs).length}) profiles found!`);
        debug(`[${this.sessionId}]: All Live Audio Manifest URIs have been collected. (${Object.keys(this.audioManifestURIs[Object.keys(this.audioManifestURIs)[0]]).length}) tracks found!`);
        resolve();
        parser.on("error", (exc) => {
          debug(`Parser Error: ${JSON.stringify(exc)}`);
          reject(exc);
        });
      });
    });
  }

  // FOLLOWER only function
  _updateLiveSegQueue() {
    if (Object.keys(this.liveSegsForFollowers).length === 0) {
      debug(`[${this.sessionId}]: FOLLOWER: Error No Segments found at all.`);
    }
    const liveBws = Object.keys(this.liveSegsForFollowers);
    const size = this.liveSegsForFollowers[liveBws[0]].length;

    // Push the New Live Segments to All Variants
    for (let segIdx = 0; segIdx < size; segIdx++) {
      for (let i = 0; i < liveBws.length; i++) {
        const liveBw = liveBws[i];
        const liveSegFromLeader = this.liveSegsForFollowers[liveBw][segIdx];
        if (!this.liveSegQueue[liveBw]) {
          this.liveSegQueue[liveBw] = [];
        }
        // Do not push duplicates
        const liveSegURIs = this.liveSegQueue[liveBw].filter((seg) => seg.uri).map((seg) => seg.uri);
        if (liveSegFromLeader.uri && liveSegURIs.includes(liveSegFromLeader.uri)) {
          debug(`[${this.sessionId}]: FOLLOWER: Found duplicate live segment. Skip push! (${liveBw})`);
        } else {
          this.liveSegQueue[liveBw].push(liveSegFromLeader);
          debug(`[${this.sessionId}]: FOLLOWER: Pushed segment (${liveSegFromLeader.uri ? liveSegFromLeader.uri : "Disc-tag"}) to 'liveSegQueue' (${liveBw})`);
        }
      }
    }
    // Remove older segments and update counts
    const newTotalDuration = this._incrementAndShift("FOLLOWER");
    if (newTotalDuration) {
      debug(`[${this.sessionId}]: FOLLOWER: New Adjusted Playlist Duration=${newTotalDuration}s`);
    }
  }

  _updateLiveAudioSegQueue() {
    let followerGroupIds = Object.keys(this.liveAudioSegsForFollowers);
    let followerLangs = Object.keys(Object.keys(this.liveAudioSegsForFollowers[followerGroupIds[0]]));
    if (this.liveAudioSegsForFollowers[followerGroupIds[0]][followerLangs[0]].length === 0) {
      debug(`[${this.sessionId}]: FOLLOWER: Error No Segments found at all.`);
    }
    const liveGroupIds = Object.keys(this.liveAudioSegsForFollowers);
    const size = this.liveAudioSegsForFollowers[liveGroupIds[0]][Object.keys(this.liveAudioSegsForFollowers[liveGroupIds[0]])].length;

    // Push the New Live Segments to All Variants
    for (let segIdx = 0; segIdx < size; segIdx++) {
      for (let i = 0; i < liveGroupIds.length; i++) {
        x
        const liveGroupId = liveGroupIds[i];
        const liveLangs = Object.keys(this.liveAudioSegsForFollowers[liveGroupId])
        for (let j = 0; j < liveLangs.length; j++) {
          const liveLang = liveLangs[j];

          const liveSegFromLeader = this.liveAudioSegsForFollowers[liveGroupId][liveLang][segIdx];
          if (!this.liveAudioSegQueue[liveGroupId]) {
            this.liveAudioSegQueue[liveGroupId] = {};
          }
          if (!this.liveAudioSegQueue[liveGroupId][liveLang]) {
            this.liveAudioSegQueue[liveGroupId][liveLang] = [];
          }
          // Do not push duplicates
          const liveSegURIs = this.liveAudioSegQueue[liveGroupId][liveLang].filter((seg) => seg.uri).map((seg) => seg.uri);
          if (liveSegFromLeader.uri && liveSegURIs.includes(liveSegFromLeader.uri)) {
            debug(`[${this.sessionId}]: FOLLOWER: Found duplicate live segment. Skip push! (${liveGroupId})`);
          } else {
            this.liveAudioSegQueue[liveGroupId][liveLang].push(liveSegFromLeader);
            debug(`[${this.sessionId}]: FOLLOWER: Pushed segment (${liveSegFromLeader.uri ? liveSegFromLeader.uri : "Disc-tag"}) to 'liveAudioSegQueue' (${liveGroupId, liveLang})`);
          }
        }
      }
    }
    // Remove older segments and update counts
    const newTotalDuration = this._incrementAndShiftAudio("FOLLOWER");
    if (newTotalDuration) {
      debug(`[${this.sessionId}]: FOLLOWER: New Adjusted Playlist Duration=${newTotalDuration}s`);
    }
  }

  /**
   * This function adds new live segments to the node from which it can
   * generate new manifests from. Method for attaining new segments differ
   * depending on node Rank. The Leader collects from live source and
   * Followers collect from shared storage.
   *
   * @returns Nothing, but gives data to certain class-variables
   */
  async _loadAllMediaManifests() {
    debug(`[${this.sessionId}]: Attempting to load all media manifest URIs in=${Object.keys(this.mediaManifestURIs)}`);
    let currentMseqRaw = null;
    // -------------------------------------
    //  If I am a Follower-node then my job
    //  ends here, where I only read from store.
    // -------------------------------------
    let isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader && this.lastRequestedMediaSeqRaw !== null) {
      debug(`[${this.sessionId}]: FOLLOWER: Reading data from store!`);

      let leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");

      if (!leadersMediaSeqRaw < this.lastRequestedMediaSeqRaw && this.blockGenerateManifest) {
        this.blockGenerateManifest = false;
      }

      let attempts = 10;
      //  CHECK AGAIN CASE 1: Store Empty
      while (!leadersMediaSeqRaw && attempts > 0) {
        if (!leadersMediaSeqRaw) {
          isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          if (isLeader) {
            debug(`[${this.instanceId}]: I'm the new leader`);
            return;
          }
        }

        if (!this.allowedToSet) {
          debug(`[${this.sessionId}]: We are about to switch away from LIVE. Abort fetching from Store`);
          break;
        }
        const segDur = this._getAnyFirstSegmentDurationMs() || DEFAULT_PLAYHEAD_INTERVAL_MS;
        const waitTimeMs = parseInt(segDur / 3, 10);
        debug(`[${this.sessionId}]: FOLLOWER: Leader has not put anything in store... Will check again in ${waitTimeMs}ms (Tries left=[${attempts}])`);
        await timer(waitTimeMs);
        this.timerCompensation = false;
        leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
        attempts--;
      }

      if (!leadersMediaSeqRaw) {
        debug(`[${this.instanceId}]: The leader is still alive`);
        return;
      }

      let liveSegsInStore = await this.sessionLiveState.get("liveSegsForFollowers");
      attempts = 10;
      //  CHECK AGAIN CASE 2: Store Old
      while ((leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw && attempts > 0) || (this._containsSegment(this.liveSegsForFollowers, liveSegsInStore) && attempts > 0)) {
        if (!this.allowedToSet) {
          debug(`[${this.sessionId}]: We are about to switch away from LIVE. Abort fetching from Store`);
          break;
        }
        if (leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw) {
          isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          if (isLeader) {
            debug(`[${this.instanceId}][${this.sessionId}]: I'm the new leader`);
            return;
          }
        }
        if (this._containsSegment(this.liveSegsForFollowers, liveSegsInStore)) {
          debug(`[${this.sessionId}]: FOLLOWER: _containsSegment=true,${leadersMediaSeqRaw},${this.lastRequestedMediaSeqRaw}`);
        }
        const segDur = this._getAnyFirstSegmentDurationMs() || DEFAULT_PLAYHEAD_INTERVAL_MS;
        const waitTimeMs = parseInt(segDur / 3, 10);
        debug(`[${this.sessionId}]: FOLLOWER: Cannot find anything NEW in store... Will check again in ${waitTimeMs}ms (Tries left=[${attempts}])`);
        await timer(waitTimeMs);
        this.timerCompensation = false;
        leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
        liveSegsInStore = await this.sessionLiveState.get("liveSegsForFollowers");
        attempts--;
      }
      // FINALLY
      if (leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw) {
        debug(`[${this.instanceId}][${this.sessionId}]: The leader is still alive`);
        return;
      }
      // Follower updates its manifest building blocks (segment holders & counts)
      this.lastRequestedMediaSeqRaw = leadersMediaSeqRaw;
      this.liveSegsForFollowers = liveSegsInStore;
      debug(`[${this.sessionId}]: These are the segments from store: [${JSON.stringify(this.liveSegsForFollowers)}]`);
      this._updateLiveSegQueue();
      return;
    }

    // ---------------------------------
    // FETCHING FROM LIVE-SOURCE - New Followers (once) & Leaders do this.
    // ---------------------------------
    let FETCH_ATTEMPTS = 10;
    this.liveSegsForFollowers = {};
    let bandwidthsToSkipOnRetry = [];
    while (FETCH_ATTEMPTS > 0) {
      if (isLeader) {
        debug(`[${this.sessionId}]: LEADER: Trying to fetch manifests for all bandwidths\n Attempts left=[${FETCH_ATTEMPTS}]`);
      } else {
        debug(`[${this.sessionId}]: NEW FOLLOWER: Trying to fetch manifests for all bandwidths\n Attempts left=[${FETCH_ATTEMPTS}]`);
      }

      if (!this.allowedToSet) {
        debug(`[${this.sessionId}]: We are about to switch away from LIVE. Abort fetching from Live-Source`);
        break;
      }

      // Reset Values Each Attempt
      let livePromises = [];
      let manifestList = [];
      this.pushAmount = 0;
      try {
        if (bandwidthsToSkipOnRetry.length > 0) {
          debug(`[${this.sessionId}]: (X) Skipping loadMedia promises for bws ${JSON.stringify(bandwidthsToSkipOnRetry)}`);
        }
        // Collect Live Source Requesting Promises
        for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
          let bw = Object.keys(this.mediaManifestURIs)[i];
          if (bandwidthsToSkipOnRetry.includes(bw)) {
            continue;
          }
          livePromises.push(this._loadMediaManifest(bw));
          debug(`[${this.sessionId}]: Pushed loadMedia promise for bw=[${bw}]`);
        }
        // Fetch From Live Source
        debug(`[${this.sessionId}]: Executing Promises I: Fetch From Live Source`);
        manifestList = await allSettled(livePromises);
        livePromises = [];
      } catch (err) {
        debug(`[${this.sessionId}]: Promises I: FAILURE!\n${err}`);
        return;
      }

      // Handle if any promise got rejected
      if (manifestList.some((result) => result.status === "rejected")) {
        FETCH_ATTEMPTS--;
        debug(`[${this.sessionId}]: ALERT! Promises I: Failed, Rejection Found! Trying again in 1000ms...`);
        await timer(1000);
        continue;
      }

      // Store the results locally
      manifestList.forEach((variantItem) => {
        const bw = variantItem.value.bandwidth;
        if (!this.liveSourceM3Us[bw]) {
          this.liveSourceM3Us[bw] = {};
        }
        this.liveSourceM3Us[bw] = variantItem.value;
      });

      const allStoredMediaSeqCounts = Object.keys(this.liveSourceM3Us).map((variant) => this.liveSourceM3Us[variant].mediaSeq);

      // Handle if mediaSeqCounts are NOT synced up!
      if (!allStoredMediaSeqCounts.every((val, i, arr) => val === arr[0])) {
        debug(`[${this.sessionId}]: Live Mseq counts=[${allStoredMediaSeqCounts}]`);
        // Figure out what bw's are behind.
        const highestMediaSeqCount = Math.max(...allStoredMediaSeqCounts);
        bandwidthsToSkipOnRetry = Object.keys(this.liveSourceM3Us).filter((bw) => {
          if (this.liveSourceM3Us[bw].mediaSeq === highestMediaSeqCount) {
            return true;
          }
          return false;
        });
        // Decrement fetch counter
        FETCH_ATTEMPTS--;
        // Calculate retry delay time. Default=1000
        let retryDelayMs = 1000;
        if (Object.keys(this.liveSegQueue).length > 0) {
          const firstBw = Object.keys(this.liveSegQueue)[0];
          const lastIdx = this.liveSegQueue[firstBw].length - 1;
          if (this.liveSegQueue[firstBw][lastIdx].duration) {
            retryDelayMs = this.liveSegQueue[firstBw][lastIdx].duration * 1000 * 0.25;
          }
        }
        // Wait a little before trying again
        debug(`[${this.sessionId}]: ALERT! Live Source Data NOT in sync! Will try again after ${retryDelayMs}ms`);
        await timer(retryDelayMs);
        if (isLeader) {
          this.timerCompensation = false;
        }
        continue;
      }

      currentMseqRaw = allStoredMediaSeqCounts[0];

      if (!isLeader) {
        let leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
        let tries = 20;

        while ((!isLeader && !leadersFirstSeqCounts.liveSourceMseqCount && tries > 0) || leadersFirstSeqCounts.liveSourceMseqCount === 0) {
          debug(`[${this.sessionId}]: NEW FOLLOWER: Waiting for LEADER to add 'firstCounts' in store! Will look again after 1000ms (tries left=${tries})`);
          await timer(1000);
          leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
          tries--;
          // Might take over as Leader if Leader is not setting data due to being down.
          isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          if (isLeader) {
            debug(`[${this.sessionId}][${this.instanceId}]: I'm the new leader, and now I am going to add 'firstCounts' in store`);
          }
        }

        if (tries === 0) {
          isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          if (isLeader) {
            debug(`[${this.sessionId}][${this.instanceId}]: I'm the new leader, and now I am going to add 'firstCounts' in store`);
            break;
          } else {
            debug(`[${this.sessionId}][${this.instanceId}]: The leader is still alive`);
            leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
            if (!leadersFirstSeqCounts.liveSourceMseqCount) {
              debug(`[${this.sessionId}][${this.instanceId}]: Could not find 'firstCounts' in store. Abort Executing Promises II & Returning to Playhead.`);
              return;
            }
          }
        }

        if (isLeader) {
          debug(`[${this.sessionId}]: NEW LEADER: Original Leader went missing, I am retrying live source fetch...`);
          await this.sessionLiveState.set("transitSegs", this.vodSegments);
          debug(`[${this.sessionId}]: NEW LEADER: I am adding 'transitSegs' to Store for future followers`);
          continue;
        }

        // Respawners never do this, only starter followers.
        // Edge Case: FOLLOWER transitioned from session with different segments from LEADER
        if (leadersFirstSeqCounts.discSeqCount !== this.discSeqCount) {
          this.discSeqCount = leadersFirstSeqCounts.discSeqCount;
        }
        if (leadersFirstSeqCounts.mediaSeqCount !== this.mediaSeqCount) {
          this.mediaSeqCount = leadersFirstSeqCounts.mediaSeqCount;
          debug(
            `[${this.sessionId}]: FOLLOWER transistioned with wrong V2L segments, updating counts to [${this.mediaSeqCount}][${this.discSeqCount}], and reading 'transitSegs' from store`
          );
          const transitSegs = await this.sessionLiveState.get("transitSegs");
          if (!this._isEmpty(transitSegs)) {
            this.vodSegments = transitSegs;
          }
        }

        // Prepare to load segments...
        debug(`[${this.instanceId}][${this.sessionId}]: Newest mseq from LIVE=${currentMseqRaw} First mseq in store=${leadersFirstSeqCounts.liveSourceMseqCount}`);
        if (currentMseqRaw === leadersFirstSeqCounts.liveSourceMseqCount) {
          this.pushAmount = 1; // Follower from start
        } else {
          // TODO: To support and account for past discontinuity tags in the Live Source stream,
          // we will need to get the real 'current' discontinuity-sequence count from Leader somehow.

          // RESPAWNED NODES
          this.pushAmount = currentMseqRaw - leadersFirstSeqCounts.liveSourceMseqCount + 1;

          const transitSegs = await this.sessionLiveState.get("transitSegs");
          //debug(`[${this.sessionId}]: NEW FOLLOWER: I tried to get 'transitSegs'. This is what I found ${JSON.stringify(transitSegs)}`);
          if (!this._isEmpty(transitSegs)) {
            this.vodSegments = transitSegs;
          }
        }
        debug(`[${this.sessionId}]: ...pushAmount=${this.pushAmount}`);
      } else {
        // LEADER calculates pushAmount differently...
        if (this.firstTime) {
          this.pushAmount = 1; // Leader from start
        } else {
          this.pushAmount = currentMseqRaw - this.lastRequestedMediaSeqRaw;
          debug(`[${this.sessionId}]: ...calculating pushAmount=${currentMseqRaw}-${this.lastRequestedMediaSeqRaw}=${this.pushAmount}`);
        }
        debug(`[${this.sessionId}]: ...pushAmount=${this.pushAmount}`);
        break;
      }
      // Live Source Data is in sync, and LEADER & new FOLLOWER are in sync
      break;
    }

    if (FETCH_ATTEMPTS === 0) {
      debug(`[${this.sessionId}]: Fetching from Live-Source did not work! Returning to Playhead Loop...`);
      return;
    }

    isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    // NEW FOLLOWER - Edge Case: One Instance is ahead of another. Read latest live segs from store
    if (!isLeader) {
      const leadersCurrentMseqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
      const counts = await this.sessionLiveState.get("firstCounts");
      const leadersFirstMseqRaw = counts.liveSourceMseqCount;
      if (leadersCurrentMseqRaw !== null && leadersCurrentMseqRaw > currentMseqRaw) {
        // if leader never had any segs from prev mseq
        if (leadersFirstMseqRaw !== null && leadersFirstMseqRaw === leadersCurrentMseqRaw) {
          // Follower updates it's manifest ingedients (segment holders & counts)
          this.lastRequestedMediaSeqRaw = leadersCurrentMseqRaw;
          this.liveSegsForFollowers = await this.sessionLiveState.get("liveSegsForFollowers");
          debug(`[${this.sessionId}]: NEW FOLLOWER: Leader is ahead or behind me! Clearing Queue and Getting latest segments from store.`);
          this._updateLiveSegQueue();
          this.firstTime = false;
          debug(`[${this.sessionId}]: Got all needed segments from live-source (read from store).\nWe are now able to build Live Manifest: [${this.mediaSeqCount}]`);
          return;
        } else if (leadersCurrentMseqRaw < this.lastRequestedMediaSeqRaw) {
          // WE ARE A RESPAWN-NODE, and we are ahead of leader.
          this.blockGenerateManifest = true;
        }
      }
    }
    if (this.allowedToSet) {
      // Collect and Push Segment-Extracting Promises
      let pushPromises = [];
      for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
        let bw = Object.keys(this.mediaManifestURIs)[i];
        // will add new segments to live seg queue
        pushPromises.push(this._parseMediaManifest(this.liveSourceM3Us[bw].M3U, this.mediaManifestURIs[bw], bw, isLeader));
        debug(`[${this.sessionId}]: Pushed pushPromise for bw=${bw}`);
      }
      // Segment Pushing
      debug(`[${this.sessionId}]: Executing Promises II: Segment Pushing`);
      await allSettled(pushPromises);

      // UPDATE COUNTS, & Shift Segments in vodSegments and liveSegQueue if needed.
      const leaderORFollower = isLeader ? "LEADER" : "NEW FOLLOWER";
      const newTotalDuration = this._incrementAndShift(leaderORFollower);
      if (newTotalDuration) {
        debug(`[${this.sessionId}]: New Adjusted Playlist Duration=${newTotalDuration}s`);
      }
    }

    // -----------------------------------------------------
    // Leader writes to store so that Followers can read.
    // -----------------------------------------------------
    if (isLeader) {
      if (this.allowedToSet) {
        const liveBws = Object.keys(this.liveSegsForFollowers);
        const segListSize = this.liveSegsForFollowers[liveBws[0]].length;
        // Do not replace old data with empty data
        if (segListSize > 0) {
          debug(`[${this.sessionId}]: LEADER: Adding data to store!`);
          await this.sessionLiveState.set("lastRequestedMediaSeqRaw", this.lastRequestedMediaSeqRaw);
          await this.sessionLiveState.set("liveSegsForFollowers", this.liveSegsForFollowers);
        }
      }

      // [LASTLY]: LEADER does this for respawned-FOLLOWERS' sake.
      if (this.firstTime && this.allowedToSet) {
        // Buy some time for followers (NOT Respawned) to fetch their own L.S m3u8.
        await timer(1000); // maybe remove
        let firstCounts = await this.sessionLiveState.get("firstCounts");
        firstCounts.liveSourceMseqCount = this.lastRequestedMediaSeqRaw;
        firstCounts.mediaSeqCount = this.prevMediaSeqCount;
        firstCounts.discSeqCount = this.prevDiscSeqCount;

        debug(`[${this.sessionId}]: LEADER: I am adding 'firstCounts'=${JSON.stringify(firstCounts)} to Store for future followers`);
        await this.sessionLiveState.set("firstCounts", firstCounts);
      }
      debug(`[${this.sessionId}]: LEADER: I am using segs from Mseq=${this.lastRequestedMediaSeqRaw}`);
    } else {
      debug(`[${this.sessionId}]: NEW FOLLOWER: I am using segs from Mseq=${this.lastRequestedMediaSeqRaw}`);
    }

    this.firstTime = false;
    debug(`[${this.sessionId}]: Got all needed segments from live-source (from all bandwidths).\nWe are now able to build Live Manifest: [${this.mediaSeqCount}]`);

    return;
  }

  async _loadAllAudioManifests() {
    debug(`[${this.sessionId}]: Attempting to load all audio manifest URIs in=${Object.keys(this.audioManifestURIs[Object.keys(this.audioManifestURIs)[0]])}`);
    let currentMseqRaw = null;
    // -------------------------------------
    //  If I am a Follower-node then my job
    //  ends here, where I only read from store.
    // -------------------------------------
    let isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader && this.lastRequestedAudioSeqRaw !== null) {
      debug(`[${this.sessionId}]: FOLLOWER: Reading data from store!`);

      let leadersAudioSeqRaw = await this.sessionLiveState.get("lastRequestedAudioSeqRaw");

      if (!leadersAudioSeqRaw < this.lastRequestedAudioSeqRaw && this.blockGenerateManifest) {
        this.blockGenerateManifest = false;
      }

      let attempts = 10;
      //  CHECK AGAIN CASE 1: Store Empty
      while (!leadersAudioSeqRaw && attempts > 0) {
        if (!leadersAudioSeqRaw) {
          isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          if (isLeader) {
            debug(`[${this.instanceId}]: I'm the new leader`);
            return;
          }
        }

        if (!this.allowedToSet) {
          debug(`[${this.sessionId}]: We are about to switch away from LIVE. Abort fetching from Store`);
          break;
        }
        const segDur = this._getAnyFirstAudioSegmentDurationMs() || DEFAULT_PLAYHEAD_INTERVAL_MS;
        const waitTimeMs = parseInt(segDur / 3, 10);
        debug(`[${this.sessionId}]: FOLLOWER: Leader has not put anything in store... Will check again in ${waitTimeMs}ms (Tries left=[${attempts}])`);
        await timer(waitTimeMs);
        this.timerCompensation = false;
        leadersAudioSeqRaw = await this.sessionLiveState.get("lastRequestedAudioSeqRaw");
        attempts--;
      }

      if (!leadersAudioSeqRaw) {
        debug(`[${this.instanceId}]: The leader is still alive`);
        return;
      }

      let liveAudioSegsInStore = await this.sessionLiveState.get("liveAudioSegsForFollowers");
      attempts = 10;
      //  CHECK AGAIN CASE 2: Store Old
      while ((leadersAudioSeqRaw <= this.lastRequestedAudioSeqRaw && attempts > 0) || (this._containsAudioSegment(this.liveAudioSegsForFollowers, liveAudioSegsInStore) && attempts > 0)) {
        if (!this.allowedToSet) {
          debug(`[${this.sessionId}]: We are about to switch away from LIVE. Abort fetching from Store`);
          break;
        }
        if (leadersAudioSeqRaw <= this.lastRequestedAudioSeqRaw) {
          isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          if (isLeader) {
            debug(`[${this.instanceId}][${this.sessionId}]: I'm the new leader`);
            return;
          }
        }
        if (this._containsAudioSegment(this.liveAudioSegsForFollowers, liveAudioSegsInStore)) {
          debug(`[${this.sessionId}]: FOLLOWER: _containsSegment=true,${leadersAudioSeqRaw},${this.lastRequestedAudioSeqRaw}`);
        }
        const segDur = this._getAnyFirstAudioSegmentDurationMs() || DEFAULT_PLAYHEAD_INTERVAL_MS;
        const waitTimeMs = parseInt(segDur / 3, 10);
        debug(`[${this.sessionId}]: FOLLOWER: Cannot find anything NEW in store... Will check again in ${waitTimeMs}ms (Tries left=[${attempts}])`);
        await timer(waitTimeMs);
        this.timerCompensation = false;
        leadersAudioSeqRaw = await this.sessionLiveState.get("lastRequestedAudioSeqRaw");
        liveAudioSegsInStore = await this.sessionLiveState.get("liveAudioSegsForFollowers");
        attempts--;
      }
      // FINALLY
      if (leadersAudioSeqRaw <= this.lastRequestedAudioSeqRaw) {
        debug(`[${this.instanceId}][${this.sessionId}]: The leader is still alive`);
        return;
      }
      // Follower updates its manifest building blocks (segment holders & counts)
      this.lastRequestedAudioSeqRaw = leadersAudioSeqRaw;
      this.liveAudioSegsForFollowers = liveAudioSegsInStore;
      debug(`[${this.sessionId}]: These are the segments from store: [${JSON.stringify(this.liveAudioSegsForFollowers)}]`);
      this._updateLiveAudioSegQueue();
      return;
    }

    // ---------------------------------
    // FETCHING FROM LIVE-SOURCE - New Followers (once) & Leaders do this.
    // ---------------------------------
    let FETCH_ATTEMPTS = 10;
    this.liveAudioSegsForFollowers = {};
    let groupLangToSkipOnRetry = [];
    while (FETCH_ATTEMPTS > 0) {
      if (isLeader) {
        debug(`[${this.sessionId}]: LEADER: Trying to fetch manifests for all groups and language\n Attempts left=[${FETCH_ATTEMPTS}]`);
      } else {
        debug(`[${this.sessionId}]: NEW FOLLOWER: Trying to fetch manifests for all groups and language\n Attempts left=[${FETCH_ATTEMPTS}]`);
      }

      if (!this.allowedToSet) {
        debug(`[${this.sessionId}]: We are about to switch away from LIVE. Abort fetching from Live-Source`);
        break;
      }

      // Reset Values Each Attempt
      let livePromises = [];
      let manifestList = [];
      this.pushAmountAudio = 0;
      try {
        if (groupLangToSkipOnRetry.length > 0) {
          debug(`[${this.sessionId}]: (X) Skipping loadAudio promises for bws ${JSON.stringify(groupLangToSkipOnRetry)}`);
        }
        // Collect Live Source Requesting Promises
        const groupIds = Object.keys(this.audioManifestURIs)
        for (let i = 0; i < groupIds.length; i++) {
          let groupId = groupIds[i];
          let langs = Object.keys(this.audioManifestURIs[groupId]);
          for (let j = 0; j < langs.length; j++) {
            const lang = langs[j];
            if (groupLangToSkipOnRetry.includes(groupId + lang)) {
              continue;
            }
            livePromises.push(this._loadAudioManifest(groupId, lang));
            debug(`[${this.sessionId}]: Pushed loadAudio promise for groupId,lang=[${groupId}, ${lang}]`);
          }
        }
        // Fetch From Live Source
        debug(`[${this.sessionId}]: Executing Promises I: Fetch From Live Audio Source`);
        manifestList = await allSettled(livePromises);
        livePromises = [];
      } catch (err) {
        debug(`[${this.sessionId}]: Promises I: FAILURE!\n${err}`);
        return;
      }

      // Handle if any promise got rejected
      if (manifestList.some((result) => result.status === "rejected")) {
        FETCH_ATTEMPTS--;
        debug(`[${this.sessionId}]: ALERT! Promises I: Failed, Rejection Found! Trying again in 1000ms...`);
        await timer(1000);
        continue;
      }

      // Store the results locally
      manifestList.forEach((variantItem) => {
        const groupId = variantItem.value.groupId;
        const lang = variantItem.value.lang;
        if (!this.liveAudioSourceM3Us[groupId]) {
          this.liveAudioSourceM3Us[groupId] = {};
        }
        if (!this.liveAudioSourceM3Us[groupId][lang]) {
          this.liveAudioSourceM3Us[groupId][lang] = {};
        }
        this.liveAudioSourceM3Us[groupId][lang] = variantItem.value;
      });

      const allStoredAudioSeqCounts = [];//Object.keys(this.liveAudioSourceM3Us).map((variant) => this.liveSourceM3Us[variant].mediaSeq);

      const groupIds = Object.keys(this.liveAudioSourceM3Us)
      for (let i = 0; i < groupIds.length; i++) {
        const langs = Object.keys(this.liveAudioSourceM3Us[groupIds[i]]);
        for (let j = 0; j < langs.length; j++) {
          allStoredAudioSeqCounts.push(this.liveAudioSourceM3Us[groupIds[i]][langs[j]].mediaSeq);
        }
      }
      // Handle if mediaSeqCounts are NOT synced up!
      if (!allStoredAudioSeqCounts.every((val, i, arr) => val === arr[0])) {
        debug(`[${this.sessionId}]: Live audio Mseq counts=[${allStoredAudioSeqCounts}]`);
        // Figure out what group lang is behind.
        const highestMediaSeqCount = Math.max(...allStoredAudioSeqCounts);

        const gi = Object.keys(this.liveAudioSourceM3Us)
        for (let i = 0; i < gi.length; i++) {
          const langs = Object.keys(this.liveAudioSourceM3Us[groupIds[i]]);
          for (let j = 0; j < langs.length; j++) {
            if (this.liveSourceM3Us[gi[i]][langs[j]].mediaSeq === highestMediaSeqCount) {
              groupLangToSkipOnRetry.push(gi[i] + langs[j])
            }
          }
        }

        // Decrement fetch counter
        FETCH_ATTEMPTS--;
        // Calculate retry delay time. Default=1000
        let retryDelayMs = 1000;
        if (Object.keys(this.liveAudioSegQueue).length > 0) {
          const firstGroupId = Object.keys(this.liveAudioSegQueue)[0];
          const firstLang = Object.keys(this.liveAudioSegQueue[firstGroupId])[0];
          const lastIdx = this.liveAudioSegQueue[firstGroupId][firstLang].length - 1;
          if (this.liveAudioSegQueue[firstGroupId][lastIdx].duration) {
            retryDelayMs = this.liveAudioSegQueue[firstGroupId][lastIdx].duration * 1000 * 0.25;
          }
        }
        // Wait a little before trying again
        debug(`[${this.sessionId}]: ALERT! Live Source Data NOT in sync! Will try again after ${retryDelayMs}ms`);
        await timer(retryDelayMs);
        if (isLeader) {
          this.timerCompensation = false;
        }
        continue;
      }

      currentMseqRaw = allStoredAudioSeqCounts[0];

      if (!isLeader) {
        let leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
        let tries = 20;

        while ((!isLeader && !leadersFirstSeqCounts.liveSourceAudioMseqCount && tries > 0) || leadersFirstSeqCounts.liveAudioSourceMseqCount === 0) {
          debug(`[${this.sessionId}]: NEW FOLLOWER: Waiting for LEADER to add 'firstCounts' in store! Will look again after 1000ms (tries left=${tries})`);
          await timer(1000);
          leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
          tries--;
          // Might take over as Leader if Leader is not setting data due to being down.
          isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          if (isLeader) {
            debug(`[${this.sessionId}][${this.instanceId}]: I'm the new leader, and now I am going to add 'firstCounts' in store`);
          }
        }

        if (tries === 0) {
          isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          if (isLeader) {
            debug(`[${this.sessionId}][${this.instanceId}]: I'm the new leader, and now I am going to add 'firstCounts' in store`);
            break;
          } else {
            debug(`[${this.sessionId}][${this.instanceId}]: The leader is still alive`);
            leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
            if (!leadersFirstSeqCounts.liveSourceMseqCount) {
              debug(`[${this.sessionId}][${this.instanceId}]: Could not find 'firstCounts' in store. Abort Executing Promises II & Returning to Playhead.`);
              return;
            }
          }
        }

        if (isLeader) {
          debug(`[${this.sessionId}]: NEW LEADER: Original Leader went missing, I am retrying live source fetch...`);
          await this.sessionLiveState.set("transitAudioSegs", this.vodAudioSegments);
          debug(`[${this.sessionId}]: NEW LEADER: I am adding 'transitSegs' to Store for future followers`);
          continue;
        }

        // Respawners never do this, only starter followers.
        // Edge Case: FOLLOWER transitioned from session with different segments from LEADER
        if (leadersFirstSeqCounts.discSeqCount !== this.audioDiscSeqCount) {
          this.audioDiscSeqCount = leadersFirstSeqCounts.discSeqCount;
        }
        if (leadersFirstSeqCounts.audioSeqCount !== this.audioSeqCount) {
          this.audioSeqCount = leadersFirstSeqCounts.audioSeqCount;
          debug(
            `[${this.sessionId}]: FOLLOWER transitioned with wrong V2L segments, updating counts to [${this.audioSeqCount}][${this.audioDiscSeqCount}], and reading 'transitSegs' from store`
          );
          const transitSegs = await this.sessionLiveState.get("transitAudioSegs");
          if (!this._isEmpty(transitSegs)) {
            this.vodAudioSegments = transitSegs;
          }
        }

        // Prepare to load segments...
        debug(`[${this.instanceId}][${this.sessionId}]: Newest mseq from LIVE=${currentMseqRaw} First mseq in store=${leadersFirstSeqCounts.liveAudioSourceMseqCount}`);
        if (currentMseqRaw === leadersFirstSeqCounts.liveAudioSourceMseqCount) {
          this.pushAmountAudio = 1; // Follower from start
        } else {
          // TODO: To support and account for past discontinuity tags in the Live Source stream,
          // we will need to get the real 'current' discontinuity-sequence count from Leader somehow.

          // RESPAWNED NODES
          this.pushAmountAudio = currentMseqRaw - leadersFirstSeqCounts.liveAudioSourceMseqCount + 1;

          const transitSegs = await this.sessionLiveState.get("transitAudioSegs");
          //debug(`[${this.sessionId}]: NEW FOLLOWER: I tried to get 'transitSegs'. This is what I found ${JSON.stringify(transitSegs)}`);
          if (!this._isEmpty(transitSegs)) {
            this.vodAudioSegments = transitSegs;
          }
        }
        debug(`[${this.sessionId}]: ...pushAmount=${this.pushAmountAudio}`);
      } else {
        // LEADER calculates pushAmount differently...
        if (this.firstTimeAudio) {
          this.pushAmountAudio = 1; // Leader from start
        } else {
          this.pushAmountAudio = currentMseqRaw - this.lastRequestedAudioSeqRaw;
          debug(`[${this.sessionId}]: ...calculating pushAmount=${currentMseqRaw}-${this.lastRequestedAudioSeqRaw}=${this.pushAmountAudio}`);
        }
        debug(`[${this.sessionId}]: ...pushAmount=${this.pushAmountAudio}`);
        break;
      }
      // Live Source Data is in sync, and LEADER & new FOLLOWER are in sync
      break;
    }

    if (FETCH_ATTEMPTS === 0) {
      debug(`[${this.sessionId}]: Fetching from Live-Source did not work! Returning to Playhead Loop...`);
      return;
    }

    isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    // NEW FOLLOWER - Edge Case: One Instance is ahead of another. Read latest live segs from store
    if (!isLeader) {
      const leadersCurrentMseqRaw = await this.sessionLiveState.get("lastRequestedAudioSeqRaw");
      const counts = await this.sessionLiveState.get("firstCounts");
      const leadersFirstMseqRaw = counts.liveSourceAudioMseqCount;
      if (leadersCurrentMseqRaw !== null && leadersCurrentMseqRaw > currentMseqRaw) {
        // if leader never had any segs from prev mseq
        if (leadersFirstMseqRaw !== null && leadersFirstMseqRaw === leadersCurrentMseqRaw) {
          // Follower updates it's manifest ingedients (segment holders & counts)
          this.lastRequestedAudioSeqRaw = leadersCurrentMseqRaw;
          this.liveAudioSegsForFollowers = await this.sessionLiveState.get("liveAudioSegsForFollowers");
          debug(`[${this.sessionId}]: NEW FOLLOWER: Leader is ahead or behind me! Clearing Queue and Getting latest segments from store.`);
          this._updateLiveAudioSegQueue();
          this.firstTimeAudio = false;
          debug(`[${this.sessionId}]: Got all needed segments from live-source (read from store).\nWe are now able to build Audio Live Manifest: [${this.audioSeqCount}]`);
          return;
        } else if (leadersCurrentMseqRaw < this.lastRequestedAudioSeqRaw) {
          // WE ARE A RESPAWN-NODE, and we are ahead of leader.
          this.blockGenerateManifest = true;
        }
      }
    }
    if (this.allowedToSet) {
      // Collect and Push Segment-Extracting Promises
      let pushPromises = [];

      for (let i = 0; i < Object.keys(this.audioManifestURIs).length; i++) {
        let groupId = Object.keys(this.audioManifestURIs)[i];
        let langs = Object.keys(this.audioManifestURIs[groupId]);
        for (let j = 0; j < langs.length; j++) {
          let lang = langs[j];

          // will add new segments to live seg queue
          pushPromises.push(this._parseAudioManifest(this.liveAudioSourceM3Us[groupId][lang].M3U, this.audioManifestURIs[groupId][lang], groupId, lang, isLeader));
          debug(`[${this.sessionId}]: Pushed pushPromise for groupId=${groupId} & lang${lang}`);
        }
      }
      // Segment Pushing
      debug(`[${this.sessionId}]: Executing Promises II: Segment Pushing`);
      await allSettled(pushPromises);

      // UPDATE COUNTS, & Shift Segments in vodSegments and liveSegQueue if needed.
      const leaderORFollower = isLeader ? "LEADER" : "NEW FOLLOWER";
      const newTotalDuration = this._incrementAndShiftAudio(leaderORFollower); // might need audio
      if (newTotalDuration) {
        debug(`[${this.sessionId}]: New Adjusted Playlist Duration=${newTotalDuration}s`);
      }
    }

    // -----------------------------------------------------
    // Leader writes to store so that Followers can read.
    // -----------------------------------------------------
    if (isLeader) {
      if (this.allowedToSet) {
        const liveGroupIds = Object.keys(this.liveAudioSegsForFollowers);
        const liveLangs = Object.keys(this.liveAudioSegsForFollowers[liveGroupIds[0]]);
        const segListSize = this.liveAudioSegsForFollowers[liveGroupIds[0]][liveLangs[0]].length;
        // Do not replace old data with empty data
        if (segListSize > 0) {
          debug(`[${this.sessionId}]: LEADER: Adding data to store!`);
          await this.sessionLiveState.set("lastRequestedAudioSeqRaw", this.lastRequestedAudioSeqRaw);
          await this.sessionLiveState.set("liveAudioSegsForFollowers", this.liveAudioSegsForFollowers);
        }
      }

      // [LASTLY]: LEADER does this for respawned-FOLLOWERS' sake.
      if (this.firstTimeAudio && this.allowedToSet) {
        // Buy some time for followers (NOT Respawned) to fetch their own L.S m3u8.
        await timer(1000); // maybe remove
        let firstCounts = await this.sessionLiveState.get("firstCounts");
        firstCounts.liveSourceAudioMseqCount = this.lastRequestedAudioSeqRaw;
        firstCounts.audioSeqCount = this.prevAudioSeqCount;
        firstCounts.discAudioSeqCount = this.prevAudioDiscSeqCount;

        debug(`[${this.sessionId}]: LEADER: I am adding 'firstCounts'=${JSON.stringify(firstCounts)} to Store for future followers`);
        await this.sessionLiveState.set("firstCounts", firstCounts);
      }
      debug(`[${this.sessionId}]: LEADER: I am using segs from Mseq=${this.lastRequestedAudioSeqRaw}`);
    } else {
      debug(`[${this.sessionId}]: NEW FOLLOWER: I am using segs from Mseq=${this.lastRequestedAudioSeqRaw}`);
    }

    this.firstTimeAudio = false;
    debug(`[${this.sessionId}]: Got all needed segments from live-source (from all groupIds and langs).\nWe are now able to build Audi Live Manifest: [${this.audioSeqCount}]`);

    return;
  }

  _shiftSegments(opt) {
    let _totalDur = 0;
    let _segments = {};
    let _name = "";
    let _removedSegments = 0;
    let _removedDiscontinuities = 0;
    let _type = "VIDEO";

    if (opt && opt.totalDur) {
      _totalDur = opt.totalDur;
    }
    if (opt && opt.segments) {
      _segments = JSON.parse(JSON.stringify(opt.segments)); // clone it
    }
    if (opt && opt.name) {
      _name = opt.name || "NONE";
    }
    if (opt && opt.removedSegments) {
      _removedSegments = opt.removedSegments;
    }
    if (opt && opt.removedDiscontinuities) {
      _removedDiscontinuities = opt.removedDiscontinuities;
    }
    if (opt && opt.type) {
      _type = opt.type;
    }
    const bws = Object.keys(_segments);


    /* When Total Duration is past the Limit, start Shifting V2L|LIVE segments if found */
    while (_totalDur > TARGET_PLAYLIST_DURATION_SEC) {
      let result = null;
      if (_type === "VIDEO") {
        result = this._shiftMediaSegments(bws, _name, _segments, _totalDur);
      } else {
        result = this._shiftAudioSegments(bws, _name, _segments, _totalDur);
      }
      // Skip loop if there are no more segments to remove...
      if (!result) {
        return { totalDuration: _totalDur, removedSegments: _removedSegments, removedDiscontinuities: _removedDiscontinuities, shiftedSegments: _segments };
      }
      debug(`[${this.sessionId}]: ${_name}: (${_totalDur})s/(${TARGET_PLAYLIST_DURATION_SEC})s - Playlist Duration is Over the Target. Shift needed!`);
      _segments = result.segments;
      if (result.timeToRemove) {
        _totalDur -= result.timeToRemove;
        // Increment number of removed segments...
        _removedSegments++;
      }
      if (result.incrementDiscSeqCount) {
        // Update Session Live Discontinuity Sequence Count
        _removedDiscontinuities++;
      }
    }
    return { totalDuration: _totalDur, removedSegments: _removedSegments, removedDiscontinuities: _removedDiscontinuities, shiftedSegments: _segments };
  }

  _shiftMediaSegments(bws, _name, _segments) {
    if (_segments[bws[0]].length === 0) {
      return null;
    }
    let timeToRemove = 0;
    let incrementDiscSeqCount = false;

    // Shift Segments for each variant...
    for (let i = 0; i < bws.length; i++) {
      let seg = _segments[bws[i]].shift();
      if (i === 0) {
        debug(`[${this.sessionId}]: ${_name}: (${bws[i]}) Ejected from playlist->: ${JSON.stringify(seg, null, 2)}`);
      }
      if (seg && seg.discontinuity) {
        incrementDiscSeqCount = true;
        if (_segments[bws[i]].length > 0) {
          seg = _segments[bws[i]].shift();
          if (i === 0) {
            debug(`[${this.sessionId}]: ${_name}: (${bws[i]}) Ejected from playlist->: ${JSON.stringify(seg, null, 2)}`);
          }
        }
      }
      if (seg && seg.duration) {
        timeToRemove = seg.duration;
      }
    }
    return { timeToRemove: timeToRemove, incrementDiscSeqCount: incrementDiscSeqCount, segments: _segments }
  }

  _shiftAudioSegments(groupIds, _name, _segments) {
    const firstLang = Object.keys(_segments[groupIds[0]])[0];
    if (_segments[groupIds[0]][firstLang].length === 0) {
      return null;
    }
    let timeToRemove = 0;
    let incrementDiscSeqCount = false;

    // Shift Segments for each variant...
    for (let i = 0; i < groupIds.length; i++) {
      const langs = Object.keys(_segments[groupIds[i]]);
      for (let j = 0; j < langs.length; j++) {
        let seg = _segments[groupIds[i]][langs[j]].shift();
        if (i === 0) {
          debug(`[${this.sessionId}]: ${_name}: (${groupIds[i]}) Ejected from playlist->: ${JSON.stringify(seg, null, 2)}`);
        }
        if (seg && seg.discontinuity) {
          incrementDiscSeqCount = true;
          if (_segments[groupIds[i]][langs[j]].length > 0) {
            seg = _segments[groupIds[i]][langs[j]].shift();
            if (i === 0) {
              debug(`[${this.sessionId}]: ${_name}: (${groupIds[i]}) Ejected from playlist->: ${JSON.stringify(seg, null, 2)}`);
            }
          }
        }
        if (seg && seg.duration) {
          timeToRemove = seg.duration;
        }
      }
    }
    return { timeToRemove: timeToRemove, incrementDiscSeqCount: incrementDiscSeqCount, segments: _segments }
  }

  /**
   * Shifts V2L or LIVE items if total segment duration (V2L+LIVE) are over the target duration.
   * It will also update and increment SessionLive's MediaSeqCount and DiscSeqCount based
   * on what was shifted.
   * @param {string} instanceName Name of instance "LEADER" | "FOLLOWER"
   * @returns {number} The new total duration in seconds
   */
  _incrementAndShift(instanceName) {
    if (!instanceName) {
      instanceName = "UNKNOWN";
    }
    const vodBws = Object.keys(this.vodSegments);
    const liveBws = Object.keys(this.liveSegQueue);
    let vodTotalDur = 0;
    let liveTotalDur = 0;
    let totalDur = 0;
    let removedSegments = 0;
    let removedDiscontinuities = 0;

    // Calculate Playlist Total Duration
    this.vodSegments[vodBws[0]].forEach((seg) => {
      if (seg.duration) {
        vodTotalDur += seg.duration;
      }
    });
    this.liveSegQueue[liveBws[0]].forEach((seg) => {
      if (seg.duration) {
        liveTotalDur += seg.duration;
      }
    });
    totalDur = vodTotalDur + liveTotalDur;
    debug(`[${this.sessionId}]: ${instanceName}: L2L dur->: ${liveTotalDur}s | V2L dur->: ${vodTotalDur}s | Total dur->: ${totalDur}s`);

    /** --- SHIFT then INCREMENT --- **/

    // Shift V2L Segments
    const outputV2L = this._shiftSegments({
      name: instanceName,
      totalDur: totalDur,
      segments: this.vodSegments,
      removedSegments: removedSegments,
      removedDiscontinuities: removedDiscontinuities,
    });
    // Update V2L Segments
    this.vodSegments = outputV2L.shiftedSegments;
    // Update values
    totalDur = outputV2L.totalDuration;
    removedSegments = outputV2L.removedSegments;
    removedDiscontinuities = outputV2L.removedDiscontinuities;
    // Shift LIVE Segments
    const outputLIVE = this._shiftSegments({
      name: instanceName,
      totalDur: totalDur,
      segments: this.liveSegQueue,
      removedSegments: removedSegments,
      removedDiscontinuities: removedDiscontinuities,
    });
    // Update LIVE Segments
    this.liveSegQueue = outputLIVE.shiftedSegments;
    // Update values
    totalDur = outputLIVE.totalDuration;
    removedSegments = outputLIVE.removedSegments;
    removedDiscontinuities = outputLIVE.removedDiscontinuities;

    // Update Session Live Discontinuity Sequence Count...
    this.prevDiscSeqCount = this.discSeqCount;
    this.discSeqCount += removedDiscontinuities;
    // Update Session Live Media Sequence Count...
    this.prevMediaSeqCount = this.mediaSeqCount;
    this.mediaSeqCount += removedSegments;
    if (this.restAmount) {
      this.mediaSeqCount += this.restAmount;
      debug(`[${this.sessionId}]: ${instanceName}: Added restAmount=[${this.restAmount}] to 'mediaSeqCount'`);
      this.restAmount = 0;
    }

    if (this.discSeqCount !== this.prevDiscSeqCount) {
      debug(`[${this.sessionId}]: ${instanceName}: Incrementing Dseq Count from {${this.prevDiscSeqCount}} -> {${this.discSeqCount}}`);
    }
    debug(`[${this.sessionId}]: ${instanceName}: Incrementing Mseq Count from [${this.prevMediaSeqCount}] -> [${this.mediaSeqCount}]`);
    debug(`[${this.sessionId}]: ${instanceName}: Finished updating all Counts and Segment Queues!`);
    return totalDur;
  }

  _incrementAndShiftAudio(instanceName) {
    if (!instanceName) {
      instanceName = "UNKNOWN";
    }
    const vodGroupId = Object.keys(this.vodAudioSegments)[0];
    const vodLanguage = Object.keys(this.vodAudioSegments[vodGroupId])[0];
    const liveGroupId = Object.keys(this.liveAudioSegQueue)[0];
    const liveLanguage = Object.keys(this.liveAudioSegQueue[vodGroupId])[0];
    let vodTotalDur = 0;
    let liveTotalDur = 0;
    let totalDur = 0;
    let removedSegments = 0;
    let removedDiscontinuities = 0;

    // Calculate Playlist Total Duration
    this.vodAudioSegments[vodGroupId][vodLanguage].forEach((seg) => {
      if (seg.duration) {
        vodTotalDur += seg.duration;
      }
    });
    this.liveAudioSegQueue[liveGroupId][liveLanguage].forEach((seg) => {
      if (seg.duration) {
        liveTotalDur += seg.duration;
      }
    });
    totalDur = vodTotalDur + liveTotalDur;
    debug(`[${this.sessionId}]: ${instanceName}: L2L dur->: ${liveTotalDur}s | V2L dur->: ${vodTotalDur}s | Total dur->: ${totalDur}s`);

    /** --- SHIFT then INCREMENT --- **/

    // Shift V2L Segments
    const outputV2L = this._shiftSegments({
      name: instanceName,
      totalDur: totalDur,
      segments: this.vodAudioSegments,
      removedSegments: removedSegments,
      removedDiscontinuities: removedDiscontinuities,
      type: "AUDIO",
    });
    // Update V2L Segments
    this.vodAudioSegments = outputV2L.shiftedSegments;
    // Update values
    totalDur = outputV2L.totalDuration;
    removedSegments = outputV2L.removedSegments;
    removedDiscontinuities = outputV2L.removedDiscontinuities;
    // Shift LIVE Segments
    const outputLIVE = this._shiftSegments({
      name: instanceName,
      totalDur: totalDur,
      segments: this.liveAudioSegQueue,
      removedSegments: removedSegments,
      removedDiscontinuities: removedDiscontinuities,
      type: "AUDIO",
    });
    // Update LIVE Segments
    this.liveAudioSegQueue = outputLIVE.shiftedSegments;
    // Update values
    totalDur = outputLIVE.totalDuration;
    removedSegments = outputLIVE.removedSegments;
    removedDiscontinuities = outputLIVE.removedDiscontinuities;

    // Update Session Live Discontinuity Sequence Count...
    this.prevAudioDiscSeqCount = this.audioDiscSeqCount;
    this.audioDiscSeqCount += removedDiscontinuities;
    // Update Session Live Audio Sequence Count...
    this.prevAudioSeqCount = this.audioSeqCount;
    this.audioSeqCount += removedSegments;
    if (this.restAmountAudio) {
      this.audioSeqCount += this.restAmountAudio;
      debug(`[${this.sessionId}]: ${instanceName}: Added restAmountAudio=[${this.restAmountAudio}] to 'audioSeqCount'`);
      this.restAmountAudio = 0;
    }

    if (this.audioDiscSeqCount !== this.prevAudioDiscSeqCount) {
      debug(`[${this.sessionId}]: ${instanceName}: Incrementing Dseq Count from {${this.prevAudioDiscSeqCount}} -> {${this.audioDiscSeqCount}}`);
    }
    debug(`[${this.sessionId}]: ${instanceName}: Incrementing Mseq Count from [${this.prevAudioSeqCount}] -> [${this.audioSeqCount}]`);
    debug(`[${this.sessionId}]: ${instanceName}: Finished updating all Counts and Segment Queues!`);
    return totalDur;
  }

  async _loadMediaManifest(bw) {
    if (!this.sessionLiveState) {
      throw new Error("SessionLive not ready");
    }

    const liveTargetBandwidth = this._findNearestBw(bw, Object.keys(this.mediaManifestURIs));
    debug(`[${this.sessionId}]: Requesting bw=(${bw}), Nearest Bandwidth is: ${liveTargetBandwidth}`);
    // Get the target media manifest
    const mediaManifestUri = this.mediaManifestURIs[liveTargetBandwidth];
    const parser = m3u8.createStream();
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      debug(`[${this.sessionId}]: Request Timeout! Aborting Request to ${mediaManifestUri}`);
      controller.abort();
    }, FAIL_TIMEOUT);

    const response = await fetch(mediaManifestUri, { signal: controller.signal });
    try {
      response.body.pipe(parser);
    } catch (err) {
      debug(`[${this.sessionId}]: Error when piping response to parser! ${JSON.stringify(err)}`);
      return Promise.reject(err);
    } finally {
      clearTimeout(timeout);
    }
    return new Promise((resolve, reject) => {
      parser.on("m3u", (m3u) => {
        try {
          const resolveObj = {
            M3U: m3u,
            mediaSeq: m3u.get("mediaSequence"),
            bandwidth: liveTargetBandwidth,
          };
          resolve(resolveObj);
        } catch (exc) {
          debug(`[${this.sessionId}]: Error when parsing latest manifest`);
          reject(exc);
        }
      });
      parser.on("error", (exc) => {
        debug(`Parser Error: ${JSON.stringify(exc)}`);
        reject(exc);
      });
    });
  }

  async _loadAudioManifest(groupId, lang) {
    if (!this.sessionLiveState) {
      throw new Error("SessionLive not ready");
    }
    const liveTargetGroupLang = this._findAudioGroupAndLang(groupId, lang, this.audioManifestURIs);
    debug(`[${this.sessionId}]: Requesting groupId=(${groupId}) & lang=(${lang}), Nearest match is: ${JSON.stringify(liveTargetGroupLang)}`);
    // Get the target media manifest
    const audioManifestUri = this.audioManifestURIs[liveTargetGroupLang.audioGroupId][liveTargetGroupLang.audioLanguage];
    const parser = m3u8.createStream();
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      debug(`[${this.sessionId}]: Request Timeout! Aborting Request to ${audioManifestUri}`);
      controller.abort();
    }, FAIL_TIMEOUT);

    const response = await fetch(audioManifestUri, { signal: controller.signal });
    try {
      response.body.pipe(parser);
    } catch (err) {
      debug(`[${this.sessionId}]: Error when piping response to parser! ${JSON.stringify(err)}`);
      return Promise.reject(err);
    } finally {
      clearTimeout(timeout);
    }
    return new Promise((resolve, reject) => {
      parser.on("m3u", (m3u) => {
        try {
          const resolveObj = {
            M3U: m3u,
            mediaSeq: m3u.get("mediaSequence"),
            groupId: liveTargetGroupLang.audioGroupId,
            lang: liveTargetGroupLang.audioLanguage,
          };
          resolve(resolveObj);
        } catch (exc) {
          debug(`[${this.sessionId}]: Error when parsing latest manifest`);
          reject(exc);
        }
      });
      parser.on("error", (exc) => {
        debug(`Parser Error: ${JSON.stringify(exc)}`);
        reject(exc);
      });
    });
  }

  _parseMediaManifest(m3u, mediaManifestUri, liveTargetBandwidth, isLeader) {
    return new Promise(async (resolve, reject) => {
      try {
        if (!this.liveSegQueue[liveTargetBandwidth]) {
          this.liveSegQueue[liveTargetBandwidth] = [];
        }
        if (!this.liveSegsForFollowers[liveTargetBandwidth]) {
          this.liveSegsForFollowers[liveTargetBandwidth] = [];
        }
        let baseUrl = "";
        const m = mediaManifestUri.match(/^(.*)\/.*?$/);
        if (m) {
          baseUrl = m[1] + "/";
        }

        //debug(`[${this.sessionId}]: Current RAW Mseq:  [${m3u.get("mediaSequence")}]`);
        //debug(`[${this.sessionId}]: Previous RAW Mseq: [${this.lastRequestedMediaSeqRaw}]`);

        if (this.pushAmount >= 0) {
          this.lastRequestedMediaSeqRaw = m3u.get("mediaSequence");
        }
        this.targetDuration = m3u.get("targetDuration");
        let startIdx = m3u.items.PlaylistItem.length - this.pushAmount;
        if (startIdx < 0) {
          this.restAmount = startIdx * -1;
          startIdx = 0;
        }
        if (mediaManifestUri) {
          // push segments
          this._addLiveSegmentsToQueue(startIdx, m3u.items.PlaylistItem, baseUrl, liveTargetBandwidth, isLeader);
        }
        resolve();
      } catch (exc) {
        console.error("ERROR: " + exc);
        reject(exc);
      }
    });
  }

  _parseAudioManifest(m3u, audioPlaylistUri, liveTargetGroupId, liveTargetLanguage, isLeader) {
    return new Promise(async (resolve, reject) => {
      try {
        if (!this.liveAudioSegQueue[liveTargetGroupId]) {
          this.liveAudioSegQueue[liveTargetGroupId] = {};
        }
        if (!this.liveAudioSegQueue[liveTargetGroupId][liveTargetLanguage]) {
          this.liveAudioSegQueue[liveTargetGroupId][liveTargetLanguage] = [];
        }
        if (!this.liveAudioSegsForFollowers[liveTargetGroupId]) {
          this.liveAudioSegsForFollowers[liveTargetGroupId] = {};
        }
        if (!this.liveAudioSegsForFollowers[liveTargetGroupId][liveTargetLanguage]) {
          this.liveAudioSegsForFollowers[liveTargetGroupId][liveTargetLanguage] = [];
        }
        let baseUrl = "";
        const m = audioPlaylistUri.match(/^(.*)\/.*?$/);
        if (m) {
          baseUrl = m[1] + "/";
        }

        //debug(`[${this.sessionId}]: Current RAW Mseq:  [${m3u.get("mediaSequence")}]`);
        //debug(`[${this.sessionId}]: Previous RAW Mseq: [${this.lastRequestedAudioSeqRaw}]`);

        if (this.pushAmountAudio >= 0) {
          this.lastRequestedAudioSeqRaw = m3u.get("mediaSequence");
        }
        this.targetDuration = m3u.get("targetDuration");
        let startIdx = m3u.items.PlaylistItem.length - this.pushAmountAudio;
        if (startIdx < 0) {
          this.restAmountAudio = startIdx * -1;
          startIdx = 0;
        }
        if (audioPlaylistUri) {
          this._addLiveAudioSegmentsToQueue(startIdx, m3u.items.PlaylistItem, baseUrl, liveTargetGroupId, liveTargetLanguage, isLeader);
        }
        resolve();
      } catch (exc) {
        console.error("ERROR: " + exc);
        reject(exc);
      }
    });
  }

  /**
   * Collects 'new' PlaylistItems and converts them into custom SegmentItems,
   * then Pushes them to the LiveSegQueue for all variants.
   * @param {number} startIdx
   * @param {m3u8.Item.PlaylistItem} playlistItems
   * @param {string} baseUrl
   * @param {string} liveTargetBandwidth
   */
  _addLiveSegmentsToQueue(startIdx, playlistItems, baseUrl, liveTargetBandwidth, isLeader) {
    const leaderOrFollower = isLeader ? "LEADER" : "NEW FOLLOWER";

    for (let i = startIdx; i < playlistItems.length; i++) {
      let seg = {};
      let playlistItem = playlistItems[i];
      let segmentUri;
      let cueData = null;
      let daterangeData = null;
      let attributes = playlistItem["attributes"].attributes;
      if (playlistItem.properties.discontinuity) {
        this.liveSegQueue[liveTargetBandwidth].push({ discontinuity: true });
        this.liveSegsForFollowers[liveTargetBandwidth].push({ discontinuity: true });
      }
      if ("cuein" in attributes) {
        if (!cueData) {
          cueData = {};
        }
        cueData["in"] = true;
      }
      if ("cueout" in attributes) {
        if (!cueData) {
          cueData = {};
        }
        cueData["out"] = true;
        cueData["duration"] = attributes["cueout"];
      }
      if ("cuecont" in attributes) {
        if (!cueData) {
          cueData = {};
        }
        cueData["cont"] = true;
      }
      if ("scteData" in attributes) {
        if (!cueData) {
          cueData = {};
        }
        cueData["scteData"] = attributes["scteData"];
      }
      if ("assetData" in attributes) {
        if (!cueData) {
          cueData = {};
        }
        cueData["assetData"] = attributes["assetData"];
      }
      if ("daterange" in attributes) {
        if (!daterangeData) {
          daterangeData = {};
        }
        let allDaterangeAttributes = Object.keys(attributes["daterange"]);
        allDaterangeAttributes.forEach((attr) => {
          if (attr.match(/DURATION$/)) {
            daterangeData[attr.toLowerCase()] = parseFloat(attributes["daterange"][attr]);
          } else {
            daterangeData[attr.toLowerCase()] = attributes["daterange"][attr];
          }
        });
      }
      if (playlistItem.properties.uri) {
        if (playlistItem.properties.uri.match("^http")) {
          segmentUri = playlistItem.properties.uri;
        } else {
          segmentUri = url.resolve(baseUrl, playlistItem.properties.uri);
        }
        seg["duration"] = playlistItem.properties.duration;
        seg["uri"] = segmentUri;
        seg["cue"] = cueData;
        if (daterangeData) {
          seg["daterange"] = daterangeData;
        }
        // Push new Live Segments! But do not push duplicates
        const liveSegURIs = this.liveSegQueue[liveTargetBandwidth].filter((seg) => seg.uri).map((seg) => seg.uri);
        if (seg.uri && liveSegURIs.includes(seg.uri)) {
          debug(`[${this.sessionId}]: ${leaderOrFollower}: Found duplicate live segment. Skip push! (${liveTargetBandwidth})`);
        } else {
          this.liveSegQueue[liveTargetBandwidth].push(seg);
          this.liveSegsForFollowers[liveTargetBandwidth].push(seg);
          debug(`[${this.sessionId}]: ${leaderOrFollower}: Pushed segment (${seg.uri ? seg.uri : "Disc-tag"}) to 'liveSegQueue' (${liveTargetBandwidth})`);
        }
      }
    }
  }

  _addLiveAudioSegmentsToQueue(startIdx, playlistItems, baseUrl, liveTargetGroupId, liveTargetLanguage, isLeader) {
    const leaderOrFollower = isLeader ? "LEADER" : "NEW FOLLOWER";
    for (let i = startIdx; i < playlistItems.length; i++) {
      let seg = {};
      let playlistItem = playlistItems[i];
      let segmentUri;
      let cueData = null;
      let daterangeData = null;
      let attributes = playlistItem["attributes"].attributes;
      if (playlistItem.properties.discontinuity) {
        this.liveAudioSegQueue[liveTargetGroupId][liveTargetLanguage].push({ discontinuity: true });
        this.liveAudioSegsForFollowers[liveTargetGroupId][liveTargetLanguage].push({ discontinuity: true });
      }
      if ("cuein" in attributes) {
        if (!cueData) {
          cueData = {};
        }
        cueData["in"] = true;
      }
      if ("cueout" in attributes) {
        if (!cueData) {
          cueData = {};
        }
        cueData["out"] = true;
        cueData["duration"] = attributes["cueout"];
      }
      if ("cuecont" in attributes) {
        if (!cueData) {
          cueData = {};
        }
        cueData["cont"] = true;
      }
      if ("scteData" in attributes) {
        if (!cueData) {
          cueData = {};
        }
        cueData["scteData"] = attributes["scteData"];
      }
      if ("assetData" in attributes) {
        if (!cueData) {
          cueData = {};
        }
        cueData["assetData"] = attributes["assetData"];
      }
      if ("daterange" in attributes) {
        if (!daterangeData) {
          daterangeData = {};
        }
        let allDaterangeAttributes = Object.keys(attributes["daterange"]);
        allDaterangeAttributes.forEach((attr) => {
          if (attr.match(/DURATION$/)) {
            daterangeData[attr.toLowerCase()] = parseFloat(attributes["daterange"][attr]);
          } else {
            daterangeData[attr.toLowerCase()] = attributes["daterange"][attr];
          }
        });
      }

      if (playlistItem.properties.uri) {
        if (playlistItem.properties.uri.match("^http")) {
          segmentUri = playlistItem.properties.uri;
        } else {
          segmentUri = url.resolve(baseUrl, playlistItem.properties.uri);
        }
        seg["duration"] = playlistItem.properties.duration;
        seg["uri"] = segmentUri;
        seg["cue"] = cueData;
        if (daterangeData) {
          seg["daterange"] = daterangeData;
        }
        // Push new Live Segments! But do not push duplicates
        const liveSegURIs = this.liveAudioSegQueue[liveTargetGroupId][liveTargetLanguage].filter((seg) => seg.uri).map((seg) => seg.uri);
        if (seg.uri && liveSegURIs.includes(seg.uri)) {
          debug(`[${this.sessionId}]: ${leaderOrFollower}: Found duplicate live segment. Skip push! (${liveTargetGroupId, liveTargetLanguage})`);
        } else {
          this.liveAudioSegQueue[liveTargetGroupId][liveTargetLanguage].push(seg);
          this.liveAudioSegsForFollowers[liveTargetGroupId][liveTargetLanguage].push(seg);
          debug(`[${this.sessionId}]: ${leaderOrFollower}: Pushed segment (${seg.uri ? seg.uri : "Disc-tag"}) to 'liveSegQueue' (${liveTargetGroupId, liveTargetLanguage})`);
        }
      }
    }
  }

  /*
  ----------------------
    GENERATE MANIFEST
  ----------------------
  * Should be called independently from _loadAll...,_loadMedia...
  * So long Nodes are in sync!
  *
  * (returning null will cause the engine to try again after 1000ms)
  */
  async _GenerateLiveManifest(bw) {
    if (bw === null) {
      throw new Error("No bandwidth provided");
    }
    const liveTargetBandwidth = this._findNearestBw(bw, Object.keys(this.mediaManifestURIs));
    const vodTargetBandwidth = this._getNearestBandwidth(bw, Object.keys(this.vodSegments));
    debug(`[${this.sessionId}]: Client requesting manifest for bw=(${bw}). Nearest LiveBw=(${liveTargetBandwidth})`);

    if (this.blockGenerateManifest) {
      debug(`[${this.sessionId}]: FOLLOWER: Cannot Generate Manifest! Waiting to sync-up with Leader...`);
      return null;
    }

    // Uncomment below to guarantee that node always return the most current m3u8,
    // But it will cost an extra trip to store for every client request...
    /*
    //  DO NOT GENERATE MANIFEST CASE: Node is NOT in sync with Leader. (Store has new segs, but node hasn't read them yet)
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader) {
      let leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
      if (leadersMediaSeqRaw !== this.lastRequestedMediaSeqRaw) {
        debug(`[${this.sessionId}]: FOLLOWER: Cannot Generate Manifest! <${this.instanceId}> New segments need to be collected first!...`);
        return null;
      }
    }
    */

    //  DO NOT GENERATE MANIFEST CASE: Node has not found anything in store OR Node has not even check yet.
    if (Object.keys(this.liveSegQueue).length === 0 || (this.liveSegQueue[liveTargetBandwidth] && this.liveSegQueue[liveTargetBandwidth].length === 0)) {
      debug(`[${this.sessionId}]: Cannot Generate Manifest! <${this.instanceId}> Not yet collected ANY segments from Live Source...`);
      return null;
    }

    //  DO NOT GENERATE MANIFEST CASE: Node is in the middle of gathering segs of all variants.
    if (Object.keys(this.liveSegQueue).length !== 0) {
      let segAmounts = Object.keys(this.liveSegQueue).map((bw) => this.liveSegQueue[bw].length);
      if (!segAmounts.every((val, i, arr) => val === arr[0])) {
        debug(`[${this.sessionId}]: Cannot Generate Manifest! <${this.instanceId}> Not yet collected ALL segments from Live Source...`);
        return null;
      }
    }
    if (!this._isEmpty(this.liveSegQueue) && this.liveSegQueue[Object.keys(this.liveSegQueue)[0]].length !== 0) {
      this.targetDuration = this._getMaxDuration(this.liveSegQueue[Object.keys(this.liveSegQueue)[0]]);
    }

    // Determine if VOD segments influence targetDuration
    for (let i = 0; i < this.vodSegments[vodTargetBandwidth].length; i++) {
      let vodSeg = this.vodSegments[vodTargetBandwidth][i];
      // Get max duration amongst segments
      if (vodSeg.duration > this.targetDuration) {
        this.targetDuration = vodSeg.duration;
      }
    }

    debug(`[${this.sessionId}]: Started Generating the Manifest File:[${this.mediaSeqCount}]...`);
    let m3u8 = "#EXTM3U\n";
    m3u8 += "#EXT-X-VERSION:6\n";
    m3u8 += m3u8Header(this.instanceId);
    m3u8 += "#EXT-X-INDEPENDENT-SEGMENTS\n";
    m3u8 += "#EXT-X-TARGETDURATION:" + Math.round(this.targetDuration) + "\n";
    m3u8 += "#EXT-X-MEDIA-SEQUENCE:" + this.mediaSeqCount + "\n";
    m3u8 += "#EXT-X-DISCONTINUITY-SEQUENCE:" + this.discSeqCount + "\n";
    if (Object.keys(this.vodSegments).length !== 0) {
      // Add transitional segments if there are any left.
      debug(`[${this.sessionId}]: Adding a Total of (${this.vodSegments[vodTargetBandwidth].length}) VOD segments to manifest`);
      m3u8 = this._setMediaManifestTags(this.vodSegments, m3u8, vodTargetBandwidth);
      // Add live-source segments
      m3u8 = this._setMediaManifestTags(this.liveSegQueue, m3u8, liveTargetBandwidth);
    }
    debug(`[${this.sessionId}]: Manifest Generation Complete!`);
    return m3u8;
  }

  async _GenerateLiveAudioManifest(audioGroupId, audioLanguage) {
    if (audioGroupId === null) {
      throw new Error("No audioGroupId provided");
    }
    if (audioLanguage === null) {
      throw new Error("No audioLanguage provided");
    }
    const liveTargetTrackIds = this._findAudioGroupAndLang(audioGroupId, audioLanguage, this.audioManifestURIs);
    const vodTargetTrackIds = this._findAudioGroupAndLang(audioGroupId, audioLanguage, this.vodAudioSegments);
    debug(`[${this.sessionId}]: Client requesting manifest for VodTrackInfo=(${JSON.stringify(vodTargetTrackIds)}). Nearest LiveTrackInfo=(${JSON.stringify(liveTargetTrackIds)})`);

    if (this.blockGenerateManifest) {
      debug(`[${this.sessionId}]: FOLLOWER: Cannot Generate Audio Manifest! Waiting to sync-up with Leader...`);
      return null;
    }
              

    //  DO NOT GENERATE MANIFEST CASE: Node has not found anything in store OR Node has not even check yet.
    if (Object.keys(this.liveAudioSegQueue).length === 0 ||
      (this.liveAudioSegQueue[liveTargetTrackIds.audioGroupId] &&
        this.liveAudioSegQueue[liveTargetTrackIds.audioGroupId][liveTargetTrackIds.audioLanguage] &&
        this.liveAudioSegQueue[liveTargetTrackIds.audioGroupId][liveTargetTrackIds.audioLanguage].length === 0)
    ) {
      debug(`[${this.sessionId}]: Cannot Generate Audio Manifest! <${this.instanceId}> Not yet collected ANY segments from Live Source...`);
      return null;
    }

    //  DO NOT GENERATE MANIFEST CASE: Node is in the middle of gathering segs of all variants.
    const groupIds = Object.keys(this.liveAudioSegQueue);
    let segAmounts = [];
    for (let i = 0; i < groupIds.length; i++) {
      const groupId = groupIds[i];
      const langs = Object.keys(this.liveAudioSegQueue[groupId]);
      for (let j = 0; j < langs.length; j++) {
        const lang = langs[j];
        if (this.liveAudioSegQueue[groupId][lang].length !== 0) {
          
          segAmounts.push(this.liveAudioSegQueue[groupId][lang].length);
        }
      }
    }


    if (!segAmounts.every((val, i, arr) => val === arr[0])) {
      console(`[${this.sessionId}]: Cannot Generate audio Manifest! <${this.instanceId}> Not yet collected ALL segments from Live Source...`);
      return null;
    }

    if (!this._isEmpty(this.liveAudioSegQueue) && this.liveAudioSegQueue[groupIds[0]][Object.keys(this.liveAudioSegQueue[groupIds[0]])[0]].length !== 0) {
      this.targetDuration = this._getMaxDuration(this.liveAudioSegQueue[groupIds[0]][Object.keys(this.liveAudioSegQueue[groupIds[0]])[0]]);
    }

    // Determine if VOD segments influence targetDuration
    for (let i = 0; i < this.vodAudioSegments[vodTargetTrackIds.audioGroupId][vodTargetTrackIds.audioLanguage].length; i++) {
      let vodSeg = this.vodAudioSegments[vodTargetTrackIds.audioGroupId][vodTargetTrackIds.audioLanguage][i];
      // Get max duration amongst segments
      if (vodSeg.duration > this.targetDuration) {
        this.targetDuration = vodSeg.duration;
      }
    }

    debug(`[${this.sessionId}]: Started Generating the Audio Manifest File:[${this.audioSeqCount}]...`);
    let m3u8 = "#EXTM3U\n";
    m3u8 += "#EXT-X-VERSION:6\n";
    m3u8 += m3u8Header(this.instanceId);
    m3u8 += "#EXT-X-INDEPENDENT-SEGMENTS\n";
    m3u8 += "#EXT-X-TARGETDURATION:" + Math.round(this.targetDuration) + "\n";
    m3u8 += "#EXT-X-MEDIA-SEQUENCE:" + this.audioSeqCount + "\n";
    m3u8 += "#EXT-X-DISCONTINUITY-SEQUENCE:" + this.audioDiscSeqCount + "\n";
    if (Object.keys(this.vodAudioSegments).length !== 0) {
      // Add transitional segments if there are any left.
      debug(`[${this.sessionId}]: Adding a Total of (${this.vodAudioSegments[vodTargetTrackIds.audioGroupId][vodTargetTrackIds.audioLanguage].length}) VOD audio segments to manifest`);
      m3u8 = this._setAudioManifestTags(this.vodAudioSegments, m3u8, vodTargetTrackIds);
      // Add live-source segments
      m3u8 = this._setAudioManifestTags(this.liveAudioSegQueue, m3u8, liveTargetTrackIds);
    }
    debug(`[${this.sessionId}]: Audio manifest Generation Complete!`);
    return m3u8;
  }
  _setMediaManifestTags(segments, m3u8, bw) {
    for (let i = 0; i < segments[bw].length; i++) {
      const seg = segments[bw][i];
      m3u8 += this._setTagsOnSegment(seg, m3u8)
    }
    return m3u8
  }

  _setAudioManifestTags(segments, m3u8, trackIds) {
    for (let i = 0; i < segments[trackIds.audioGroupId][trackIds.audioLanguage].length; i++) {
      const seg = segments[trackIds.audioGroupId][trackIds.audioLanguage][i];
      m3u8 += this._setTagsOnSegment(seg, m3u8)
    }
    return m3u8
  }

  _setTagsOnSegment(segment) {
    let m3u8 = "";
    if (segment.discontinuity) {
      m3u8 += "#EXT-X-DISCONTINUITY\n";
    }
    if (segment.cue) {
      if (segment.cue.out) {
        if (segment.cue.scteData) {
          m3u8 += "#EXT-OATCLS-SCTE35:" + segment.cue.scteData + "\n";
        }
        if (segment.cue.assetData) {
          m3u8 += "#EXT-X-ASSET:" + segment.cue.assetData + "\n";
        }
        m3u8 += "#EXT-X-CUE-OUT:DURATION=" + segment.cue.duration + "\n";
      }
      if (segment.cue.cont) {
        if (segment.cue.scteData) {
          m3u8 += "#EXT-X-CUE-OUT-CONT:ElapsedTime=" + segment.cue.cont + ",Duration=" + segment.cue.duration + ",SCTE35=" + segment.cue.scteData + "\n";
        } else {
          m3u8 += "#EXT-X-CUE-OUT-CONT:" + segment.cue.cont + "/" + segment.cue.duration + "\n";
        }
      }
    }
    if (segment.datetime) {
      m3u8 += `#EXT-X-PROGRAM-DATE-TIME:${segment.datetime}\n`;
    }
    if (segment.daterange) {
      const dateRangeAttributes = Object.keys(segment.daterange)
        .map((key) => daterangeAttribute(key, segment.daterange[key]))
        .join(",");
      if (!segment.datetime && segment.daterange["start-date"]) {
        m3u8 += "#EXT-X-PROGRAM-DATE-TIME:" + segment.daterange["start-date"] + "\n";
      }
      m3u8 += "#EXT-X-DATERANGE:" + dateRangeAttributes + "\n";
    }
    // Mimick logic used in hls-vodtolive
    if (segment.cue && segment.cue.in) {
      m3u8 += "#EXT-X-CUE-IN" + "\n";
    }
    if (segment.uri) {
      m3u8 += "#EXTINF:" + segment.duration.toFixed(3) + ",\n";
      m3u8 += segment.uri + "\n";
    }
    return m3u8;
  }

  _findNearestBw(bw, array) {
    const sorted = array.sort((a, b) => b - a);
    return sorted.reduce((a, b) => {
      return Math.abs(b - bw) < Math.abs(a - bw) ? b : a;
    });
  }

  _getNearestBandwidth(bandwidthToMatch, array) {
    const sortedBandwidths = array.sort((a, b) => a - b);
    const exactMatch = sortedBandwidths.find((a) => a == bandwidthToMatch);
    if (exactMatch) {
      return exactMatch;
    }
    for (let i = 0; i < sortedBandwidths.length; i++) {
      if (Number(bandwidthToMatch) <= Number(sortedBandwidths[i])) {
        return sortedBandwidths[i];
      }
    }
    return sortedBandwidths[sortedBandwidths.length - 1];
  }

  _getFirstBwWithSegmentsInList(allSegments) {
    const bandwidths = Object.keys(allSegments);
    for (let i = 0; i < bandwidths.length; i++) {
      let bw = bandwidths[i];
      if (allSegments[bw].length > 0) {
        return bw;
      }
    }
    debug(`[${this.sessionId}]: ERROR Could not find any bandwidth with segments`);
    return null;
  }

  _getFirstAudioGroupWithSegments(array) {
    const audioGroupIds = Object.keys(array).filter((id) => {
      let idLangs = Object.keys(array[id]).filter((lang) => {
        return array[id][lang].length > 0;
      });
      return idLangs.length > 0;
    });
    if (audioGroupIds.length > 0) {
      return audioGroupIds[0];
    } else {
      return null;
    }
  }

  _getFirstAudioLanguageWithSegments(groupId, array) {
    const langsWithSegments = Object.keys(array[groupId]).filter((lang) => {
      return array[groupId][lang].length > 0;
    });
    if (langsWithSegments.length > 0) {
      return langsWithSegments[0];
    } else {
      return null;
    }
  }

  _findAudioGroupsForLang(audioLanguage, segments) {
    let trackInfos = []
    const groupIds = Object.keys(segments);
    for (let i = 0; i < groupIds.length; i++) {
      const groupId = groupIds[i];
      const langs = Object.keys(segments[groupId]);
      for (let j = 0; j < langs.length; j++) {
        const lang = langs[j];
        if (lang === audioLanguage) {

          trackInfos.push({ audioGroupId: groupId, audioLanguage: lang })
          break;
        }
      }
    }
    return trackInfos;
  }

  _findAudioGroupAndLang(audioGroupId, audioLanguage, array) {
    if (audioGroupId === null || !array[audioGroupId]) {
      audioGroupId = this._getFirstAudioGroupWithSegments(array);
      if (!audioGroupId) {
        return [];
      }
    }
    if (!array[audioGroupId][audioLanguage]) {
      const fallbackLang = this._getFirstAudioLanguageWithSegments(audioGroupId, array);
      if (!fallbackLang) {
        if (Object.keys(array[audioGroupId]).length > 0) {
          return {
            "audioGroupId": audioGroupId,
            "audioLanguage": Object.keys(array[audioGroupId])[0],
          };
        }
      }
      return {
        "audioGroupId": audioGroupId,
        "audioLanguage": fallbackLang
      };
    }
    return {
      "audioGroupId": audioGroupId,
      "audioLanguage": audioLanguage
    };

  }

  _getMaxDuration(segments) {
    if (!segments) {
      debug(`[${this.sessionId}]: ERROR segments is: ${segments}`);
    }
    let max = 0;
    for (let i = 0; i < segments.length; i++) {
      const seg = segments[i];
      if (!seg.discontinuity) {
        if (seg.duration > max) {
          max = seg.duration;
        }
      }
    }
    return max;
  }

  // To only use profiles that the channel will actually need.
  _filterLiveProfiles() {
    const profiles = this.sessionLiveProfile;
    const toKeep = new Set();
    let newItem = {};
    profiles.forEach((profile) => {
      let bwToKeep = this._getNearestBandwidth(profile.bw, Object.keys(this.mediaManifestURIs));
      toKeep.add(bwToKeep);
    });
    toKeep.forEach((bw) => {
      newItem[bw] = this.mediaManifestURIs[bw];
    });
    this.mediaManifestURIs = newItem;


  }

  _filterLiveAudioTracks() {
    let audioTracks = this.sessionAudioTracks;
    const toKeep = new Set();

    let newItemsAudio = {};
    audioTracks.forEach((audioTrack) => {
      let groupAndLangToKeep = this._findAudioGroupsForLang(audioTrack.language, this.audioManifestURIs);
      toKeep.add(...groupAndLangToKeep);
    });

    toKeep.forEach((trackInfo) => {
      if (!newItemsAudio[trackInfo.audioGroupId]) {
        newItemsAudio[trackInfo.audioGroupId] = {}
      }
      newItemsAudio[trackInfo.audioGroupId][trackInfo.audioLanguage] = this.audioManifestURIs[trackInfo.audioGroupId][trackInfo.audioLanguage];

    });

    this.audioManifestURIs = newItemsAudio;
  }

  _getAnyFirstSegmentDurationMs() {
    if (this._isEmpty(this.liveSegQueue)) {
      return null;
    }

    const bw0 = Object.keys(this.liveSegQueue)[0];
    if (this.liveSegQueue[bw0].length === 0) {
      return null;
    }

    for (let i = 0; i < this.liveSegQueue[bw0].length; i++) {
      const segment = this.liveSegQueue[bw0][i];
      if (!segment.duration) {
        continue;
      }
      return segment.duration * 1000;
    }

    return null;
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

  _containsSegment(segments, newSegments) {
    if (!segments || !newSegments) {
      return false;
    }
    if (Object.keys(segments).length === 0 || Object.keys(newSegments).length === 0) {
      return false;
    }
    const someBw = Object.keys(segments)[0];
    const segList = segments[someBw];
    const mostRecentSegment = segList[segList.length - 1];

    const segListNew = newSegments[someBw];
    const mostRecentSegmentNew = segListNew[segListNew.length - 1];

    if (mostRecentSegmentNew.uri === mostRecentSegment.uri) {
      return true;
    }
    return false;
  }
}

module.exports = SessionLive;
