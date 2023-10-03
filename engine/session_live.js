const debug = require("debug")("engine-session-live");
const allSettled = require("promise.allsettled");
const crypto = require("crypto");
const m3u8 = require("@eyevinn/m3u8");
const { segToM3u8 } = require("@eyevinn/hls-vodtolive/utils.js");
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
const HIGHEST_MEDIA_SEQUENCE_COUNT = 0;
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
const PlaylistTypes = Object.freeze({
  VIDEO: 1,
  AUDIO: 2,
  SUBTITLE: 3,
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
    this.vodSegmentsAudio = {};
    this.mediaManifestURIs = {};
    this.audioManifestURIs = {};
    this.liveSegQueue = {};
    this.lastRequestedMediaSeqRaw = null;
    this.liveSourceM3Us = {};
    this.liveSegQueueAudio = {};
    this.lastRequestedAudioSeqRaw = null;
    this.liveAudioSourceM3Us = {};
    this.playheadState = PlayheadState.IDLE;
    this.liveSegsForFollowers = {};
    this.liveSegsForFollowersAudio = {};
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
    debug(
      `[${this.instanceId}][${this.sessionId}]: LEADER: Resetting SessionLive values in Store ${
        resetDelay === 0 ? "Immediately" : `after a delay=(${resetDelay}ms)`
      }`
    );
    await timer(resetDelay);
    await this.sessionLiveState.set("liveSegsForFollowers", null);
    await this.sessionLiveState.set("lastRequestedMediaSeqRaw", null);
    await this.sessionLiveState.set("liveSegsForFollowersAudio", null);
    await this.sessionLiveState.set("lastRequestedAudioSeqRaw", null);
    await this.sessionLiveState.set("transitSegs", null);
    await this.sessionLiveState.set("transitSegsAudio", null);
    await this.sessionLiveState.set("firstCounts", {
      liveSourceMseqCount: null,
      liveSourceAudioMseqCount: null,
      mediaSeqCount: null,
      audioSeqCount: null,
      discSeqCount: null,
      audioDiscSeqCount: null,
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
    this.vodSegmentsAudio = {};
    this.mediaManifestURIs = {};
    this.audioManifestURIs = {};
    this.liveSegQueue = {};
    this.liveSegQueueAudio = {};
    this.lastRequestedMediaSeqRaw = null;
    this.lastRequestedAudioSeqRaw = null;
    this.liveSourceM3Us = {};
    this.liveAudioSourceM3Us = {};
    this.liveSegsForFollowers = {};
    this.liveSegsForFollowersAudio = {};
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
        await this._loadAllPlaylistManifests();
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
          this._filterLiveProfilesAudio();
          debug(`[${this.sessionId}]: Filtered Live audio tracks! (${Object.keys([Object.keys(this.audioManifestURIs)[0]]).length}) profiles left!`);
        }
      } catch (err) {
        console.error(err);
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
    if (this._isEmpty(this.vodSegmentsAudio)) {
      const groupIds = Object.keys(segments);
      for (let i = 0; i < groupIds.length; i++) {
        const groupId = groupIds[i];
        const langs = Object.keys(segments[groupId]);
        for (let j = 0; j < langs.length; j++) {
          const lang = langs[j];
          const audiotrack = this._getTrackFromGroupAndLang(groupId, lang);
          if (!this.vodSegmentsAudio[audiotrack]) {
            this.vodSegmentsAudio[audiotrack] = [];
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
            this.vodSegmentsAudio[audiotrack].push(v2lSegment);
          }

          const endIdx = segments[groupId][lang].length - 1;
          if (!segments[groupId][lang][endIdx].discontinuity) {
            const finalSegItem = { discontinuity: true };
            if (!cueInExists) {
              finalSegItem["cue"] = { in: true };
            }
            this.vodSegmentsAudio[audiotrack].push(finalSegItem);
          } else {
            if (!cueInExists) {
              segments[groupId][lang][endIdx]["cue"] = { in: true };
            }
          }
        }
      }
    } else {
      debug(`[${this.sessionId}]: 'vodSegmentsAudio' not empty = Using 'transitSegs'`);
    }
    debug(
      `[${this.sessionId}]: Setting CurrentAudioSequenceSegments. First seg is: [${
        this.vodSegmentsAudio[Object.keys(this.vodSegmentsAudio)[0]][0].uri
      }`
    );

    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (isLeader) {
      //debug(`[${this.sessionId}]: LEADER: I am adding 'transitSegs'=${JSON.stringify(this.vodSegments)} to Store for future followers`);
      await this.sessionLiveState.set("transitSegs", this.vodSegmentsAudio);
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
    debug(
      `[${this.sessionId}]: Setting mediaSeqCount, discSeqCount, audioSeqCount and audioDiscSeqCount to: [${mediaSeq}]:[${discSeq}], [${audioMediaSeq}]:[${audioDiscSeq}]`
    );
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
          this.vodSegmentsAudio = transitAudioSegs;
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
    return this.vodSegmentsAudio;
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
        this.liveSegsForFollowersAudio = await this.sessionLiveState.get("liveSegsForFollowersAudio");
        this._updateLiveSegQueueAudio();
      }
    }

    let currentAudioSequenceSegments = {};
    let segmentCount = 0;
    let increment = 0;
    const vodAudiotracks = Object.keys(this.vodSegmentsAudio);
    for (let vat of vodAudiotracks) {
      const liveTargetTrack = this._findNearestAudiotrack(vat, Object.keys(this.audioManifestURIs));
      const vodTargetTrack = vat;
      let vti = this._getGroupAndLangFromTrack(vat); // get the Vod Track Item
      if (!currentAudioSequenceSegments[vti.groupId]) {
        currentAudioSequenceSegments[vti.groupId] = {};
      }
      // Remove segments and disc-tag if they are on top
      if (this.vodSegmentsAudio[vodTargetTrack].length > 0 && this.vodSegmentsAudio[vodTargetTrack][0].discontinuity) {
        this.vodSegmentsAudio[vodTargetTrack].shift();
        increment = 1;
      }
      segmentCount = this.vodSegmentsAudio[vodTargetTrack].length;
      currentAudioSequenceSegments[vti.groupId][vti.language] = [];
      // In case we switch back before we've depleted all transitional segments
      currentAudioSequenceSegments[vti.groupId][vti.language] = this.vodSegmentsAudio[vodTargetTrack].concat(this.liveSegQueueAudio[liveTargetTrack]);
      currentAudioSequenceSegments[vti.groupId][vti.language].push({
        discontinuity: true,
        cue: { in: true },
      });
      debug(`[${this.sessionId}]: Getting current audio segments for ${vodTargetTrack}`);
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
      audioSeq: this.mediaSeqCount,
      audioDiscSeq: this.discSeqCount,
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
        throw new Error(
          `[${this.instanceId}][${this.sessionId}]: Failed to generate audio manifest. Live Session might have ended already. \n${exc}`
        );
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
            let audioGroupId = streamItem.get("audio");
            let audioGroupItems = m3u.items.MediaItem.filter((item) => {
              return item.get("type") === "AUDIO" && item.get("group-id") === audioGroupId;
            });
            // # Find all langs amongst the mediaItems that have this group id.
            // # It extracts each mediaItems language attribute value.
            // # ALSO initialize in this.audioSegments a lang. property who's value is an array [{seg1}, {seg2}, ...].
            audioGroupItems.map((item) => {
              let itemLang;
              if (!item.get("language")) {
                itemLang = item.get("name");
              } else {
                itemLang = item.get("language");
              }
              const audiotrack = this._getTrackFromGroupAndLang(audioGroupId, itemLang);
              if (!this.audioManifestURIs[audiotrack]) {
                this.audioManifestURIs[audiotrack] = "";
              }
              const audioManifestUri = url.resolve(baseUrl, item.get("uri"));
              this.audioManifestURIs[audiotrack] = audioManifestUri;
            });
          }
        }
        debug(
          `[${this.sessionId}]: All Live Media Manifest URIs have been collected. (${Object.keys(this.mediaManifestURIs).length}) profiles found!`
        );
        debug(`[${this.sessionId}]: All Live Audio Manifest URIs have been collected. (${Object.keys(this.audioManifestURIs).length}) tracks found!`);
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
    try {
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
            debug(
              `[${this.sessionId}]: FOLLOWER: Pushed Video segment (${
                liveSegFromLeader.uri ? liveSegFromLeader.uri : "Disc-tag"
              }) to 'liveSegQueue' (${liveBw})`
            );
          }
        }
      }
      // Remove older segments and update counts
      const newTotalDuration = this._incrementAndShift("FOLLOWER");
      if (newTotalDuration) {
        debug(`[${this.sessionId}]: FOLLOWER: New Adjusted Playlist Duration=${newTotalDuration}s`);
      }
    } catch (e) {
      console.error(e);
      return Promise.reject(e);
    }
  }

  _updateLiveSegQueueAudio() {
    try {
      let followerAudiotracks = Object.keys(this.liveSegsForFollowersAudio);
      if (this.liveSegsForFollowersAudio[followerAudiotracks[0]].length === 0) {
        debug(`[${this.sessionId}]: FOLLOWER: Error No Audio Segments found at all.`);
      }
      const size = this.liveSegsForFollowersAudio[followerAudiotracks[0]].length;
      // Push the New Live Segments to All Variants
      for (let segIdx = 0; segIdx < size; segIdx++) {
        for (let i = 0; i < followerAudiotracks.length; i++) {
          const fat = followerAudiotracks[i];
          const liveSegFromLeader = this.liveSegsForFollowersAudio[fat][segIdx];
          if (!this.liveSegQueueAudio[fat]) {
            this.liveSegQueueAudio[fat] = [];
          }
          // Do not push duplicates
          const liveSegURIs = this.liveSegQueueAudio[fat].filter((seg) => seg.uri).map((seg) => seg.uri);
          if (liveSegFromLeader.uri && liveSegURIs.includes(liveSegFromLeader.uri)) {
            debug(`[${this.sessionId}]: FOLLOWER: Found duplicate live segment. Skip push! (${liveGroupId})`);
          } else {
            this.liveSegQueueAudio[fat].push(liveSegFromLeader);
            debug(
              `[${this.sessionId}]: FOLLOWER: Pushed Audio segment (${
                liveSegFromLeader.uri ? liveSegFromLeader.uri : "Disc-tag"
              }) to 'liveSegQueueAudio' (${fat})`
            );
          }
        }
      }
      // Remove older segments and update counts
      const newTotalDuration = this._incrementAndShiftAudio("FOLLOWER");
      if (newTotalDuration) {
        debug(`[${this.sessionId}]: FOLLOWER: New Adjusted Playlist Duration=${newTotalDuration}s`);
      }
    } catch (e) {
      console.error(e);
      return Promise.reject(e);
    }
  }

  async _collectSegmentsFromStore() {
    try {
      // check if audio is enabled
      let hasAudio = this.audioManifestURIs.length > 0 ? true : false;
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
          debug(
            `[${this.sessionId}]: FOLLOWER: Leader has not put anything in store... Will check again in ${waitTimeMs}ms (Tries left=[${attempts}])`
          );
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
        let liveSegsInStoreAudio = hasAudio ? await this.sessionLiveState.get("liveSegsForFollowersAudio") : null;
        attempts = 10;
        //  CHECK AGAIN CASE 2: Store Old
        while (
          (leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw && attempts > 0) ||
          (this._containsSegment(this.liveSegsForFollowers, liveSegsInStore) && attempts > 0)
        ) {
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
          liveSegsInStoreAudio = hasAudio ? await this.sessionLiveState.get("liveSegsForFollowersAudio") : null;
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
        this.liveSegsForFollowersAudio = liveSegsInStoreAudio;
        debug(
          `[${this.sessionId}]: These are the segments from store:\nV[${JSON.stringify(this.liveSegsForFollowers)}]${
            hasAudio ? `\nA[${JSON.stringify(this.liveSegsForFollowersAudio)}]` : ""
          }`
        );
        this._updateLiveSegQueue();
        if (hasAudio) {
          this._updateLiveSegQueueAudio();
        }
        return;
      }
    } catch (e) {
      console.error(e);
      return Promise.reject(e);
    }
  }

  async _fetchFromLiveSource() {
    try {
      let isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);

      let currentMseqRaw = null;
      let FETCH_ATTEMPTS = 10;
      this.liveSegsForFollowers = {};
      this.liveSegsForFollowersAudio = {};
      let bandwidthsToSkipOnRetry = [];
      let audiotracksToSkipOnRetry = [];
      const audioTracksExist = Object.keys(this.audioManifestURIs).length > 0 ? true : false;
      debug(`[${this.sessionId}]: Attempting to load all MEDIA manifest URIs in=${Object.keys(this.mediaManifestURIs)}`);
      if (audioTracksExist) {
        debug(`[${this.sessionId}]: Attempting to load all AUDIO manifest URIs in=${Object.keys(this.audioManifestURIs)}`);
      }
      // ---------------------------------
      // FETCHING FROM LIVE-SOURCE - New Followers (once) & Leaders do this.
      // ---------------------------------
      while (FETCH_ATTEMPTS > 0) {
        const MSG_1 = (rank, id, count, hasAudio) => {
          return `[${id}]: ${rank}: Trying to fetch manifests for all bandwidths${hasAudio ? " and audiotracks" : ""}\n Attempts left=[${count}]`;
        };

        if (isLeader) {
          debug(MSG_1("LEADER", this.sessionId, FETCH_ATTEMPTS, audioTracksExist));
        } else {
          debug(MSG_1("NEW FOLLOWER", this.sessionId, FETCH_ATTEMPTS, audioTracksExist));
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
          if (audiotracksToSkipOnRetry.length > 0) {
            debug(`[${this.sessionId}]: (X) Skipping loadMedia promises for audiotracks ${JSON.stringify(audiotracksToSkipOnRetry)}`);
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
          // Collect Live Source Requesting Promises (audio)
          for (let i = 0; i < Object.keys(this.audioManifestURIs).length; i++) {
            let atStr = Object.keys(this.audioManifestURIs)[i];
            if (audiotracksToSkipOnRetry.includes(atStr)) {
              continue;
            }
            livePromises.push(this._loadAudioManifest(atStr));
            debug(`[${this.sessionId}]: Pushed loadMedia promise for audiotrack=${atStr}`);
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
          console.log(
            manifestList.map((r) => {
              return { status: r.status };
            })
          );
          await timer(1000);
          continue;
        }

        // Fill "liveSourceM3Us" and Store the results locally
        manifestList.forEach((variantItem) => {
          let variantKey = "";
          if (variantItem.value.bandwidth) {
            variantKey = variantItem.value.bandwidth;
          } else if (variantItem.value.audiotrack) {
            variantKey = variantItem.value.audiotrack;
          } else {
            console.error("NO 'bandwidth' or 'audiotrack' in item:", JSON.stringify(variantItem));
          }
          if (!this.liveSourceM3Us[variantKey]) {
            this.liveSourceM3Us[variantKey] = {};
          }
          this.liveSourceM3Us[variantKey] = variantItem.value;
        });

        const allStoredMediaSeqCounts = Object.keys(this.liveSourceM3Us).map((variant) => this.liveSourceM3Us[variant].mediaSeq);

        // Handle if mediaSeqCounts are NOT synced up!
        if (!allStoredMediaSeqCounts.every((val, i, arr) => val === arr[0])) {
          bandwidthsToSkipOnRetry = [];
          audiotracksToSkipOnRetry = [];
          debug(`[${this.sessionId}]: Live Mseq counts=[${allStoredMediaSeqCounts}]`);
          // Figure out what variants's are behind.
          HIGHEST_MEDIA_SEQUENCE_COUNT = Math.max(...allStoredMediaSeqCounts);
          Object.keys(this.liveSourceM3Us).map((variantKey) => {
            if (this.liveSourceM3Us[variantKey].mediaSeq === HIGHEST_MEDIA_SEQUENCE_COUNT) {
              if (this._isBandwidth(variantKey)) {
                bandwidthsToSkipOnRetry.push(variantKey);
              } else {
                audiotracksToSkipOnRetry.push(variantKey);
              }
            }
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
          // If 3 tries already and only video is unsynced, Make the BAD VARIANTS INHERIT M3U's from the good ones.
          if (FETCH_ATTEMPTS >= 7 && audiotracksToSkipOnRetry.length === this.audioManifestURIs.length) {
            // Find Highest MSEQ
            let [ahead, behind] = Object.keys(this.liveSourceM3Us).map((v) => {
              const c = this.liveSourceM3Us[v].mediaSeq;
              const a = [];
              const b = [];
              if (c === HIGHEST_MEDIA_SEQUENCE_COUNT) {
                a.push({ c, v });
              } else {
                b.push({ c, v });
              }
            });
            // Find lowest bitrate with that highest MSEQ
            const variantToPaste = ahead.reduce((min, item) => (item.v < min.v ? item : min), list[0]);
            // Reassign that bitrate onto the one's originally planned for retry
            const m3uToPaste = this.liveSourceM3Us[variantToPaste];
            behind.forEach((item) => {
              this.liveSourceM3Us[item.v] = m3uToPaste;
            });
            debug(`[${this.sessionId}]: ALERT! Live Source Data NOT in sync! Will fake sync by copy-pasting segments from best mseq`);
          } else {
            // Wait a little before trying again
            debug(`[${this.sessionId}]: ALERT! Live Source Data NOT in sync! Will try again after ${retryDelayMs}ms`);
            await timer(retryDelayMs);
            if (isLeader) {
              this.timerCompensation = false;
            }
            continue;
          }
        }

        currentMseqRaw = allStoredMediaSeqCounts[0];

        if (!isLeader) {
          let leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
          let tries = 20;

          while ((!isLeader && !leadersFirstSeqCounts.liveSourceMseqCount && tries > 0) || leadersFirstSeqCounts.liveSourceMseqCount === 0) {
            debug(
              `[${this.sessionId}]: NEW FOLLOWER: Waiting for LEADER to add 'firstCounts' in store! Will look again after 1000ms (tries left=${tries})`
            );
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
                debug(
                  `[${this.sessionId}][${this.instanceId}]: Could not find 'firstCounts' in store. Abort Executing Promises II & Returning to Playhead.`
                );
                return;
              }
            }
          }

          if (isLeader) {
            debug(`[${this.sessionId}]: NEW LEADER: Original Leader went missing, I am retrying live source fetch...`);
            await this.sessionLiveState.set("transitSegs", this.vodSegments);
            if (audioTracksExist) {
              await this.sessionLiveState.set("transitSegsAudio", this.vodSegmentsAudio);
            }
            debug(
              `[${this.sessionId}]: NEW LEADER: I am adding 'transitSegs'${
                audioTracksExist ? "and 'transitSegsAudio'" : ""
              } to Store for future followers`
            );
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
            if (audioTracksExist) {
              const transitSegsAudio = await this.sessionLiveState.get("transitSegsAudio");
              if (!this._isEmpty(transitSegsAudio)) {
                this.vodSegmentsAudio = transitSegsAudio;
              }
            }
          }

          // Prepare to load segments...
          debug(
            `[${this.instanceId}][${this.sessionId}]: Newest mseq from LIVE=${currentMseqRaw} First mseq in store=${leadersFirstSeqCounts.liveSourceMseqCount}`
          );
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
            if (audioTracksExist) {
              const transitSegsAudio = await this.sessionLiveState.get("transitSegsAudio");
              if (!this._isEmpty(transitSegsAudio)) {
                this.vodSegmentsAudio = transitSegsAudio;
              }
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
      return {
        success: FETCH_ATTEMPTS ? true : false,
        currentMseqRaw: currentMseqRaw,
      };
    } catch (e) {
      console.error(e);
      return Promise.reject(e);
    }
  }

  async _parseFromLiveSource(current_mediasequence_raw) {
    try {
      // ---------------------------------
      // PARSE M3U's FROM LIVE-SOURCE
      // ---------------------------------
      let isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
      const audioTracksExist = Object.keys(this.audioManifestURIs).length > 0 ? true : false;
      // NEW FOLLOWER - Edge Case: One Instance is ahead of another. Read latest live segs from store
      if (!isLeader) {
        const leadersCurrentMseqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
        const counts = await this.sessionLiveState.get("firstCounts");
        const leadersFirstMseqRaw = counts.liveSourceMseqCount;
        if (leadersCurrentMseqRaw !== null && leadersCurrentMseqRaw > current_mediasequence_raw) {
          // if leader never had any segs from prev mseq
          if (leadersFirstMseqRaw !== null && leadersFirstMseqRaw === leadersCurrentMseqRaw) {
            // Follower updates it's manifest ingedients (segment holders & counts)
            this.lastRequestedMediaSeqRaw = leadersCurrentMseqRaw;
            this.liveSegsForFollowers = await this.sessionLiveState.get("liveSegsForFollowers");
            if (audioTracksExist) {
              this.liveSegsForFollowersAudio = await this.sessionLiveState.get("liveSegsForFollowersAudio");
            }

            debug(`[${this.sessionId}]: NEW FOLLOWER: Leader is ahead or behind me! Clearing Queue and Getting latest segments from store.`);
            this._updateLiveSegQueue();
            if (audioTracksExist) {
              this._updateLiveSegQueueAudio();
            }

            this.firstTime = false;
            debug(
              `[${this.sessionId}]: Got all needed segments from live-source (read from store).\nWe are now able to build Live Manifest: [${this.mediaSeqCount}]`
            );
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
        // Collect and Push Segment-Extracting Promises (audio)
        for (let i = 0; i < Object.keys(this.audioManifestURIs).length; i++) {
          let at = Object.keys(this.audioManifestURIs)[i];
          // will add new segments to live seg queue
          pushPromises.push(this._parseAudioManifest(this.liveSourceM3Us[at].M3U, this.audioManifestURIs[at], at, isLeader));
          debug(`[${this.sessionId}]: Pushed pushPromise for audiotrack=${at}`);
        }
        // Segment Pushing
        debug(`[${this.sessionId}]: Executing Promises II: Segment Pushing`);
        await allSettled(pushPromises);

        // UPDATE COUNTS, & Shift Segments in vodSegments and liveSegQueue if needed.
        const leaderORFollower = isLeader ? "LEADER" : "NEW FOLLOWER";
        const newTotalDuration = this._incrementAndShift(leaderORFollower);
        if (audioTracksExist) {
          this._incrementAndShiftAudio(leaderORFollower);
        }
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
            if (audioTracksExist) {
              await this.sessionLiveState.set("liveSegsForFollowersAudio", this.liveSegsForFollowersAudio);
            }
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
      debug(
        `[${this.sessionId}]: Got all needed segments from live-source (from all bandwidths).\nWe are now able to build Live Manifest: [${this.mediaSeqCount}]`
      );

      return;
    } catch (e) {
      console.error(e);
      return Promise.reject(e);
    }
  }
  /**
   * This function adds new live segments to the node from which it can
   * generate new manifests from. Method for attaining new segments differ
   * depending on node Rank. The Leader collects from live source and
   * Followers collect from shared storage.
   */
  async _loadAllPlaylistManifests() {
    try {
      let isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
      if (!isLeader && this.lastRequestedMediaSeqRaw !== null) {
        // FOLLWERS Do this
        await this._collectSegmentsFromStore();
      } else {
        // LEADERS and NEW-FOLLOWERS Do this
        const result = await this._fetchFromLiveSource();
        if (result.success) {
          await this._parseFromLiveSource(result.currentMseqRaw);
        }
      }
      return;
    } catch (e) {
      console.error("Failure in _loadAllPlaylistManifests:" + e);
    }
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
    const variantKeys = Object.keys(_segments);

    /* When Total Duration is past the Limit, start Shifting V2L|LIVE segments if found */
    while (_totalDur > TARGET_PLAYLIST_DURATION_SEC) {
      let result = null;
      result = this._shiftVariantSegments(variantKeys, _name, _segments);
      // Skip loop if there are no more segments to remove...
      if (!result) {
        return {
          totalDuration: _totalDur,
          removedSegments: _removedSegments,
          removedDiscontinuities: _removedDiscontinuities,
          shiftedSegments: _segments,
        };
      }
      debug(
        `[${this.sessionId}]: ${_name}: (${_totalDur})s/(${TARGET_PLAYLIST_DURATION_SEC})s - Playlist Duration is Over the Target. Shift needed!`
      );
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
    return {
      totalDuration: _totalDur,
      removedSegments: _removedSegments,
      removedDiscontinuities: _removedDiscontinuities,
      shiftedSegments: _segments,
    };
  }

  _shiftVariantSegments(variantKeys, _name, _segments) {
    if (_segments[variantKeys[0]].length === 0) {
      return null;
    }
    let timeToRemove = 0;
    let incrementDiscSeqCount = false;

    // Shift Segments for each variant...
    for (let i = 0; i < variantKeys.length; i++) {
      let seg = _segments[variantKeys[i]].shift();
      if (i === 0) {
        debug(`[${this.sessionId}]: ${_name}: (${variantKeys[i]}) Ejected from playlist->: ${JSON.stringify(seg, null, 2)}`);
      }
      if (seg && seg.discontinuity) {
        incrementDiscSeqCount = true;
        if (_segments[variantKeys[i]].length > 0) {
          seg = _segments[variantKeys[i]].shift();
          if (i === 0) {
            debug(`[${this.sessionId}]: ${_name}: (${variantKeys[i]}) Ejected from playlist->: ${JSON.stringify(seg, null, 2)}`);
          }
        }
      }
      if (seg && seg.duration) {
        timeToRemove = seg.duration;
      }
    }
    return { timeToRemove: timeToRemove, incrementDiscSeqCount: incrementDiscSeqCount, segments: _segments };
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
    const vodAudiotrack = Object.keys(this.vodSegmentsAudio);
    const liveAudiotrack = Object.keys(this.liveSegQueueAudio);
    let vodTotalDur = 0;
    let liveTotalDur = 0;
    let totalDur = 0;
    let removedSegments = 0;
    let removedDiscontinuities = 0;

    // Calculate Playlist Total Duration
    this.vodSegmentsAudio[vodAudiotrack[0]].forEach((seg) => {
      if (seg.duration) {
        vodTotalDur += seg.duration;
      }
    });
    this.liveSegQueueAudio[liveAudiotrack[0]].forEach((seg) => {
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
      segments: this.vodSegmentsAudio,
      removedSegments: removedSegments,
      removedDiscontinuities: removedDiscontinuities,
      type: "AUDIO",
    });
    // Update V2L Segments
    this.vodSegmentsAudio = outputV2L.shiftedSegments;
    // Update values
    totalDur = outputV2L.totalDuration;
    removedSegments = outputV2L.removedSegments;
    removedDiscontinuities = outputV2L.removedDiscontinuities;
    // Shift LIVE Segments
    const outputLIVE = this._shiftSegments({
      name: instanceName,
      totalDur: totalDur,
      segments: this.liveSegQueueAudio,
      removedSegments: removedSegments,
      removedDiscontinuities: removedDiscontinuities,
      type: "AUDIO",
    });
    // Update LIVE Segments
    this.liveSegQueueAudio = outputLIVE.shiftedSegments;
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

  async _loadAudioManifest(audiotrack) {
    try {
      if (!this.sessionLiveState) {
        throw new Error("SessionLive not ready");
      }
      const liveTargetAudiotrack = this._findNearestAudiotrack(audiotrack, Object.keys(this.audioManifestURIs));
      debug(`[${this.sessionId}]: Requesting audiotrack (${audiotrack}), Nearest match is: ${JSON.stringify(liveTargetAudiotrack)}`);
      // Get the target media manifest
      const audioManifestUri = this.audioManifestURIs[liveTargetAudiotrack];
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
              audiotrack: liveTargetAudiotrack,
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
    } catch (err) {
      console.error(err);
      return Promise.reject(err);
    }
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
          this._addLiveSegmentsToQueue(startIdx, m3u.items.PlaylistItem, baseUrl, liveTargetBandwidth, isLeader, PlaylistTypes.VIDEO);
        }
        resolve();
      } catch (exc) {
        console.error("ERROR: " + exc);
        reject(exc);
      }
    });
  }

  _parseAudioManifest(m3u, audioPlaylistUri, liveTargetAudiotrack, isLeader) {
    return new Promise(async (resolve, reject) => {
      try {
        if (!this.liveSegQueueAudio[liveTargetAudiotrack]) {
          this.liveSegQueueAudio[liveTargetAudiotrack] = [];
        }
        if (!this.liveSegsForFollowersAudio[liveTargetAudiotrack]) {
          this.liveSegsForFollowersAudio[liveTargetAudiotrack] = [];
        }
        let baseUrl = "";
        const m = audioPlaylistUri.match(/^(.*)\/.*?$/);
        if (m) {
          baseUrl = m[1] + "/";
        }

        //debug(`[${this.sessionId}]: Current RAW Mseq:  [${m3u.get("mediaSequence")}]`);
        //debug(`[${this.sessionId}]: Previous RAW Mseq: [${this.lastRequestedAudioSeqRaw}]`);
        
        /* 
        WARN: We are assuming here that the MSEQ and Segment lengths are the same on Audio and Video
        and therefor need to push an equal amount of segments
        */
        if (this.pushAmount >= 0) {
          this.lastRequestedMediaSeqRaw = m3u.get("mediaSequence");
        }
        this.targetDuration = m3u.get("targetDuration");
        let startIdx = m3u.items.PlaylistItem.length - this.pushAmount;
        if (startIdx < 0) {
          this.restAmount = startIdx * -1;
          startIdx = 0;
        }
        if (audioPlaylistUri) {
          this._addLiveSegmentsToQueue(startIdx, m3u.items.PlaylistItem, baseUrl, liveTargetAudiotrack, isLeader, PlaylistTypes.AUDIO);
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
  _addLiveSegmentsToQueue(startIdx, playlistItems, baseUrl, liveTargetVariant, isLeader, plType) {
    try {
      const leaderOrFollower = isLeader ? "LEADER" : "NEW FOLLOWER";
      for (let i = startIdx; i < playlistItems.length; i++) {
        let seg = {};
        const playlistItem = playlistItems[i];
        let segmentUri;
        let byteRange = undefined;
        let initSegment = undefined;
        let initSegmentByteRange = undefined;
        let keys = undefined;
        let daterangeData = null;
        if (i === startIdx) {
          for (let j = startIdx; j >= 0; j--) {
            const pli = playlistItems[j];
            if (pli.get("map-uri")) {
              initSegmentByteRange = pli.get("map-byterange");
              if (pli.get("map-uri").match("^http")) {
                initSegment = pli.get("map-uri");
              } else {
                initSegment = urlResolve(baseUrl, pli.get("map-uri"));
              }
              break;
            }
          }
        }
        let attributes = playlistItem["attributes"].attributes;
        if (playlistItem.get("map-uri")) {
          initSegmentByteRange = playlistItem.get("map-byterange");
          if (playlistItem.get("map-uri").match("^http")) {
            initSegment = playlistItem.get("map-uri");
          } else {
            initSegment = urlResolve(baseUrl, playlistItem.get("map-uri"));
          }
        }
        // some items such as CUE-IN parse as a PlaylistItem
        // but have no URI
        if (playlistItem.get("uri")) {
          if (playlistItem.get("uri").match("^http")) {
            segmentUri = playlistItem.get("uri");
          } else {
            segmentUri = urlResolve(baseUrl, playlistItem.get("uri"));
          }
        }
        if (playlistItem.get("discontinuity")) {
          if (plType === PlaylistTypes.VIDEO) {
            this.liveSegQueue[liveTargetVariant].push({ discontinuity: true });
            this.liveSegsForFollowers[liveTargetVariant].push({ discontinuity: true });
          } else if (plType === PlaylistTypes.AUDIO) {
            this.liveSegQueueAudio[liveTargetVariant].push({ discontinuity: true });
            this.liveSegsForFollowersAudio[liveTargetVariant].push({ discontinuity: true });
          } else {
            console.warn(`[${this.sessionId}]: WARNING: plType=${plType} Not valid (disc-seg)`);
          }
        }
        if (playlistItem.get("byteRange")) {
          let [_, r, o] = playlistItem.get("byteRange").match(/^(\d+)@*(\d*)$/);
          if (!o) {
            o = byteRangeOffset;
          }
          byteRangeOffset = parseInt(r) + parseInt(o);
          byteRange = `${r}@${o}`;
        }
        if (playlistItem.get("keys")) {
          keys = playlistItem.get("keys");
        }
        let assetData = playlistItem.get("assetdata");
        let cueOut = playlistItem.get("cueout");
        let cueIn = playlistItem.get("cuein");
        let cueOutCont = playlistItem.get("cont-offset");
        let duration = 0;
        let scteData = playlistItem.get("sctedata");
        if (typeof cueOut !== "undefined") {
          duration = cueOut;
        } else if (typeof cueOutCont !== "undefined") {
          duration = playlistItem.get("cont-dur");
        }
        let cue =
          cueOut || cueIn || cueOutCont || assetData
            ? {
                out: typeof cueOut !== "undefined",
                cont: typeof cueOutCont !== "undefined" ? cueOutCont : null,
                scteData: typeof scteData !== "undefined" ? scteData : null,
                in: cueIn ? true : false,
                duration: duration,
                assetData: typeof assetData !== "undefined" ? assetData : null,
              }
            : null;
        seg = {
          duration: playlistItem.get("duration"),
          timelinePosition: this.timeOffset != null ? this.timeOffset + timelinePosition : null,
          cue: cue,
          byteRange: byteRange,
        };
        if (initSegment) {
          seg.initSegment = initSegment;
        }
        if (initSegmentByteRange) {
          seg.initSegmentByteRange = initSegmentByteRange;
        }
        if (segmentUri) {
          seg.uri = segmentUri;
        }
        if (keys) {
          seg.keys = keys;
        }
        if (i === startIdx) {
          // Add daterange metadata if this is the first segment
          if (this.rangeMetadata && !this._isEmpty(this.rangeMetadata)) {
            seg["daterange"] = this.rangeMetadata;
          }
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
        if (playlistItem.get("uri")) {
          if (daterangeData && !this._isEmpty(daterangeData)) {
            seg["daterange"] = daterangeData;
          }
          // Push new Live Segments! But do not push duplicates
          if (plType === PlaylistTypes.VIDEO) {
            this._pushToQueue(seg, liveTargetVariant, leaderOrFollower);
          } else if (plType === PlaylistTypes.AUDIO) {
            this._pushToQueueAudio(seg, liveTargetVariant, leaderOrFollower);
          } else {
            console.warn(`[${this.sessionId}]: WARNING: plType=${plType} Not valid (seg)`);
          }
        }
      }
    } catch (e) {
      console.error(e);
      return Promise.reject(e);
    }
  }

  _pushToQueue(seg, liveTargetBandwidth, logName) {
    const liveSegURIs = this.liveSegQueue[liveTargetBandwidth].filter((seg) => seg.uri).map((seg) => seg.uri);
    if (seg.uri && liveSegURIs.includes(seg.uri)) {
      debug(`[${this.sessionId}]: ${logName}: Found duplicate live segment. Skip push! (${liveTargetBandwidth})`);
    } else {
      this.liveSegQueue[liveTargetBandwidth].push(seg);
      this.liveSegsForFollowers[liveTargetBandwidth].push(seg);
      debug(`[${this.sessionId}]: ${logName}: Pushed Video segment (${seg.uri ? seg.uri : "Disc-tag"}) to 'liveSegQueue' (${liveTargetBandwidth})`);
    }
  }

  _pushToQueueAudio(seg, liveTargetAudiotrack, logName) {
    const liveSegURIs = this.liveSegQueueAudio[liveTargetAudiotrack].filter((seg) => seg.uri).map((seg) => seg.uri);
    if (seg.uri && liveSegURIs.includes(seg.uri)) {
      debug(`[${this.sessionId}]: ${logName}: Found duplicate live segment. Skip push! track -> (${liveTargetAudiotrack})`);
    } else {
      this.liveSegQueueAudio[liveTargetAudiotrack].push(seg);
      this.liveSegsForFollowersAudio[liveTargetAudiotrack].push(seg);
      debug(
        `[${this.sessionId}]: ${logName}: Pushed Audio segment (${
          seg.uri ? seg.uri : "Disc-tag"
        }) to 'liveSegQueue' track -> (${liveTargetAudiotrack})`
      );
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
    if (
      Object.keys(this.liveSegQueue).length === 0 ||
      (this.liveSegQueue[liveTargetBandwidth] && this.liveSegQueue[liveTargetBandwidth].length === 0)
    ) {
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
      m3u8 = this._setVariantManifestTags(this.vodSegments, m3u8, vodTargetBandwidth);
      // Add live-source segments
      m3u8 = this._setVariantManifestTags(this.liveSegQueue, m3u8, liveTargetBandwidth);
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
    const liveTargetTrack = this._findNearestAudiotrack(
      this._getTrackFromGroupAndLang(audioGroupId, audioLanguage),
      Object.keys(this.audioManifestURIs)
    );
    const vodTargetTrack = this._findNearestAudiotrack(
      this._getTrackFromGroupAndLang(audioGroupId, audioLanguage),
      Object.keys(this.vodSegmentsAudio)
    );
    debug(
      `[${this.sessionId}]: Client requesting manifest for VodTrackInfo=(${JSON.stringify(vodTargetTrack)}). Nearest LiveTrackInfo=(${JSON.stringify(
        liveTargetTrack
      )})`
    );

    if (this.blockGenerateManifest) {
      debug(`[${this.sessionId}]: FOLLOWER: Cannot Generate Audio Manifest! Waiting to sync-up with Leader...`);
      return null;
    }

    //  DO NOT GENERATE MANIFEST CASE: Node has not found anything in store OR Node has not even check yet.
    if (Object.keys(this.liveSegQueueAudio).length === 0 || this.liveSegQueueAudio[liveTargetTrack].length === 0) {
      debug(`[${this.sessionId}]: Cannot Generate Audio Manifest! <${this.instanceId}> Not yet collected ANY segments from Live Source...`);
      return null;
    }

    //  DO NOT GENERATE MANIFEST CASE: Node is in the middle of gathering segs of all variants.
    const tracks = Object.keys(this.liveSegQueueAudio);
    let segAmounts = [];
    for (let i = 0; i < tracks.length; i++) {
      const track = tracks[i];
      if (this.liveSegQueueAudio[track].length !== 0) {
        segAmounts.push(this.liveSegQueueAudio[track].length);
      }
    }

    if (!segAmounts.every((val, i, arr) => val === arr[0])) {
      console(`[${this.sessionId}]: Cannot Generate audio Manifest! <${this.instanceId}> Not yet collected ALL segments from Live Source...`);
      return null;
    }

    if (!this._isEmpty(this.liveSegQueueAudio) && this.liveSegQueueAudio[tracks[0]].length !== 0) {
      this.targetDuration = this._getMaxDuration(this.liveSegQueueAudio[tracks[0]]);
    }

    // Determine if VOD segments influence targetDuration
    for (let i = 0; i < this.vodSegmentsAudio[vodTargetTrack].length; i++) {
      let vodSeg = this.vodSegmentsAudio[vodTargetTrack][i];
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
    if (Object.keys(this.vodSegmentsAudio).length !== 0) {
      // Add transitional segments if there are any left.
      debug(`[${this.sessionId}]: Adding a Total of (${this.vodSegmentsAudio[vodTargetTrack].length}) VOD audio segments to manifest`);
      m3u8 = this._setVariantManifestTags(this.vodSegmentsAudio, m3u8, vodTargetTrack);
      // Add live-source segments
      m3u8 = this._setVariantManifestTags(this.liveSegQueueAudio, m3u8, liveTargetTrack);
    }
    debug(`[${this.sessionId}]: Audio manifest Generation Complete!`);
    return m3u8;
  }
  _setVariantManifestTags(segments, m3u8, variantKey) {
    let previousSeg = null;
    const size = segments[variantKey].length;
    for (let i = 0; i < size; i++) {
      const seg = segments[variantKey][i];
      const nextSeg = segments[variantKey][i + 1];
      if (seg.discontinuity && nextSeg && nextSeg.discontinuity) {
        nextSeg.discontinuity = false;
      }
      if (seg.discontinuity && !seg.cue) {
        // Avoid printing duplicate disc-tags
        if (!m3u8.endsWith("#EXT-X-DISCONTINUITY\n")) {
          if (!nextSeg || !nextSeg.discontinuity) {
            m3u8 += "#EXT-X-DISCONTINUITY\n";
          }
        }
      }
      m3u8 += segToM3u8(seg, i, size, nextSeg, previousSeg);
      previousSeg = seg;
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
  _findAudioGroupsForLang(audioLanguage, segments) {
    let trackInfos = [];
    const groupIds = Object.keys(segments);
    for (let i = 0; i < groupIds.length; i++) {
      const groupId = groupIds[i];
      const langs = Object.keys(segments[groupId]);
      for (let j = 0; j < langs.length; j++) {
        const lang = langs[j];
        if (lang === audioLanguage) {
          trackInfos.push({ audioGroupId: groupId, audioLanguage: lang });
          break;
        }
      }
    }
    return trackInfos;
  }

  _findNearestAudiotrack(track, tracks) {
    // perfect match
    if (tracks.includes(track)) {
      return track;
    }
    let tracksMatchingOnLanguage = tracks.filter((t) => {
      if (this._getLangFromTrack(t) === track) {
        return true;
      }
      return false;
    });
    // If any matches, then it implies that no group ID matches, so use a fallback (first) group
    if (tracksMatchingOnLanguage.length > 0) {
      return tracksMatchingOnLanguage[0];
    }
    // If no matches then check if we have any matched on group id, then use fallback (first) language
    let tracksMatchingOnGroupId = tracks.filter((t) => {
      if (this._getLangFromTrack(t) === track) {
        return true;
      }
      return false;
    });
    if (tracksMatchingOnGroupId.length > 0) {
      return tracksMatchingOnGroupId[0];
    }
    // No groupId or language matches the target, use fallback (first) track
    return tracks[0];
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
  _filterLiveProfilesAudio() {
    const tracks = this.sessionAudioTracks.map((trackItem) => {
      return this._getTrackFromGroupAndLang(trackItem.groupId, trackItem.language);
    });
    const toKeep = new Set();
    let newItem = {};
    tracks.forEach((t) => {
      let atToKeep = this._findNearestAudiotrack(t, Object.keys(this.audioManifestURIs));
      toKeep.add(atToKeep);
    });
    toKeep.forEach((at) => {
      newItem[at] = this.audioManifestURIs[at];
    });
    this.audioManifestURIs = newItem;
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
      if (trackInfo) {
        if (!newItemsAudio[trackInfo.audioGroupId]) {
          newItemsAudio[trackInfo.audioGroupId] = {};
        }
        newItemsAudio[trackInfo.audioGroupId][trackInfo.audioLanguage] = this.audioManifestURIs[trackInfo.audioGroupId][trackInfo.audioLanguage];
      }
    });
    if (!this._isEmpty(newItemsAudio)) {
      this.audioManifestURIs = newItemsAudio;
    }
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

  _getGroupAndLangFromTrack(track) {
    const GLItem = {
      groupId: null,
      language: null,
    };
    const match = track.match(/g:(.*?),l:(.*)/);
    if (match) {
      const g = match[1];
      const l = match[2];
      if (g && l) {
        GLItem.groupId = g;
        GLItem.language = l;
        return GLItem;
      }
    }
    console.error(`Failed to extract GroupID and Language g=${g};l=${l}`);
    return GLItem;
  }

  _getLangFromTrack(track) {
    const match = track.match(/g:(.*?),l:(.*)/);
    if (match) {
      const g = match[1];
      const l = match[2];
      if (g && l) {
        return l;
      }
    }
    console.error(`Failed to extract Language g=${g};l=${l}`);
    return null;
  }

  _getGroupFromTrack(track) {
    const match = track.match(/g:(.*?),l:(.*)/);
    if (match) {
      const g = match[1];
      const l = match[2];
      if (g && l) {
        return g;
      }
    }
    console.error(`Failed to extract Group ID g=${g};l=${l}`);
    return null;
  }

  _getTrackFromGroupAndLang(g, l) {
    return `g:${g},l:${l}`;
  }

  _isBandwidth(bw) {
    if (typeof bw === "number") {
      return true;
    } else if (typeof bw === "string") {
      const parsedNumber = parseFloat(bw);
      if (!isNaN(parsedNumber)) {
        return true;
      }
    }
    return false;
  }
}

module.exports = SessionLive;
