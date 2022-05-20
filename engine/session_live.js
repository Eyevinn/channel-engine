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

class SessionLive {
  constructor(config, sessionLiveStore) {
    this.sessionId = crypto.randomBytes(20).toString("hex");
    this.sessionLiveStateStore = sessionLiveStore.sessionLiveStateStore;
    this.instanceId = sessionLiveStore.instanceId;
    this.mediaSeqCount = 0;
    this.prevMediaSeqCount = 0;
    this.discSeqCount = 0;
    this.prevDiscSeqCount = 0;
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.vodSegments = {};
    this.mediaManifestURIs = {};
    this.liveSegQueue = {};
    this.lastRequestedMediaSeqRaw = null;
    this.liveSourceM3Us = {};
    this.playheadState = PlayheadState.IDLE;
    this.liveSegsForFollowers = {};
    this.timerCompensation = null;
    this.firstTime = true;
    this.allowedToSet = false;
    this.pushAmount = 0;
    this.restAmount = 0;
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

    await timer(resetDelay);
    await this.sessionLiveState.set("liveSegsForFollowers", null);
    await this.sessionLiveState.set("lastRequestedMediaSeqRaw", null);
    await this.sessionLiveState.set("transitSegs", null);
    await this.sessionLiveState.set("firstCounts", {
      liveSourceMseqCount: null,
      mediaSeqCount: null,
      discSeqCount: null,
    });
    debug(`[${this.instanceId}][${this.sessionId}]: LEADER: Resetting SessionLive values in Store ${resetDelay === 0 ? "Immediately" : `after a delay=(${resetDelay}ms)`}`);
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
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.vodSegments = {};
    this.mediaManifestURIs = {};
    this.liveSegQueue = {};
    this.lastRequestedMediaSeqRaw = null;
    this.liveSourceM3Us = {};
    this.liveSegsForFollowers = {};
    this.timerCompensation = null;
    this.firstTime = true;
    this.pushAmount = 0;
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
      } catch (err) {
        this.masterManifestUri = null;
        debug(`[${this.instanceId}][${this.sessionId}]: Failed to fetch Live Master Manifest! ${err}`);
        debug(`[${this.instanceId}][${this.sessionId}]: Will try again in 1000ms! (tries left=${attempts})`);
        await timer(1000);
      }
      // To make sure certain operations only occur once.
      this.firstTime = true;
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

  async setCurrentMediaAndDiscSequenceCount(mediaSeq, discSeq) {
    if (mediaSeq === null || discSeq === null) {
      debug(`[${this.sessionId}]: No media or disc sequence provided`);
      return false;
    }
    debug(`[${this.sessionId}]: Setting mediaSeqCount and discSeqCount to: [${mediaSeq}]:[${discSeq}]`);
    this.mediaSeqCount = mediaSeq;
    this.discSeqCount = discSeq;

    // IN CASE: New/Respawned Node Joins the Live Party
    // Don't use what Session gave you. Use the Leaders number if it's available
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    let liveCounts = await this.sessionLiveState.get("firstCounts");
    if (liveCounts === null) {
      liveCounts = {
        liveSourceMseqCount: null,
        mediaSeqCount: null,
        discSeqCount: null,
      };
    }
    if (isLeader) {
      liveCounts.discSeqCount = this.discSeqCount;
      await this.sessionLiveState.set("firstCounts", liveCounts);
    } else {
      const leadersMediaSeqCount = liveCounts.mediaSeqCount;
      const leadersDiscSeqCount = liveCounts.discSeqCount;
      if (leadersMediaSeqCount !== null) {
        this.mediaSeqCount = leadersMediaSeqCount;
        debug(`[${this.sessionId}]: Setting mediaSeqCount to: [${this.mediaSeqCount}]`);
        const transitSegs = await this.sessionLiveState.get("transitSegs");
        if (!this._isEmpty(transitSegs)) {
          debug(`[${this.sessionId}]: Getting and loading 'transitSegs'`);
          this.vodSegments = transitSegs;
        }
      }
      if (leadersDiscSeqCount !== null) {
        this.discSeqCount = leadersDiscSeqCount;
        debug(`[${this.sessionId}]: Setting discSeqCount to: [${this.discSeqCount}]`);
      }
    }
  }

  async getTransitionalSegments() {
    return this.vodSegments;
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

  async getCurrentMediaAndDiscSequenceCount() {
    return {
      mediaSeq: this.mediaSeqCount,
      discSeq: this.discSeqCount,
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

  // TODO: Implement this later
  async getCurrentAudioManifestAsync(audioGroupId, audioLanguage) {
    debug(`[${this.sessionId}]: getCurrentAudioManifestAsync is NOT Implemented`);
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
        }
        debug(`[${this.sessionId}]: All Live Media Manifest URIs have been collected. (${Object.keys(this.mediaManifestURIs).length}) profiles found!`);
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

      attempts = 10;
      //  CHECK AGAIN CASE 2: Store Old
      while (leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw && attempts > 0) {
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

        const segDur = this._getAnyFirstSegmentDurationMs() || DEFAULT_PLAYHEAD_INTERVAL_MS;
        const waitTimeMs = parseInt(segDur / 3, 10);
        debug(`[${this.sessionId}]: FOLLOWER: Cannot find anything NEW in store... Will check again in ${waitTimeMs}ms (Tries left=[${attempts}])`);
        await timer(waitTimeMs);
        this.timerCompensation = false;
        leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
        attempts--;
      }
      if(leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw) {
        debug(`[${this.instanceId}][${this.sessionId}]: The leader is still alive`);
        return;
      }
      // Follower updates its manifest building blocks (segment holders & counts)
      this.lastRequestedMediaSeqRaw = leadersMediaSeqRaw;
      this.liveSegsForFollowers = await this.sessionLiveState.get("liveSegsForFollowers");
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
        const higestMediaSeqCount = Math.max(...allStoredMediaSeqCounts);
        bandwidthsToSkipOnRetry = Object.keys(this.liveSourceM3Us).filter((bw) => {
          if (this.liveSourceM3Us[bw].mediaSeq === higestMediaSeqCount) {
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
        const firstCounts = {
          liveSourceMseqCount: this.lastRequestedMediaSeqRaw,
          mediaSeqCount: this.prevMediaSeqCount,
          discSeqCount: this.prevDiscSeqCount,
        };
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

  _shiftSegments(opt) {
    let _totalDur = 0;
    let _segments = {};
    let _name = "";
    let _removedSegments = 0;
    let _removedDiscontinuities = 0;

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
    const bws = Object.keys(_segments);

    /* When Total Duration is past the Limit, start Shifting V2L|LIVE segments if found */
    while (_totalDur > TARGET_PLAYLIST_DURATION_SEC) {
      // Skip loop if there are no more segments to remove...
      if (_segments[bws[0]].length === 0) {
        return { totalDuration: _totalDur, removedSegments: _removedSegments, removedDiscontinuities: _removedDiscontinuities, shiftedSegments: _segments };
      }
      debug(`[${this.sessionId}]: ${_name}: (${_totalDur})s/(${TARGET_PLAYLIST_DURATION_SEC})s - Playlist Duration is Over the Target. Shift needed!`);
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
      if (timeToRemove) {
        _totalDur -= timeToRemove;
        // Increment number of removed segments...
        _removedSegments++;
      }
      if (incrementDiscSeqCount) {
        // Update Session Live Discontinuity Sequence Count
        _removedDiscontinuities++;
      }
    }
    return { totalDuration: _totalDur, removedSegments: _removedSegments, removedDiscontinuities: _removedDiscontinuities, shiftedSegments: _segments };
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

  _setMediaManifestTags(segments, m3u8, bw) {
    for (let i = 0; i < segments[bw].length; i++) {
      const seg = segments[bw][i];
      if (seg.discontinuity) {
        m3u8 += "#EXT-X-DISCONTINUITY\n";
      }
      if (seg.cue) {
        if (seg.cue.out) {
          if (seg.cue.scteData) {
            m3u8 += "#EXT-OATCLS-SCTE35:" + seg.cue.scteData + "\n";
          }
          if (seg.cue.assetData) {
            m3u8 += "#EXT-X-ASSET:" + seg.cue.assetData + "\n";
          }
          m3u8 += "#EXT-X-CUE-OUT:DURATION=" + seg.cue.duration + "\n";
        }
        if (seg.cue.cont) {
          if (seg.cue.scteData) {
            m3u8 += "#EXT-X-CUE-OUT-CONT:ElapsedTime=" + seg.cue.cont + ",Duration=" + seg.cue.duration + ",SCTE35=" + seg.cue.scteData + "\n";
          } else {
            m3u8 += "#EXT-X-CUE-OUT-CONT:" + seg.cue.cont + "/" + seg.cue.duration + "\n";
          }
        }
      }
      if (seg.datetime) {
        m3u8 += `#EXT-X-PROGRAM-DATE-TIME:${seg.datetime}\n`;
      }
      if (seg.daterange) {
        const dateRangeAttributes = Object.keys(seg.daterange)
          .map((key) => daterangeAttribute(key, seg.daterange[key]))
          .join(",");
        if (!seg.datetime && seg.daterange["start-date"]) {
          m3u8 += "#EXT-X-PROGRAM-DATE-TIME:" + seg.daterange["start-date"] + "\n";
        }
        m3u8 += "#EXT-X-DATERANGE:" + dateRangeAttributes + "\n";
      }
      // Mimick logic used in hls-vodtolive
      if (seg.cue && seg.cue.in) {
        m3u8 += "#EXT-X-CUE-IN" + "\n";
      }
      if (seg.uri) {
        m3u8 += "#EXTINF:" + seg.duration.toFixed(3) + ",\n";
        m3u8 += seg.uri + "\n";
      }
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
}

module.exports = SessionLive;
