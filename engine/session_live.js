const debug = require("debug")("engine-sessionLive");
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
const RESET_DELAY = 5000;
const FAIL_TIMEOUT = 4000;
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
    this.discSeqCount = 0;
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.vodSegments = {};
    this.mediaManifestURIs = {};
    this.liveSegQueue = {};
    this.lastRequestedMediaSeqRaw = null;
    this.targetNumSeg = 0;
    this.liveSourceM3Us = {};
    this.playheadState = PlayheadState.IDLE;
    this.liveSegsForFollowers = {};
    this.timerCompensation = null;
    this.firstTime = true;
    this.allowedToSet = false;
    this.pushAmount = 0;
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
    this.discSeqCount = 0;
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.vodSegments = {};
    this.mediaManifestURIs = {};
    this.liveSegQueue = {};
    this.lastRequestedMediaSeqRaw = null;
    this.targetNumSeg = 0;
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

        // Let the playhead move at an interval set according to live segment duration
        let liveSegmentDurationMs = 6000;
        let liveBws = Object.keys(this.liveSegQueue);
        if (liveBws.length !== 0 && this.liveSegQueue[liveBws[0]].length !== 0 && this.liveSegQueue[liveBws[0]][0].duration) {
          liveSegmentDurationMs = this.liveSegQueue[liveBws[0]][0].duration * 1000;
        }

        // Fetch Live-Source Segments, and get ready for on-the-fly manifest generation
        // And also compensate for processing time

        this.waitForPlayhead = true;
        const tsIncrementBegin = Date.now();
        await this._loadAllMediaManifests();
        const tsIncrementEnd = Date.now();
        this.waitForPlayhead = false;

        // Set the timer
        let timerValueMs = 0;
        if (this.timerCompensation) {
          if (liveSegmentDurationMs - (tsIncrementEnd - tsIncrementBegin) > 0) {
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
        debug(`[${this.instanceId}][${this.sessionId}]: Going to fetch Live Master Manifest!`)
        // Load & Parse all Media Manifest URIs from Master
        await this._loadMasterManifest(masterManifestUri);
        this.masterManifestUri = masterManifestUri;
        if (this.sessionLiveProfile) {
          this._filterLiveProfiles();
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
    let segCount = 0;
    const allBws = Object.keys(segments);
    for (let i = 0; i < allBws.length; i++) {
      const bw = allBws[i];
      if (!this.vodSegments[bw]) {
        this.vodSegments[bw] = [];
      }

      if (segments[bw][0].discontinuity) {
        segments[bw].shift();
      }

      for (let segIdx = 0; segIdx < segments[bw].length; segIdx++) {
        this.vodSegments[bw].push(segments[bw][segIdx]);
      }
      if (!segments[bw][segments[bw].length - 1].discontinuity) {
        this.vodSegments[bw].push({ discontinuity: true });
      }
    }

    for (let segIdx = 0; segIdx < segments[allBws[0]].length; segIdx++) {
      if (segments[allBws[0]][segIdx].uri) {
        segCount++;
      }
    }
    this.targetNumSeg = segCount;
    debug(`[${this.sessionId}]: Setting CurrentMediaSequenceSegments. First seg is: [${this.vodSegments[allBws[0]][0].uri}]`);

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
    if (isLeader) {
      liveCounts.discSeqCount = this.discSeqCount;
      await this.sessionLiveState.set("firstCounts", liveCounts);
    } else {
      const leadersMediaSeqCount = liveCounts.mediaSeqCount;
      const leadersDiscSeqCount = liveCounts.discSeqCount;
      if (leadersMediaSeqCount !== null) {
        this.mediaSeqCount = leadersMediaSeqCount - 1;
        debug(`[${this.sessionId}]: Setting mediaSeqCount to: [${this.mediaSeqCount}]`);
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
      const vodTargetBandwidth = this._findNearestBw(bw, Object.keys(this.vodSegments));

      // Remove segments and disc-tag if they are on top
      if (this.vodSegments[vodTargetBandwidth].length > 0 && this.vodSegments[vodTargetBandwidth][0].discontinuity) {
        this.vodSegments[vodTargetBandwidth].shift();
        increment = 1;
      }

      segmentCount = this.vodSegments[vodTargetBandwidth].length;
      currentMediaSequenceSegments[liveTargetBandwidth] = [];
      // In case we switch back before we've depleted all transitional segments
      currentMediaSequenceSegments[liveTargetBandwidth] = this.vodSegments[vodTargetBandwidth].concat(this.liveSegQueue[liveTargetBandwidth]);
      currentMediaSequenceSegments[liveTargetBandwidth].push({ discontinuity: true });
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
      debug(`[${this.sessionId}]: Error No Segments found at all.`);
    }
    const vodBws = Object.keys(this.vodSegments);
    const liveBws = Object.keys(this.liveSegsForFollowers);
    const size = this.liveSegsForFollowers[liveBws[0]].length;
    debug(`[${this.sessionId}]: [size=${size}]->this.liveSegsForFollowers=${Object.keys(this.liveSegsForFollowers)} `);
    // Remove transitional segs & add live source segs collected from store
    for (let k = 0; k < size; k++) {
      let incrementDiscSeqCount = false;
      // Shift the top vod segment on all variants
      for (let i = 0; i < vodBws.length; i++) {
        let seg = this.vodSegments[vodBws[i]].shift();
        if (seg && seg.discontinuity || seg && seg.cue) {
          if (seg.discontinuity) {
            incrementDiscSeqCount = true;
          }
          seg = this.vodSegments[vodBws[i]].shift();
        }
        if (seg && seg.discontinuity || seg && seg.cue) {
          if (seg.discontinuity) {
            incrementDiscSeqCount = true;
          }
          this.vodSegments[vodBws[i]].shift();
        }
      }
      if (incrementDiscSeqCount) {
        this.mediaSeqCount++;
      }
      // Push to bottom, new live source segment on all variants
      for (let i = 0; i < liveBws.length; i++) {
        const bw = liveBws[i];
        const liveSegFromLeader = this.liveSegsForFollowers[bw][k];
        if (!this.liveSegQueue[bw]) {
          this.liveSegQueue[bw] = [];
        }
        this.liveSegQueue[bw].push(liveSegFromLeader);

        let segCount = 0;
        this.liveSegQueue[bw].map((seg) => {if (seg.uri) { segCount++ }});
        if (segCount > this.targetNumSeg) {
          seg = this.liveSegQueue[bw].shift();
          if (seg && seg.discontinuity || seg && seg.cue) {
            if (seg.discontinuity) {
              incrementDiscSeqCount = true;
            }
            seg = this.liveSegQueue[bw].shift();
          }
          if (seg && seg.discontinuity || seg && seg.cue) {
            if (seg.discontinuity) {
              incrementDiscSeqCount = true;
            }
            this.liveSegQueue[bw].shift();
          }
        }
        debug(`[${this.sessionId}]: Pushed a segment to 'liveSegQueue'`);
      }
      if (incrementDiscSeqCount) {
        this.discSeqCount++;
      }
      this.mediaSeqCount++;
    }
    debug(`[${this.sessionId}]: Finished updating all Follower's Counts and Segment Queues!`);
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

    // -------------------------------------
    //  If I am a Follower-node then my job
    //  ends here, where I only read from store.
    // -------------------------------------
    let isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader && this.lastRequestedMediaSeqRaw) {
      debug(`[${this.sessionId}]: FOLLOWER: Reading data from store!`);

      let leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");

      if (!leadersMediaSeqRaw < this.lastRequestedMediaSeqRaw && this.blockGenerateManifest) {
        this.blockGenerateManifest = false;
      }
      let attempts = 5;

      //  CHECK AGAIN CASE 1: Store Empty
      while (!leadersMediaSeqRaw && attempts > 0) {
        debug(`[${this.sessionId}]: FOLLOWER: Leader has not put anything in store... Will check again in 2000ms (Tries left=[${attempts}])`);
        await timer(2000);
        this.timerCompensation = false;
        leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
        attempts--;
      }

      if (!leadersMediaSeqRaw) {
        isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
        if (isLeader) {
          debug(`[${this.instanceId}]: I'm the new leader`);
          return;
        } else {
          debug(`[${this.instanceId}]: The leader is still alive`);
          return;
        }
      }

      attempts = 5;
      //  CHECK AGAIN CASE 2: Store Old
      while (leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw && attempts > 0) {
        debug(`[${this.sessionId}]: FOLLOWER: Cannot find anything NEW in store... Will check again in 2000ms (Tries left=[${attempts}])`);
        await timer(2000);
        this.timerCompensation = false;
        leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
        attempts--;
      }

      if (leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw) {
        isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
        if (isLeader) {
          debug(`[${this.instanceId}][${this.sessionId}]: I'm the new leader`);
          return;
        } else {
          debug(`[${this.instanceId}][${this.sessionId}]: The leader is still alive`);
          return;
        }
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
        // Collect Live Source Requesting Promises
        for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
          let bw = Object.keys(this.mediaManifestURIs)[i];
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
      if (manifestList.some(result => result.status === "rejected")) {
        debug(`[${this.sessionId}]: ALERT! Promises I: Failed, Rejection Found! Trying again...`);
        continue;
      }

      const allMediaSeqCounts = manifestList.map((item) => {
        if (item.status === "rejected") {
          return item.reason.mediaSeq;
        }
        return item.value.mediaSeq;
      });


      // Handle if mediaSeqCounts are NOT synced up!
      if (!allMediaSeqCounts.every((val, i, arr) => val === arr[0])) {

        debug(`[${this.sessionId}]: Live Mseq counts=[${allMediaSeqCounts}]`);
        // Decrement fetch counter
        FETCH_ATTEMPTS--;
        // Wait a little before trying again
        debug(`[${this.sessionId}]: ALERT! Live Source Data NOT in sync! Will try again after 1500ms`);
        await timer(1500);
        this.timerCompensation = false;
        continue;
      }

      if (!isLeader) {
        let leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
        let tries = 20;

        while (!leadersFirstSeqCounts.liveSourceMseqCount && tries > 0 || (leadersFirstSeqCounts.liveSourceMseqCount === 0)) {
          debug(`[${this.sessionId}]: NEW FOLLOWER: Waiting for LEADER to add 'firstCounts' in store! Will look again after 1000ms (tries left=${tries})`);
          await timer(1000);
          leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
          tries--;
        }

        if (tries === 0) {
          isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          if (isLeader) {
            debug(`[${this.instanceId}]: I'm the new leader, and now I am going to add 'firstCounts' in store`);
            break;
          } else {
            debug(`[${this.instanceId}]: The leader is still alive`);
            leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
            if (!leadersFirstSeqCounts.liveSourceMseqCount) {
              debug(`[${this.instanceId}]: Could not find 'firstCounts' in store. Abort Executing Promises II & Returning to Playhead.`);
              return;
            }
          }
        }

        // Respawners never do this, only starter followers.
        // Edge Case: FOLLOWER transitioned from session with different segments from LEADER
        if (leadersFirstSeqCounts.mediaSeqCount - 1 !== this.mediaSeqCount) {
          this.mediaSeqCount = leadersFirstSeqCounts.mediaSeqCount - 1;
          const transitSegs = await this.sessionLiveState.get("transitSegs");
          if (!this._isEmpty(transitSegs)) {
            this.vodSegments = transitSegs;
            let discTagCount = 0;
            const allBws = Object.keys(this.vodSegments);
            for (let segIdx = 0; segIdx < this.vodSegments[allBws[0]].length; segIdx++) {
              if (this.vodSegments[allBws[0]][segIdx].discontinuity) {
                discTagCount++;
              }
            }
            this.targetNumSeg = this.vodSegments[allBws[0]].length - (discTagCount - 1);
          }
        }

        // Prepare to load segments...
        debug(`[${this.instanceId}]: Newest mseq from LIVE=${allMediaSeqCounts[0]} First mseq in store=${leadersFirstSeqCounts.liveSourceMseqCount}`);
        if (allMediaSeqCounts[0] === leadersFirstSeqCounts.liveSourceMseqCount) {
          this.pushAmount = 1; // Follower from start
        } else {
          // RESPAWNED NODES
          this.pushAmount = (allMediaSeqCounts[0] - leadersFirstSeqCounts.liveSourceMseqCount) + 1;

          const transitSegs = await this.sessionLiveState.get("transitSegs");
          //debug(`[${this.sessionId}]: NEW FOLLOWER: I tried to get 'transitSegs'. This is what I found ${JSON.stringify(transitSegs)}`);
          if (!this._isEmpty(transitSegs)) {
            this.vodSegments = transitSegs;
            let discTagCount = 0;
            const allBws = Object.keys(this.vodSegments);
            for (let segIdx = 0; segIdx < this.vodSegments[allBws[0]].length; segIdx++) {
              if (this.vodSegments[allBws[0]][segIdx].discontinuity) {
                discTagCount++;
              }
            }
            this.targetNumSeg = this.vodSegments[allBws[0]].length - (discTagCount - 1);
          }
        }
        debug(`[${this.sessionId}]: ...pushAmount=${this.pushAmount}`);
      } else {
        // LEADER calculates pushAmount differently...
        if (this.firstTime) {
          this.pushAmount = 1; // Leader from start
        } else {
          this.pushAmount = allMediaSeqCounts[0] - this.lastRequestedMediaSeqRaw;
          debug(`[${this.sessionId}]: ...calculating pushAmount=${allMediaSeqCounts[0]}-${this.lastRequestedMediaSeqRaw}=${this.pushAmount}`);
        }
        debug(`[${this.sessionId}]: ...pushAmount=${this.pushAmount}`);
        break;
      }
      // Live Source Data is in sync, and LEADER & new FOLLOWER are in sync
      break;
    }

    if (FETCH_ATTEMPTS === 0) {
      debug(`[${this.sessionId}]: Fetching from Live-Source did not work! Returning to Playhead Loop...`)
      return;
    }

    isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    // NEW FOLLOWER - Edge Case: One Instance is ahead of another. Read latest live segs from store
    if (!isLeader) {
      const leadersCurrentMseqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
      const counts = await this.sessionLiveState.get("firstCounts");
      const leadersFirstMseqRaw = counts.liveSourceMseqCount;
      if (leadersCurrentMseqRaw && leadersCurrentMseqRaw !== this.lastRequestedMediaSeqRaw) {
        // if leader never had any segs from prev mseq
        if (leadersFirstMseqRaw && leadersFirstMseqRaw === leadersCurrentMseqRaw) {
          // Follower updates it's manifest ingedients (segment holders & counts)
          this.lastRequestedMediaSeqRaw = leadersCurrentMseqRaw;
          this.liveSegsForFollowers = await this.sessionLiveState.get("liveSegsForFollowers");
          debug(`[${this.sessionId}]: NEW FOLLOWER: Leader is ahead or behind me! Clearing Queue and Getting latest segments from store.`);
          this._updateLiveSegQueue();
          this.firstTime = false;
          debug(`[${this.sessionId}]: Got all needed segments from live-source (read from store).\nWe are now able to build Live Manifest: [${this.mediaSeqCount}]`);
          return;
        }
        else if (leadersCurrentMseqRaw < this.lastRequestedMediaSeqRaw) {
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
        pushPromises.push(this._parseMediaManifest(this.liveSourceM3Us[bw].M3U, bw, this.mediaManifestURIs[bw], bw));
        //debug(`[${this.sessionId}]: Pushed pushPromise for bw=${bw}`);
      }

      // Segment Pushing
      debug(`[${this.sessionId}]: Executing Promises II: Segment Pushing`);
      await Promise.all(pushPromises);
      // UPDATE COUNTS, & Shift Segments in vodSegments
      await this._incrementAndShift();
    }

    // -----------------------------------------------------
    // Leader writes to store so that Followers can read.
    // -----------------------------------------------------
    if (isLeader) {
      debug(`[${this.sessionId}]: LEADER: Adding data to store!`);
      if (this.allowedToSet) {
        await this.sessionLiveState.set("liveSegsForFollowers", this.liveSegsForFollowers);
        await this.sessionLiveState.set("lastRequestedMediaSeqRaw", this.lastRequestedMediaSeqRaw);
      }

      // [LASTLY]: LEADER does this for respawned-FOLLOWERS' sake.
      if (this.firstTime) {
        // Buy some time for followers (NOT Respawned) to fetch their own L.S m3u8.
        await timer(1000); // maybe remove
        const firstCounts = {
          liveSourceMseqCount: this.lastRequestedMediaSeqRaw,
          mediaSeqCount: this.mediaSeqCount,
          discSeqCount: this.discSeqCount,
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

  async _incrementAndShift() {
    const vodBandwidths = Object.keys(this.vodSegments);
    for (let j = 0; j < this.pushAmount; j++) {
      let incrementDiscSeqCount = false;
      // Shift the top vod segment
      for (let i = 0; i < vodBandwidths.length; i++) {
        let seg = this.vodSegments[vodBandwidths[i]].shift();
        if (seg && seg.discontinuity || seg && seg.cue) {
          if (seg.discontinuity) {
            incrementDiscSeqCount = true;
          }
          seg = this.vodSegments[vodBandwidths[i]].shift();
        }
        if (seg && seg.discontinuity || seg && seg.cue) {
          if (seg.discontinuity) {
            incrementDiscSeqCount = true;
          }
          this.vodSegments[vodBandwidths[i]].shift();
        }
      }
      if (incrementDiscSeqCount) {
        this.discSeqCount++;
      }
      this.mediaSeqCount++;
    }

    // TODO: LASTLY: SHRINK IF NEEDED. (should only happen once if ever.)
    // let extraSegments = (this.liveSegQueue[liveTargetBandwidth].length + this.vodSegments[vodBandwidths[0]].length) - this.targetNumSeg;
    // if(extraSegments > 0) {
    //   // POP top vod seg, 'extraSegments' times for all variants
    //   for (let k = 0; k < extraSegments; k++) {
    //     // Shift the top vod segment
    //     for (let i = 0; i < vodBandwidths.length; i++) {
    //       this.vodSegments[vodBandwidths[i]].shift();
    //     }
    //     this.mediaSeqCount++;
    //   }
    //   debug(`[${this.sessionId}]: Success! We shrunk manifest segment amount by ${extraSegments}_units`);
    // }
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
          let manifestObj = this._miniparse(m3u, bw, mediaManifestUri, liveTargetBandwidth);
          resolve(manifestObj);
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

  _miniparse(m3u, bw, mediaManifestUri, liveTargetBandwidth) {
    if (m3u === null) {
      throw new Error("No m3u object provided");
    }
    return new Promise(async (resolve, reject) => {
      try {
        const resolveObj = {
          M3U: m3u,
          mediaSeq: m3u.get("mediaSequence"),
        };

        this.liveSourceM3Us[liveTargetBandwidth] = resolveObj;

        resolve(resolveObj);
      } catch (exc) {
        console.error("ERROR: " + exc);
        reject({
          message: exc,
          bandwidth: liveTargetBandwidth,
          m3u8: null,
          mediaSeq: -1,
        });
      }
    });
  }

  _parseMediaManifest(m3u, bw, mediaManifestUri, liveTargetBandwidth) {
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

        // TODO: UPDATE target for number of segments in a manifest, if appropriate.
        // if (m3u.items.PlaylistItem.length < this.targetNumSeg) {
        //   debug(`[${this.sessionId}]: WE PLAN TO LOWER SEGMENT AMOUNT IN MANIFEST FROM (${this.targetNumSeg}) TO (${m3u.items.PlaylistItem.length})`);
        //   this.targetNumSeg = m3u.items.PlaylistItem.length;
        // }

        this.lastRequestedMediaSeqRaw = m3u.get("mediaSequence");
        this.targetDuration = m3u.get("targetDuration");

        // Switch out relative URIs if they are used, with absolute URLs
        if (mediaManifestUri) {
          // CREATE NEW MANIFEST
          let startIdx;
          startIdx = m3u.items.PlaylistItem.length - this.pushAmount;
          if (startIdx < 0) {
            startIdx = 0;
          }
          // push segments
          this._addLiveSegmentsToQueue(startIdx, m3u.items.PlaylistItem, baseUrl, liveTargetBandwidth);
        }

        resolve(this.lastRequestedMediaSeqRaw);
      } catch (exc) {
        console.error("ERROR: " + exc);
        reject(exc);
      }
    });
  }

  _addLiveSegmentsToQueue(startIdx, playlistItems, baseUrl, liveTargetBandwidth) {
    let incrementDiscSeqCount = false;
    for (let i = startIdx; i < playlistItems.length; i++) {
      debug(`[${this.sessionId}]: Adding Live Segment(s) to Queue (for bw=${liveTargetBandwidth})`);
      let seg = {};
      let playlistItem = playlistItems[i];
      let segmentUri;
      let attributes = playlistItem["attributes"].attributes;

      if (playlistItem.properties.discontinuity) {
        this.liveSegQueue[liveTargetBandwidth].push({ discontinuity: true });
        this.liveSegsForFollowers[liveTargetBandwidth].push({ discontinuity: true });
      }
      if ("cuein" in attributes) {
        this.liveSegQueue[liveTargetBandwidth].push({ cue: { in: true } });
        this.liveSegsForFollowers[liveTargetBandwidth].push({ cue: { in: true } });
      }
      if ("cueout" in attributes) {
        this.liveSegQueue[liveTargetBandwidth].push({ cue: { out: true, duration: attributes["cueout"] }, });
        this.liveSegsForFollowers[liveTargetBandwidth].push({ cue: { out: true, duration: attributes["cueout"] } });
      }
      if ("cuecont" in attributes) {
        this.liveSegQueue[liveTargetBandwidth].push({ cue: { cont: true } });
        this.liveSegsForFollowers[liveTargetBandwidth].push({ cue: { cont: true } });
      }
      if ("scteData" in attributes) {
        this.liveSegQueue[liveTargetBandwidth].push({ cue: { scteData: attributes["scteData"] } });
        this.liveSegsForFollowers[liveTargetBandwidth].push({ cue: { scteData: attributes["scteData"] } });
      }
      if ("scteData" in attributes) {
        this.liveSegQueue[liveTargetBandwidth].push({ cue: { assetData: attributes["assetData"] } });
        this.liveSegsForFollowers[liveTargetBandwidth].push({ cue: { assetData: attributes["assetData"] } });
      }
      if ("daterange" in attributes) {
        this.liveSegQueue[liveTargetBandwidth].push({
          daterange: { 
            id: attributes["daterange"]["ID"],
            "start-date": attributes["daterange"]["START-DATE"],
            "planned-duration": parseFloat(attributes["daterange"]["PLANNED-DURATION"]),
          }
        });
        this.liveSegsForFollowers[liveTargetBandwidth].push({
          daterange: { 
            id: attributes["daterange"]["ID"],
            "start-date": attributes["daterange"]["START-DATE"],
            "planned-duration": parseFloat(attributes["daterange"]["PLANNED-DURATION"]),
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

        this.liveSegQueue[liveTargetBandwidth].push(seg);
        this.liveSegsForFollowers[liveTargetBandwidth].push(seg);
        let segCount = 0;
        this.liveSegQueue[liveTargetBandwidth].map((seg) => {if (seg.uri) { segCount++ }});
        debug(`[${this.sessionId}]: size of queue=${segCount}_targetNumseg=${this.targetNumSeg}`);
        if (segCount > this.targetNumSeg) {
          seg = this.liveSegQueue[liveTargetBandwidth].shift();
          if (seg && seg.discontinuity || seg && seg.cue) {
            if (seg.discontinuity) {
              incrementDiscSeqCount = true;
            }
            seg = this.liveSegQueue[liveTargetBandwidth].shift();
          }
          if (seg && seg.discontinuity || seg && seg.cue) {
            if (seg.discontinuity) {
              incrementDiscSeqCount = true;
            }
            this.liveSegQueue[liveTargetBandwidth].shift();
          }
        }
      }
    }
    if (incrementDiscSeqCount) {
      this.discSeqCount++;
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
    const vodTargetBandwidth = this._findNearestBw(bw, Object.keys(this.vodSegments));
    debug(`[${this.sessionId}]: Client requesting manifest for bw=(${bw}). Nearest LiveBw=(${liveTargetBandwidth})`)


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
        if (seg.cue.in){
          m3u8 += "#EXT-X-CUE-IN" + "\n";
        }
        if(seg.cue.out) {
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
          }
          else {
            m3u8 += "#EXT-X-CUE-OUT-CONT:" + seg.cue.cont + "/" + seg.cue.duration + "\n";
          }
        }
      }
      if (seg.daterange) {
        const dateRangeAttributes = Object.keys(seg.daterange).map(key => daterangeAttribute(key, seg.daterange[key])).join(',');
        if (seg.daterange['start-date']) {
          m3u8 += "#EXT-X-PROGRAM-DATE-TIME:" + seg.daterange['start-date'] + "\n";
        }
        m3u8 += "#EXT-X-DATERANGE:" + dateRangeAttributes + "\n";
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
    profiles.forEach(profile => {
      let bwToKeep = this._findNearestBw(profile.bw, Object.keys(this.mediaManifestURIs));
      toKeep.add(bwToKeep);
    });
    toKeep.forEach((bw) => {
      newItem[bw] = this.mediaManifestURIs[bw];
    })
    this.mediaManifestURIs = newItem;
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
