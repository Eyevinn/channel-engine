const debug = require("debug")("engine-sessionLive");
const allSettled = require("promise.allsettled");
const crypto = require("crypto");
const m3u8 = require("@eyevinn/m3u8");
const request = require("request");
const url = require("url");
const { m3u8Header } = require("./util.js");

const timer = (ms) => new Promise((res) => setTimeout(res, ms));
const DELAY_FACTOR = 0.5;
const RESET_DELAY = 5000;

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
    this.lastRequestedM3U8 = null;
    this.vodSegments = {};
    this.mediaManifestURIs = {};
    this.liveSegQueue = {};
    this.lastRequestedMediaSeqRaw = 0;
    this.targetNumSeg = 0;
    this.liveSourceM3Us = {};
    this.delayFactor = DELAY_FACTOR;
    this.playheadState = PlayheadState.IDLE;
    this.liveSegsForFollowers = {};
    this.timerCompensation = null;
    this.firstTime = true;
    this.pushAmount = 0;
    this.allowedToSet = true;

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
    }
  }

  async initAsync() {
    this.sessionLiveState = await this.sessionLiveStateStore.create(this.sessionId, this.instanceId);
  }

  async _resetLiveStoreAsync() {
    await timer(RESET_DELAY);
    await this.sessionLiveState.set("liveSegsForFollowers", null);
    await this.sessionLiveState.set("lastRequestedMediaSeqRaw", null);
    await this.sessionLiveState.set("firstCounts", {
      liveSourceMseqCount: null,
      mediaSeqCount: null,
      discSeqCount: null,
    });
  }

  async resetSessionAsync() {
    this.mediaSeqCount = 0;
    this.discSeqCount = 0;
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.lastRequestedM3U8 = null;
    this.vodSegments = {};
    this.mediaManifestURIs = {};
    this.liveSegQueue = {};
    this.lastRequestedMediaSeqRaw = 0;
    this.targetNumSeg = 0;
    this.liveSourceM3Us = {};
    this.playheadState = PlayheadState.IDLE;
    this.liveSegsForFollowers = {};
    this.timerCompensation = null;
    this.firstTime = true;
    this.pushAmount = 0;
    this.allowedToSet = true;

    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (isLeader) {
      this._resetLiveStoreAsync();
      debug(`[${this.instanceId}][${this.sessionId}]: I'm the Leader resetting local Live Session and store`);
    } else {
      debug(`[${this.instanceId}][${this.sessionId}]: Follower resetting local Live Session`);
    }
  }

  async startPlayheadAsync() {
    debug(`[${this.sessionId}]: SessionLive-Playhead consumer started:`);
    this.playheadState = PlayheadState.RUNNING;
    while (this.playheadState !== PlayheadState.CRASHED) {
      try {
        this.timerCompensation = true;
        // Nothing to do if we have no Live Source to probe
        if (!this.masterManifestUri) {
          debug(`[${this.sessionId}]: SessionLive-Playhead running, but has no content to work with. Will try again after 3000ms`);
          await timer(3000);
          continue;
        }

        // End the loop is state==STOPPED
        if (this.playheadState === PlayheadState.STOPPED) {
          debug(`[${this.sessionId}]: Playhead has Stopped.`);
          return;
        }

        // Let the playhead move at an interval set according to live segment duration
        let liveSegmentDurationMs = 6000;
        let liveBws = Object.keys(this.liveSegQueue);
        debug(`[${this.sessionId}]: +-+ liveBws=${liveBws}`);
        //debug(`[${this.sessionId}]: +-+ this.liveSegQueue[liveBws[0]]=${this.liveSegQueue[liveBws[0]]}`);
        if (liveBws.length !== 0 && this.liveSegQueue[liveBws[0]].length !== 0 && this.liveSegQueue[liveBws[0]][0].duration){
          liveSegmentDurationMs = this.liveSegQueue[liveBws[0]][0].duration * 1000;
        }

        // Fetch Live-Source Segments, and get ready for on-the-fly manifest generaion.
        // And also compensate for processing time.
        const tsIncrementBegin = Date.now();
        const manifest = await this._loadAllMediaManifests(); // could set 'timerCompensation'=false
        const tsIncrementEnd = Date.now();

        // Set the timer
        let timerValueMs = 0;
        if (this.timerCompensation) {
          if (liveSegmentDurationMs - (tsIncrementEnd - tsIncrementBegin) > 0) {
            timerValueMs = liveSegmentDurationMs - (tsIncrementEnd - tsIncrementBegin);
          }
        } else {
          // DO NOT compensate if manifest fetching was out-of-sync.
          // It means that Live Source and Channel-Engine were awkwardly time-synced.
          timerValueMs = liveSegmentDurationMs;
        }
        debug(`[${this.sessionId}]: SessionLive-Playhead going to ping again after ${timerValueMs}ms`);
        await timer(timerValueMs);

        this.firstTime = false;
      } catch (err) {
        debug(`[${this.sessionId}]: SessionLive-Playhead consumer crashed (1)`);
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

  async setLiveUri(liveUri) {
    // Load & Parse all Media Manifest uris from Master.
    await this._loadMasterManifest(liveUri);
    // This will let playhead call Live Source for manifests.
    this.masterManifestUri = liveUri;
  }

  async setCurrentMediaSequenceSegments(segments) {
    const allBws = Object.keys(segments);
    for (let i = 0; i < allBws.length; i++) {
      const bw = allBws[i];
      if (!this.vodSegments[bw]) {
        this.vodSegments[bw] = [];
      }
      if (segments[bw][0].discontinuity) {
        segments[bw].shift();
      }

      const segLen = segments[bw].length;
      for (let segIdx = segLen - segLen; segIdx < segLen; segIdx++) {
        this.vodSegments[bw].push(segments[bw][segIdx]);
      }
      if (!segments[bw][segLen - 1].discontinuity) {
        this.vodSegments[bw].push({ discontinuity: true });
      }
    }
    this.targetNumSeg = this.vodSegments[allBws[0]].length;
    debug(`[${this.sessionId}]: Setting CurrentMediaSequenceSegments. First seg is: ${this.vodSegments[allBws[0]][0].uri}`);
  }

  async setCurrentMediaAndDiscSequenceCount(mediaSeq, discSeq) {
    debug(`[${this.sessionId}]: Setting mediaSeqCount and discSeqCount to: [${mediaSeq}]:[${discSeq}]`);
    this.mediaSeqCount = mediaSeq;
    this.discSeqCount = discSeq;

    // IN CASE: New/Respawned Node Joins the Live Party.
    // Don't use what Session gave you. Use the Leaders number if it's available.
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader) {
      const liveCounts = await this.sessionLiveState.get("firstCounts");
      const leadersMediaSeqCount = liveCounts.mediaSeqCount;
      const leadersDiscSeqCount = liveCounts.discSeqCount;
      if (leadersMediaSeqCount) {
        this.mediaSeqCount = leadersMediaSeqCount;
        debug(`[${this.sessionId}]: Setting mediaSeqCount to: [${this.mediaSeqCount}]`);
      }
      if (leadersDiscSeqCount) {
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
     * Might be possible that Follower Sends segments to Session
     * BEFORE Leader finished fetching new segs and sending segs himself.
     * As long as Leader sends same segs to session as Follower even though Leader
     * is trying to get new segs, it should be fine!
     **/
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (isLeader) {
      this.allowedToSet = false;
    } else {
      let leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
      if (leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw) {
        // Follower updates its manifest ingedients (segment holders & counts)
        this.lastRequestedMediaSeqRaw = leadersMediaSeqRaw;
        this.liveSegsForFollowers = await this.sessionLiveState.get("liveSegsForFollowers");
        this._updateLiveSegQueue();
      }
    }

    let currentMediaSequenceSegments = {};

    for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
      let bw = Object.keys(this.mediaManifestURIs)[i];

      const liveTargetBandwidth = this._findNearestBw(bw, Object.keys(this.mediaManifestURIs));
      const vodTargetBandwidth = this._findNearestBw(bw, Object.keys(this.vodSegments));

      currentMediaSequenceSegments[liveTargetBandwidth] = [];
      // In case we switch back before we've depleted all transitional segments.
      currentMediaSequenceSegments[liveTargetBandwidth] = this.vodSegments[vodTargetBandwidth].concat(this.liveSegQueue[liveTargetBandwidth]);
      // # Book-end it with DISCONTINUITY-TAG
      currentMediaSequenceSegments[liveTargetBandwidth].push({
        discontinuity: true,
      });
      debug(`[${this.sessionId}]: Pushed loadMedia promise for bw=${bw}`);
    }
    return currentMediaSequenceSegments;
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
    debug(`[${this.sessionId}]: ...Loading the selected Live Media Manifest`);
    let manifest = null;
    while (!manifest) {
      manifest = await this._GenerateLiveManifest(bw);
      if (!manifest) {
        debug(`[${this.sessionId}]: No manifest available yet, will try again after 1000ms`);
        await timer(1000);
        continue;
      }
    }

    this.lastRequestedM3U8.m3u8 = manifest;

    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    // TODO: Remove this
    if (isLeader) {
      debug(`[${this.sessionId}]: LEADER-> Sending requested manifest to client [${JSON.stringify(this.lastRequestedM3U8.m3u8)}]\n\n`);
    } else {
      debug(`[${this.sessionId}]: FOLLOWER-> Sending requested manifest to client [${JSON.stringify(this.lastRequestedM3U8.m3u8)}]\n\n`);
    }
    return this.lastRequestedM3U8.m3u8;
  }

  // TODO: Implement this later
  async getCurrentAudioManifestAsync(audioGroupId, audioLanguage) {
    debug(`[${this.sessionId}]: getCurrentAudioManifestAsync is NOT Implemented`);
    return "Not Implemented";
  }

  /**
   *
   * @returns Loads the URIs to the different media playlists from the given master playlist
   */
  _loadMasterManifest(masterManifestURI) {
    return new Promise((resolve, reject) => {
      const parser = m3u8.createStream();
      try {
        request({ uri: masterManifestURI, gzip: true })
        .on("error", (exc) => {
          debug(`ERROR: ${Object.keys(exc)}`);
          reject(exc);
        })
        .pipe(parser);
      } catch (exc) {
        debug(`ERROR: ${Object.keys(exc)}`);
        reject(exc);
      }
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
      });
      parser.on("error", (exc) => {
        debug(`ERROR: ${Object.keys(exc)}`);
        reject(exc);
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
    debug(`[${this.sessionId}]: [__size=${size}__]->this.liveSegsForFollowers=${ Object.keys(this.liveSegsForFollowers)} `);
    // # -REMOVE- transitional segs & -ADD- live source segs collected from store
    for (let k = 0; k < size; k++) {
      // Increase Discontinuity count if top segment is a discontinuity segment
      if (this.vodSegments[vodBws[0]].length !== 0 && this.vodSegments[vodBws[0]][0].discontinuity) {
        this.discSeqCount++;
      }
      if (true) {
        //this.lastRequestedM3U8) {
        // Shift the top vod segment on all variants
        for (let i = 0; i < vodBws.length; i++) {
          this.vodSegments[vodBws[i]].shift();
        }
      }
      // Push to bottom, new live source segment on all variants
      for (let i = 0; i < liveBws.length; i++) {
        const bw = liveBws[i];
        const liveSegFromLeader = this.liveSegsForFollowers[bw][k];
        if (!this.liveSegQueue[bw]) {
          this.liveSegQueue[bw] = [];
        }
        this.liveSegQueue[bw].push(liveSegFromLeader);
        if (this.liveSegQueue[bw].length >= this.targetNumSeg) {
          this.liveSegQueue[bw].shift();
        }
        debug(`[${this.sessionId}]: I just pushed a segment to 'liveSegQueue')`);
      }
      this.mediaSeqCount++;
    }
    debug(`[${this.sessionId}]: Finished updating all Follower's Counts and Segment Queues!`);
  }

  // LEADER only function
  _updateLiveSegsForFollowers() {
    // No Guarentee that only last seg is new. Depending on the Live Source update rate and timing, leader might fetch manifest 2 steps ahead.
    const bandwidths = Object.keys(this.liveSegQueue);
    bandwidths.forEach((bw) => {
      if (!this.liveSegsForFollowers[bw]) {
        this.liveSegsForFollowers[bw] = [];
      }

      let startIdx = this.liveSegQueue[bandwidths[0]].length - this.pushAmount;
      if (startIdx < 0) {
        startIdx = 0;
      }
      // Most often we only push 1 segment. But there is an Edge Case where liveSegQueue has grown more than 1...
      for (let i = startIdx; i < this.liveSegQueue[bandwidths[0]].length; i++) {
        const newestSegment = this.liveSegQueue[bw][i];
        this.liveSegsForFollowers[bw].push(newestSegment);
      }
    });
    debug(`[${this.sessionId}]: LEADER: Transfered Newest Live Source segment to 'liveSegsForFollowers'...`);
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
    debug(`[${this.sessionId}]: ...Attempting to load all media manifest URIs in=${Object.keys(this.mediaManifestURIs)}`);
    // To make sure... we load all profiles!
    if (this.lastRequestedM3U8 && this.lastRequestedM3U8.bandwidth) {
      this.lastRequestedM3U8.bandwidth = null;
    }

    // -------------------------------------
    //  If I am a Follower-node then my job
    //  ends here, where I only read from store.
    // -------------------------------------
    let isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader && this.lastRequestedMediaSeqRaw !== 0) {
      debug(`[${this.sessionId}]: FOLLOWER: Reading data from store!`);

      let leadersMediaSeqRaw = await this.sessionLiveState.get(
        "lastRequestedMediaSeqRaw"
      );
      let attempts = 6;

      /**
       * (HEY!) these two while loops can be one if we init lastRequestedMediaSeqRaw=0 in store!
       */
      //  CHECK AGAIN CASE 1: Store Empty
      while (!leadersMediaSeqRaw && attempts > 0) {
        debug(`[${this.sessionId}]: FOLLOWER: Leader has not put anything in store...Will check again in 2000ms (Tries left=${attempts})`);
        await timer(2000);
        this.timerCompensation = false;
        leadersMediaSeqRaw = await this.sessionLiveState.get(
          "lastRequestedMediaSeqRaw"
        );
        attempts--;
      }

      if (!leadersMediaSeqRaw) {
        isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
        if (isLeader) {
          debug(`[${this.instanceId}]: I'm the new leader`);
          return;
        } else {
          debug(`[${this.instanceId}]: The leader is still alive`);
        }
      }

      attempts = 6;
      //  CHECK AGAIN CASE 2: Store Old
      while (leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw && attempts > 0) {
        debug(`[${this.sessionId}]: FOLLOWER: Cannot find anything NEW in store...Will check again in 2000ms (Tries left=${attempts})`);
        await timer(2000);
        this.timerCompensation = false;
        leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
        attempts--;
      }

      if (leadersMediaSeqRaw <= this.lastRequestedMediaSeqRaw) {
        isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
        if (isLeader) {
          debug(`[${this.instanceId}]: I'm the new leader`);
          return;
        } else {
          debug(`[${this.instanceId}]: The leader is still alive`);
        }
      }

      // Follower updates its manifest ingedients (segment holders & counts)
      this.lastRequestedMediaSeqRaw = leadersMediaSeqRaw;
      this.liveSegsForFollowers = await this.sessionLiveState.get("liveSegsForFollowers");
      debug(`[${this.sessionId}]: +-+ Look these are my segs from store->${JSON.stringify(this.liveSegsForFollowers)}`);
      this._updateLiveSegQueue();
      return;
    }

    // ---------------------------------
    // FETCHING FROM LIVE-SOURCE
    // ---------------------------------
    let FETCH_ATTEMPTS = 10;
    this.liveSegsForFollowers = {};

    while (FETCH_ATTEMPTS > 0) {
      if (isLeader) {
        debug(`[${this.sessionId}]: LEADER: Trying to fetch manifests for all bandwidths\n Attempts left=${FETCH_ATTEMPTS}`);
      } else {
        debug(`[${this.sessionId}]: NEW FOLLOWER: Trying to fetch manifests for all bandwidths\n Attempts left=${FETCH_ATTEMPTS}`);
      }

      // Reset Values Each Attempt
      let livePromises = [];
      this.pushAmount = 0;

      // Collect Live Source Requesting Promises
      for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
        let bw = Object.keys(this.mediaManifestURIs)[i];
        livePromises.push(this._loadMediaManifest(bw));
        debug(`[${this.sessionId}]: Pushed loadMedia promise for bw=${bw}`);
      }

      // Execute Promises I: Fetech From Live Source
      debug(`[${this.sessionId}]: Executing Promises I: Fetech From Live Source`);
      const manifestList = await allSettled(livePromises);
      livePromises = [];

      // Extract the media sequence count from promise results
      const allMediaSeqCounts = manifestList.map((item) => {
        if (item.status === "rejected") {
          return item.reason.mediaSeq;
        }
        return item.value.mediaSeq;
      });

      // Handle if mediaSeqCounts are NOT synced up!
      if (!allMediaSeqCounts.every((val, i, arr) => val === arr[0])) {
        //
        debug(`[${this.sessionId}]: Live Mseq counts=${allMediaSeqCounts}`);
        //  Decement fetch counter.
        --FETCH_ATTEMPTS;
        //  Wait a little before trying again.
        debug(`[${this.sessionId}]: XX [[ ALERT! ]] | Live Source Data NOT in sync! Will try again after 1500ms`);
        await timer(1500);
        this.timerCompensation = false;
        continue;
      }

      if (!isLeader) {
        let leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
        let tries = 20;

        while (
          !leadersFirstSeqCounts.liveSourceMseqCount ||
          (leadersFirstSeqCounts.liveSourceMseqCount === 0 && tries > 0)
        ) {
          debug(`[${this.sessionId}]: NEW FOLLOWER: Waiting for LEADER to add 'firstLiveSourceMseq' in store! Will look again after 1000ms`);
          await timer(1000);
          leadersFirstSeqCounts = await this.sessionLiveState.get(
            "firstCounts"
          );
          tries--;
        }

        if (tries === 0) {
          isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
          if (isLeader) {
            debug(`[${this.instanceId}]: I'm the new leader`);
          } else {
            debug(`[${this.instanceId}]: The leader is still alive`);
          }
        }

        // Prepare to load segments...
        debug(`[${this.instanceId}]: newest mseq from LIVE=${allMediaSeqCounts[0]}__ 1st mseq in store=${leadersFirstSeqCounts.liveSourceMseqCount}`);
        if (allMediaSeqCounts[0] === leadersFirstSeqCounts.liveSourceMseqCount) {
          this.pushAmount = 1;
        } else {
          this.pushAmount = allMediaSeqCounts[0] - leadersFirstSeqCounts.liveSourceMseqCount;
        }
        debug(`[${this.sessionId}]: ...pushAmount=${this.pushAmount}`);
      } else {
        //  LEADER calculates pushAmount differently...
        if (!this.lastRequestedM3U8 || this.lastRequestedMediaSeqRaw === 0) {
          this.pushAmount = 1;
        } else {
          this.pushAmount = allMediaSeqCounts[0] - this.lastRequestedMediaSeqRaw;
        }
        debug(`[${this.sessionId}]: ...pushAmount=${allMediaSeqCounts[0]} - ${this.lastRequestedMediaSeqRaw}`);
        break;
      }
      // Live Source Data is in sync, and LEADER & new FOLLOWER are in sync.
      break;
    }

    // Collect and Push Segment-Extracting Promises
    let pushPromises = [];
    for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
      let bw = Object.keys(this.mediaManifestURIs)[i];
      pushPromises.push(this._parseMediaManifest(this.liveSourceM3Us[bw].M3U, bw, this.mediaManifestURIs[bw], bw));
      debug(`[${this.sessionId}]: Pushed pushPromise for bw=${bw}`);
    }

    // Execute Promises II: Segment Pushing
    debug(`[${this.sessionId}]: Executing Promises II: Segment Pushing`);
    const results = await Promise.all(pushPromises);

    // NEW FOLLOWER - Edge Case: Leader is ahead for Follower. Read latest live segs from store.
    if (!isLeader) {
      let leadersCurrentMseqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
      let counts = await this.sessionLiveState.get("firstCounts");
      let leadersFirstMseqRaw = counts.liveSourceMseqCount;
      if (leadersCurrentMseqRaw && leadersCurrentMseqRaw > this.lastRequestedMediaSeqRaw) {
        // if leader never had any segs from prev mseq...
        if (leadersFirstMseqRaw && leadersFirstMseqRaw === leadersCurrentMseqRaw) {
          // ...then we clear all we collected and simply use what is in store.
          this.liveSegQueue = {};
          this.mediaSeqCount--;
        }
        // Follower updates its manifest ingedients (segment holders & counts)
        this.lastRequestedMediaSeqRaw = leadersCurrentMseqRaw;
        this.liveSegsForFollowers = await this.sessionLiveState.get("liveSegsForFollowers");
        debug(`[${this.sessionId}]: NEW FOLLOWER: Leader is ahead of me! Getting his latest segments from store.`);
        this._updateLiveSegQueue();
        // this piece is kinda required for handing out manifests to the client... Is it though? Come Back to this please.
        this.lastRequestedM3U8 = {
          bandwidth: 0,
          m3u8: null,
          mediaSeq: this.lastRequestedMediaSeqRaw,
        };
      }
    }

    // delete this debugging stuff later.
    debug(`[${this.sessionId}]: My 'liveSegQueue' looks like this-> ${Object.keys(this.liveSegQueue).length}_bws`);
    if (Object.keys(this.liveSegQueue).length !== 0) {
      let segAmounts = Object.keys(this.liveSegQueue).map((bw) => this.liveSegQueue[bw].length);
      debug(`[${this.sessionId}]: And each bw has->${segAmounts}_segs `);
    }

    // -----------------------------------------------------
    // Leader writes to store so that Followers can read.
    // -----------------------------------------------------
    if (isLeader) {
      debug(`[${this.sessionId}]: LEADER: Uploading these to store, liveSegQueue->${JSON.stringify(this.liveSegQueue)}`);
      debug(`[${this.sessionId}]: LEADER: Adding data to store!`);

      if (this.allowedToSet) {
        await this.sessionLiveState.set("liveSegsForFollowers", this.liveSegsForFollowers);
        await this.sessionLiveState.set("lastRequestedMediaSeqRaw", this.lastRequestedMediaSeqRaw);
      }

      // [LASTLY]: LEADER does this for respawn-FOLLOWERS' sake.
      if (this.firstTime) {
        // Buy some time for Followers (NOT Respawners) to fetch their own L.S m3u8.
        await timer(1000);
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
    debug(`[${this.sessionId}]: Got all needed segments from all live-source bandwidths. We are now able to build a Live Manifest`);

    return;
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
    try {
      request({ uri: mediaManifestUri, gzip: true })
      .on("error", exc => {
        debug(`ERROR: ${JSON.stringify(exc)}`);
        return new Promise((resolve, reject) => {
          reject({
            message: exc,
            bandwidth: liveTargetBandwidth,
            m3u8: null,
            mediaSeq: -1,
          });
        });
      })
      .pipe(parser);
    } catch (exc) {
      debug(`ERROR: ${JSON.stringify(exc)}`);
      return new Promise((resolve, reject) => {
        reject({
          message: exc,
          bandwidth: liveTargetBandwidth,
          m3u8: null,
          mediaSeq: -1,
        });
      });
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
    });
  }

  _miniparse(m3u, bw, mediaManifestUri, liveTargetBandwidth) {
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
        // List of all bandwidths from the VOD
        const vodBandwidths = Object.keys(this.vodSegments);
        const liveBandwidths = Object.keys(this.liveSegQueue);

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

        debug(`[${this.sessionId}]: Current RAW Mseq:  [${m3u.get("mediaSequence")}]`);
        debug(`[${this.sessionId}]: Previous RAW Mseq: [${this.lastRequestedMediaSeqRaw}]`);

        // UPDATE target for number of segments in a manifest, if appropriate.
        // if (m3u.items.PlaylistItem.length < this.targetNumSeg) {
        //   debug(`[${this.sessionId}]: WE PLAN TO LOWER SEGMENT AMOUNT IN MANIFEST FROM (${this.targetNumSeg}) TO (${m3u.items.PlaylistItem.length})`);
        //   this.targetNumSeg = m3u.items.PlaylistItem.length;
        // }

        if (this.lastRequestedM3U8 && m3u.get("mediaSequence") === this.lastRequestedMediaSeqRaw && liveTargetBandwidth !== this.lastRequestedM3U8.bandwidth) {
          debug(`[${this.sessionId}]: [What To Create?] Creating An Identical Media Sequence, but for new Bandwidth!`);
        } else {
          //  Increment mediaSeqCount - and POP vodSegs and LiveSegs appropriatly
          debug(`[${this.sessionId}]: [What To Create?] Creating a Completely New Media Sequence`);

          this.lastRequestedMediaSeqRaw = m3u.get("mediaSequence");
          this.targetDuration = m3u.get("targetDuration");

          //  REMOVE old & ADD new segments! (Dequeue and increase mediaSeqCount)
          debug(`[${this.sessionId}]: in _parseMediaManifest() ...pushAmount=${this.pushAmount}`);
          for (let j = 0; j < this.pushAmount; j++) {
            // Increase Media Sequence Count
            this.mediaSeqCount++;
            // Increase Discontinuity count if top segment is a discontinuity segment
            if (this.vodSegments[vodBandwidths[0]].length != 0 && this.vodSegments[vodBandwidths[0]][0].discontinuity) {
              this.discSeqCount++;
            }
            // Shift the top vod segment
            for (let i = 0; i < vodBandwidths.length; i++) {
              this.vodSegments[vodBandwidths[i]].shift();
            }

            // LASTLY: SHRINK IF NEEDED. (should only happen once if ever.)
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
          debug(`[${this.sessionId}]: Time to make MEDIA-SEQUENCE number: [${this.mediaSeqCount}]`);
        }

        // Switch out relative URIs if they are used, with absolute URLs
        if (mediaManifestUri) {
          // CREATE NEW MANIFEST
          let startIdx;
          startIdx = m3u.items.PlaylistItem.length - this.pushAmount;
          if (startIdx < 0) {
            startIdx = 0;
          }
          // # push segments...
          this._addLiveSegmentsToQueue(startIdx, m3u.items.PlaylistItem, baseUrl, liveTargetBandwidth);
        }

        let data = {
          bandwidth: liveTargetBandwidth,
          mediaSeq: this.lastRequestedMediaSeqRaw,
          m3u8: null,
        }
        debug(`[${this.sessionId}]: ...lastRequestedM3U8 Updated! [-][-][-][-][-]`)
        this.lastRequestedM3U8 = data;
        resolve(data);
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

  _addLiveSegmentsToQueue(startIdx, playlistItems, baseUrl, liveTargetBandwidth) {
    for (let i = startIdx; i < playlistItems.length; i++) {
      let seg = {};
      let playlistItem = playlistItems[i];
      let segmentUri;
      if (!playlistItem.properties.discontinuity) {
        if (playlistItem.properties.uri.match("^http")) {
          segmentUri = playlistItem.properties.uri;
        } else {
          segmentUri = url.resolve(baseUrl, playlistItem.properties.uri);
        }
        seg["duration"] = playlistItem.properties.duration;
        seg["uri"] = segmentUri;
        debug(`[${this.sessionId}]: X | PUSHED this segment to the QUEUE:${JSON.stringify(seg)}`);
        this.liveSegQueue[liveTargetBandwidth].push(seg);
        this.liveSegsForFollowers[liveTargetBandwidth].push(seg);
        let sizeOfQueue = this.liveSegQueue[liveTargetBandwidth].length;
        debug(`[${this.sessionId}]: size of queue = ${sizeOfQueue}_ targetNumseg=${this.targetNumSeg}`);
        if (sizeOfQueue >= this.targetNumSeg) {
          this.liveSegQueue[liveTargetBandwidth].shift();
        }
      } else {
        debug(`[${this.sessionId}]: X | PUSHED a DISCONTINUITY tag to the QUEUE`);
        this.liveSegQueue[liveTargetBandwidth].push({ discontinuity: true });
        this.liveSegsForFollowers[liveTargetBandwidth].push({
          discontinuity: true,
        });
        let sizeOfQueue = this.liveSegQueue[liveTargetBandwidth].length;
        if (sizeOfQueue >= this.targetNumSeg) {
          this.liveSegQueue[liveTargetBandwidth].shift();
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
    const liveTargetBandwidth = this._findNearestBw(bw, Object.keys(this.mediaManifestURIs));
    const vodTargetBandwidth = this._findNearestBw(bw, Object.keys(this.vodSegments));

    //  DO NOT GENERATE MANIFEST CASE: Node is NOT in sync with Leader. (Store has new segs, but node hasn't read them yet)
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader) {
      let leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
      if (leadersMediaSeqRaw !== this.lastRequestedMediaSeqRaw) {
        debug(`[${this.sessionId}]: FOLLOWER: Cannot Generate Manifest! <${this.instanceId}> New segments need to be collected first!...`);
        return null;
      }
    }

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

    // Determine if VOD segments influence targetDuration
    for (let i = 0; i < this.vodSegments[vodTargetBandwidth].length; i++) {
      let vodSeg = this.vodSegments[vodTargetBandwidth][i];
      // Get max duration amongst segments
      if (vodSeg.duration > this.targetDuration) {
        this.targetDuration = Math.round(vodSeg.duration);
      }
    }

    let m3u8FromNode = isLeader ? "LEADER" : "FOLLOWER";
    debug(`[${this.sessionId}]: Started Generating the Manifest...`);
    let m3u8 = "#EXTM3U\n";
    m3u8 += "#EXT-X-VERSION:6\n";
    m3u8 += "## " + m3u8FromNode + "\n"; // TODO: Remove this line
    m3u8 += m3u8Header(this.instanceId);
    m3u8 += "#EXT-X-INDEPENDENT-SEGMENTS\n";
    m3u8 += "#EXT-X-TARGETDURATION:" + this.targetDuration + "\n";
    m3u8 += "#EXT-X-MEDIA-SEQUENCE:" + this.mediaSeqCount + "\n";
    m3u8 += "#EXT-X-DISCONTINUITY-SEQUENCE:" + this.discSeqCount + "\n";

    if (Object.keys(this.vodSegments).length !== 0) {
      // # Add transitional segments if there are any left.
      debug(`[${this.sessionId}]: Adding a Total of (${this.vodSegments[vodTargetBandwidth].length}) VOD segments to manifest`);
      for (let i = 0; i < this.vodSegments[vodTargetBandwidth].length; i++) {
        let vodSeg = this.vodSegments[vodTargetBandwidth][i];
        if (!vodSeg.discontinuity) {
          m3u8 += "#EXTINF:" + vodSeg.duration.toFixed(3) + ",\n";
          m3u8 += vodSeg.uri + "\n";
        } else {
          m3u8 += "#EXT-X-DISCONTINUITY\n";
        }
      }
      // # Add live-source segments.
      debug(`[${this.sessionId}]: Appending Segments from Live Source of bw=(${liveTargetBandwidth}). Segment QUEUE is [${this.liveSegQueue[liveTargetBandwidth].length}] large`);
      for (let i = 0; i < this.liveSegQueue[liveTargetBandwidth].length; i++) {
        const liveSeg = this.liveSegQueue[liveTargetBandwidth][i];
        if (liveSeg.uri && liveSeg.duration) {
          m3u8 += "#EXTINF:" + liveSeg.duration.toFixed(3) + ",\n";
          m3u8 += liveSeg.uri + "\n";
        }
      }
    }
    debug(`[${this.sessionId}]: ...Manifest Generation Complete!`);
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

  _getAverageDuration(segments) {
    if (!segments) {
      debug(`[${this.sessionId}]: ERROR segments is: ${segments}`);
    }
    let total = 0;
    for (let i = 0; i < segments.length; i++) {
      const seg = segments[i];
      if (!seg.discontinuity) {
        total += seg.duration;
      }
    }
    return Math.round(total / segments.length);
  }

  _isEmpty(obj) {
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        return false;
      }
    }
    return true;
  }

  _getDelay() {
    const delayMs = 1000 * (this.delayFactor * this._getAverageDuration(this.liveSegsForFollowers[this._getFirstBwWithSegmentsInList(this.liveSegsForFollowers)]));
    debug(`[${this.sessionId}]: Current delay is: [${delayMs}ms] `);
    return delayMs;
  }
}

module.exports = SessionLive;
