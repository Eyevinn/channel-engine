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
    this.allowedToSet = false;
    this.pushAmount = 0;

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

  async resetLiveStoreAsync(resetDelay) {
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader) {
      return;
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
    debug(`[${this.instanceId}][${this.sessionId}]: I'm the leader and have cleared the local store after ${resetDelay}ms`);
  }

  resetSession() {
    this.mediaSeqCount = 0;
    this.discSeqCount = 0;
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.vodSegments = {};
    this.mediaManifestURIs = {};
    this.liveSegQueue = {};
    this.lastRequestedMediaSeqRaw = 0;
    this.targetNumSeg = 0;
    this.liveSourceM3Us = {};
    this.liveSegsForFollowers = {};
    this.timerCompensation = null;
    this.firstTime = true;
    this.pushAmount = 0;
    this.allowedToSet = false;
    debug(`[${this.instanceId}][${this.sessionId}]: resetting sessionLive`);
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
          this.resetSession();
          this.resetLiveStoreAsync(RESET_DELAY);
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

        // Fetch Live-Source Segments, and get ready for on-the-fly manifest generation
        // And also compensate for processing time
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

  async setLiveUri(liveUri) {
    // Load & Parse all Media Manifest uris from Master
    await this._loadMasterManifest(liveUri);
    // This will let playhead call Live Source for manifests
    this.masterManifestUri = liveUri;
    this.firstTime = true;
  }

  async setCurrentMediaSequenceSegments(segments) {

    // Make it possible to add & share new segments
    this.allowedToSet = true;

    let discCount = 0;
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
      if (segments[allBws[0]][segIdx].discontinuity) {
        discCount++;
      }
    }
    this.targetNumSeg = this.vodSegments[allBws[0]].length - discCount;
    debug(`[${this.sessionId}]: Setting CurrentMediaSequenceSegments. First seg is: [${this.vodSegments[allBws[0]][0].uri}]`);

    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (isLeader) {
      debug(`[${this.sessionId}]: LEADER: I am adding 'transitSegs'=${JSON.stringify(this.vodSegments)} to Store for future followers`);
      await this.sessionLiveState.set("transitSegs", this.vodSegments);
    }
  }

  async setCurrentMediaAndDiscSequenceCount(mediaSeq, discSeq) {
    debug(`[${this.sessionId}]: Setting mediaSeqCount and discSeqCount to: [${mediaSeq}]:[${discSeq}]`);
    this.mediaSeqCount = mediaSeq;
    this.discSeqCount = discSeq;

    // IN CASE: New/Respawned Node Joins the Live Party
    // Don't use what Session gave you. Use the Leaders number if it's available
    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (isLeader) {
      let firstCounts = await this.sessionLiveState.get("firstCounts");
      firstCounts.discSeqCount = this.discSeqCount;
      await this.sessionLiveState.set("firstCounts", firstCounts);
    } else {
      const liveCounts = await this.sessionLiveState.get("firstCounts");
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
        // Follower updates its manifest ingedients (segment holders & counts)
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
      debug(`[${this.sessionId}]: Pushed loadMedia promise for bw=${bw}`);
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
    debug(`[${this.sessionId}]: ...Loading the selected Live Media Manifest`);
    let attempts = 10;
    let m3u8 = null;
    while (!m3u8 && attempts > 0) {
      attempts--;
      m3u8 = await this._GenerateLiveManifest(bw);
      if (!m3u8) {
        debug(`[${this.sessionId}]: No manifest available yet, will try again after 1000ms`);
        await timer(1000);
      }
    }
    if (!m3u8) {
      throw new Error("Failed to generate manifest after 10000ms");
    }

    const isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    // TODO: Remove this
    if (isLeader) {
      debug(`[${this.sessionId}]: LEADER: Sending requested manifest to client [${JSON.stringify(m3u8)}]\n\n`);
    } else {
      debug(`[${this.sessionId}]: FOLLOWER: Sending requested manifest to client [${JSON.stringify(m3u8)}]\n\n`);
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
    debug(`[${this.sessionId}]: [size=${size}]->this.liveSegsForFollowers=${ Object.keys(this.liveSegsForFollowers)} `);
    // Remove transitional segs & add live source segs collected from store
    for (let k = 0; k < size; k++) {
      // Increase Discontinuity count if top segment is a discontinuity segment
      if (this.vodSegments[vodBws[0]].length !== 0 && this.vodSegments[vodBws[0]][0].discontinuity) {
        this.discSeqCount++;
      }
      // Shift the top vod segment on all variants
      for (let i = 0; i < vodBws.length; i++) {
        let seg = this.vodSegments[vodBws[i]].shift();
        if (seg && seg.discontinuity) {
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

    // -------------------------------------
    //  If I am a Follower-node then my job
    //  ends here, where I only read from store.
    // -------------------------------------
    let isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    if (!isLeader && this.lastRequestedMediaSeqRaw !== 0) {
      debug(`[${this.sessionId}]: FOLLOWER: Reading data from store!`);

      let leadersMediaSeqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
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
          debug(`[${this.instanceId}]: I'm the new leader`);
          return;
        } else {
          debug(`[${this.instanceId}]: The leader is still alive`);
          return
        }
      }

      // Follower updates its manifest ingedients (segment holders & counts)
      this.lastRequestedMediaSeqRaw = leadersMediaSeqRaw;
      this.liveSegsForFollowers = await this.sessionLiveState.get("liveSegsForFollowers");
      debug(`[${this.sessionId}]: +-+ Look these are my segments from store: [${JSON.stringify(this.liveSegsForFollowers)}]`);
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
        debug(`[${this.sessionId}]: LEADER: Trying to fetch manifests for all bandwidths\n Attempts left=[${FETCH_ATTEMPTS}]`);
      } else {
        debug(`[${this.sessionId}]: NEW FOLLOWER: Trying to fetch manifests for all bandwidths\n Attempts left=[${FETCH_ATTEMPTS}]`);
      }

      // Reset Values Each Attempt
      let livePromises = [];
      this.pushAmount = 0;

      // Collect Live Source Requesting Promises
      for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
        let bw = Object.keys(this.mediaManifestURIs)[i];
        livePromises.push(this._loadMediaManifest(bw));
        debug(`[${this.sessionId}]: Pushed loadMedia promise for bw=[${bw}]`);
      }

      // Fetech From Live Source
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
        debug(`[${this.sessionId}]: Live Mseq counts=[${allMediaSeqCounts}]`);
        //  Decement fetch counter.
        --FETCH_ATTEMPTS;
        //  Wait a little before trying again.
        debug(`[${this.sessionId}]: [ALERT] | Live Source Data NOT in sync! Will try again after 1500ms`);
        await timer(1500);
        this.timerCompensation = false;
        continue;
      }

      if (!isLeader) {
        let leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
        let tries = 20;

        while (!leadersFirstSeqCounts.liveSourceMseqCount || (leadersFirstSeqCounts.liveSourceMseqCount === 0 && tries > 0)) {
          debug(`[${this.sessionId}]: NEW FOLLOWER: Waiting for LEADER to add 'firstCounts' in store! Will look again after 1000ms`);
          await timer(1000);
          leadersFirstSeqCounts = await this.sessionLiveState.get("firstCounts");
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
        // if (leadersFirstSeqCounts):
        // Prepare to load segments...
        debug(`[${this.instanceId}]: newest mseq from LIVE=${allMediaSeqCounts[0]} first mseq in store=${leadersFirstSeqCounts.liveSourceMseqCount}`);
        if (allMediaSeqCounts[0] === leadersFirstSeqCounts.liveSourceMseqCount) {
          this.pushAmount = 1; // Follower from start
        } else {
          // RESPAWNED NODES DO THIS!
          this.pushAmount = (allMediaSeqCounts[0] - leadersFirstSeqCounts.liveSourceMseqCount) + 1;
          const transitSegs = await this.sessionLiveState.get("transitSegs");
          debug(`[${this.sessionId}]: NEW FOLLOWER: I tried to get 'transitSegs'. This is what I found ${JSON.stringify(transitSegs)}`);
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
        }
        debug(`[${this.sessionId}]: ...pushAmount=${allMediaSeqCounts[0]} - ${this.lastRequestedMediaSeqRaw}`);
        break;
      }
      // Live Source Data is in sync, and LEADER & new FOLLOWER are in sync.
      break;
    }

    isLeader = await this.sessionLiveStateStore.isLeader(this.instanceId);
    // NEW FOLLOWER - Edge Case: One Instance is ahead of another. Read latest live segs from store.
    if (!isLeader) {
      const leadersCurrentMseqRaw = await this.sessionLiveState.get("lastRequestedMediaSeqRaw");
      const counts = await this.sessionLiveState.get("firstCounts");
      const leadersFirstMseqRaw = counts.liveSourceMseqCount;
      if (leadersCurrentMseqRaw && leadersCurrentMseqRaw !== this.lastRequestedMediaSeqRaw) {
        // if leader never had any segs from prev mseq...
        if (leadersFirstMseqRaw && leadersFirstMseqRaw === leadersCurrentMseqRaw) {
          // Follower updates it's manifest ingedients (segment holders & counts)
          this.lastRequestedMediaSeqRaw = leadersCurrentMseqRaw;
          this.liveSegsForFollowers = await this.sessionLiveState.get("liveSegsForFollowers");
          debug(`[${this.sessionId}]: NEW FOLLOWER: Leader is ahead or behind me! Clearing Queue and Getting his latest segments from store.`);
          this._updateLiveSegQueue();
          this.firstTime = false;
          debug(`[${this.sessionId}]: Got all needed segments from all live-source bandwidths. We are now able to build a Live Manifest`);
          return;
        }
      }
    }
    if (this.allowedToSet) {
      // Collect and Push Segment-Extracting Promises
      let pushPromises = [];
      for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
        let bw = Object.keys(this.mediaManifestURIs)[i];
        pushPromises.push(this._parseMediaManifest(this.liveSourceM3Us[bw].M3U, bw, this.mediaManifestURIs[bw], bw));
        debug(`[${this.sessionId}]: Pushed pushPromise for bw=${bw}`);
      }

      // Segment Pushing
      debug(`[${this.sessionId}]: Executing Promises II: Segment Pushing`);
      const results = await Promise.all(pushPromises);
      // UPDATE COUNTS, & Shift Segments in vodSegments
      await this._incrementAndShift();
    }

    // -----------------------------------------------------
    // Leader writes to store so that Followers can read.
    // -----------------------------------------------------
    if (isLeader) {
      debug(`[${this.sessionId}]: LEADER: Uploading these to store, liveSegQueue->[${JSON.stringify(this.liveSegQueue)}]`);
      debug(`[${this.sessionId}]: LEADER: Adding data to store!`);

      if (this.allowedToSet) {
        await this.sessionLiveState.set("liveSegsForFollowers", this.liveSegsForFollowers);
        await this.sessionLiveState.set("lastRequestedMediaSeqRaw", this.lastRequestedMediaSeqRaw);
      }

      // [LASTLY]: LEADER does this for respawned-FOLLOWERS' sake.
      if (this.firstTime) {
        // Buy some time for Followers (NOT Respawned) to fetch their own L.S m3u8.
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

    this.firstTime = false;
    debug(`[${this.sessionId}]: Got all needed segments from all live-source bandwidths. We are now able to build a Live Manifest`);

    return;
  }

  async _incrementAndShift() {
    const vodBandwidths = Object.keys(this.vodSegments);
    for (let j = 0; j < this.pushAmount; j++) {
      // Increase Media Sequence Count
      this.mediaSeqCount++;
      // Increase Discontinuity count if top segment is a discontinuity segment
      if (this.vodSegments[vodBandwidths[0]].length != 0 && this.vodSegments[vodBandwidths[0]][0].discontinuity) {
        this.discSeqCount++;
      }
      // Shift the top vod segment
      for (let i = 0; i < vodBandwidths.length; i++) {
        let seg = this.vodSegments[vodBandwidths[i]].shift();
        if (seg && seg.discontinuity) {
          this.vodSegments[vodBandwidths[i]].shift();
        }
      }
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
          // push segments...
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

    const date = new Date(); // TODO: Remove this line
    const dateString = date.toISOString(); // TODO: Remove this line

    let m3u8FromNode = isLeader ? "LEADER" : "FOLLOWER";
    debug(`[${this.sessionId}]: Started Generating the Manifest...`);
    let m3u8 = "#EXTM3U\n";
    m3u8 += "#EXT-X-VERSION:6\n";
    m3u8 += "## " + m3u8FromNode + "\n"; // TODO: Remove this line
    m3u8 += "## CurrentTime: " + dateString + "\n"; // TODO: Remove this line
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
      // Add live-source segments
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

  _getDelay() {
    const delayMs = 1000 * (this.delayFactor * this._getAverageDuration(this.liveSegsForFollowers[this._getFirstBwWithSegmentsInList(this.liveSegsForFollowers)]));
    debug(`[${this.sessionId}]: Current delay is: [${delayMs}ms] `);
    return delayMs;
  }
}

module.exports = SessionLive;
