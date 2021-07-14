const debug = require('debug')('engine-sessionLive');
const m3u8 = require('@eyevinn/m3u8');
const request = require('request');
const url = require('url');
const { m3u8Header } = require('./util.js');
const timer = ms => new Promise(res => setTimeout(res, ms));
const DELAY_FACTOR = 0.75;

class SessionLive {
  constructor(config) {
    this.instanceId = null;
    this.sessionId = 0;
    this.mediaSeqCount = 0;
    this.discSeqCount = 0;
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.lastRequestedM3U8 = null;
    this.vodSegments = {};
    this.mediaManifestURIs = {};
    this.liveSegQueue = [];
    this.lastRequestedMediaSeqRaw = 0;
    this.targetNumSeg = 0;
    this.latestMediaSeqSegs = {};
    this.delayFactor = DELAY_FACTOR;

    if (config) {
      if (config.instanceId) {
        this.instanceId = config.instanceId;
      }
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

  async setLiveUri(liveUri) {
    this.masterManifestUri = liveUri;
    // Get All media manifest from the Master manifest
    await this._loadMasterManifest();
  }

  async setCurrentMediaSequenceSegments(segments) {
    const allBws = Object.keys(segments);
    for (let i = 0; i < allBws.length; i++) {
      const bw = allBws[i];
      if (!this.vodSegments[bw]) {
        this.vodSegments[bw] = [];
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
  }

  // To handoff data from normal session
  async setCurrentMediaAndDiscSequenceCount(mediaSeq, discSeq) {
    debug(`[${this.sessionId}]: Setting mediaSeqCount and discSeqCount to -> [${mediaSeq}]:[${discSeq}]`);
    this.mediaSeqCount = mediaSeq - 1;
    this.discSeqCount = discSeq;
  }

  async getLiveUri() {
    return this.masterManifestUri;
  }

  // To handoff data to normal session
  async getCurrentMediaSequenceSegments() {
    await this._loadAllMediaManifests();
    debug(`[${this.sessionId}]: All bw in segs we sent to Session: ${Object.keys(this.latestMediaSeqSegs)}`);
    const lastMediaSegs = this.latestMediaSeqSegs;
    await this._resetSession();
    return lastMediaSegs;
  }

  // To handoff data to normal session
  async getCurrentMediaAndDiscSequenceCount() {
    return {
      mediaSeq: this.mediaSeqCount,
      discSeq: this.discSeqCount,
    };
  }

  // Generate manifest to give to client
  async getCurrentMediaManifestAsync(bw) {
    debug(`[${this.sessionId}]: ...Loading the selected Live Media Manifest`);
    let manifest = null;
    try {
      manifest = await this._loadMediaManifest(bw);
      if (!manifest.m3u8) {
        const delayMs = await this._getDelay();
        debug(`[${this.sessionId}]: Trying to fetch Live Media Manifest again, after a ${delayMs}ms delay!`);
        await timer(delayMs);
        manifest = await this._loadMediaManifest(bw);
        if (!manifest.m3u8) {
          debug(`[${this.sessionId}]: Could not generate a new manifest return last generated`);
          return this.lastRequestedM3U8.m3u8;
        }
      }
      // Store and update the last manifest sent to client
      if (manifest.m3u8) {
        this.lastRequestedM3U8 = manifest;
        debug(`[${this.sessionId}]: Updated lastRequestedM3U8 with new data`);
      }
      debug(`[${this.sessionId}]: Sending Requested Manifest To Client`);
      return this.lastRequestedM3U8.m3u8;
    } catch (e) {
      // session live not yet ready
      throw new Error(e);
    }
  }

  // TODO: Implement this later
  async getCurrentAudioManifestAsync(audioGroupId, audioLanguage) {
    debug(`[${this.sessionId}]: getCurrentAudioManifestAsync is NOT Implemented`);
    return "Not Implemented";
  }

  _findNearestBw(bw, array) {
    const sorted = array.sort((a, b) => b - a);
    return sorted.reduce((a, b) => {
      return Math.abs(b - bw) < Math.abs(a - bw) ? b : a;
    });
  }

  async _loadAllMediaManifests() {
    debug(`[${this.sessionId}]: ...Attempting to load all media manifest URIs in=${Object.keys(this.mediaManifestURIs)}`);
    // To make sure... we load all profiles!
    this.lastRequestedM3U8.bandwidth = null;
    let livePromises = [];
    for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
      let bw = Object.keys(this.mediaManifestURIs)[i];
      livePromises.push(this._loadMediaManifest(bw));
      debug(`[${this.sessionId}]: Pushed to this.latestMediaSeqSegs for bw=${bw}`);
    }

    const manifestList = await Promise.allSettled(livePromises);
    livePromises = [];
    const allMediaSeqCounts = manifestList.map((item) => {
      if (item.status === 'rejected') {
        return item.reason.mediaSeq;
      }
      return item.value.mediaSeq;
    });

    if (!allMediaSeqCounts.every( (val, i, arr) => val === arr[0])) {
      const maxMseq = Math.max(...allMediaSeqCounts);
      debug(`[${this.sessionId}]: maxMseq=${maxMseq}, everyone=${allMediaSeqCounts}`);
      const delayMs = await this._getDelay();
      let isFirstTime = true;
      for (let i = 0; i < manifestList.length; i++) {
        let retryBw = 0;
        let mseq = 0;
        if (manifestList[i].status === 'rejected') {
          retryBw = manifestList[i].reason.bandwidth;
          mseq = manifestList[i].reason.mediaSeq;
        } else {
          retryBw = manifestList[i].value.bandwidth;
          mseq = manifestList[i].value.mediaSeq;
        }
        if (mseq < maxMseq) {
          if (isFirstTime) {
            // Try to not fetch in the middle of a segment by accident,
            // Wait for the live source to generate the next one before trying again
            // Time to wait is approximately 75% of an average seg duration
            debug(`[${this.sessionId}]: Delay before trying again is ${delayMs}ms`);
            await timer(delayMs);
            isFirstTime = false;
          }
          livePromises.push(this._loadMediaManifest(retryBw));
        }
      }
      if (livePromises.length > 0) {
        // Try to fetch new media manifests one last time
        debug(`[${this.sessionId}]: ...Trying one more time to create updated manifests for [${livePromises.length}] bandwidths.`);
        try {
          await Promise.all(livePromises);
          debug(`[${this.sessionId}]: Succeeded when trying again.`);
        } catch (e) {
          throw new Error(e);
        }
      }
    }
    for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
      const bw = Object.keys(this.mediaManifestURIs)[i];
      this.latestMediaSeqSegs[bw].push({ discontinuity: true });
      debug(`[${this.sessionId}]: ...Added a disc-segment to this.latestMediaSeqSegs for bw=${bw}`);
    }
    debug(`[${this.sessionId}]: Got all segments for all bandwidths to pass on to VOD2Live Session`);
  }

  /**
   *
   * @returns Loads the URIs to the different media playlists from the given master playlist
   */
  _loadMasterManifest() {
    return new Promise((resolve, reject) => {
      const parser = m3u8.createStream();
      try {
        request({ uri: this.masterManifestUri, gzip: true })
        .on("error", err => {
          debug(`ERROR: ${Object.keys(exc)}`);
          reject(err);
        })
        .pipe(parser);
      } catch (exc) {
        debug(`ERROR: ${Object.keys(exc)}`);
        reject(exc);
      }
      parser.on("m3u", m3u => {
        debug(`[${this.sessionId}]: ...Fetched a New Live Master Manifest from:\n${this.masterManifestUri}`);
        let baseUrl = "";
        const m = this.masterManifestUri.match(/^(.*)\/.*?$/);
        if (m) {
          baseUrl = m[1] + '/';
        }
        // Get all Profile manifest URIs in the Live Master Manifest
        for (let i = 0; i < m3u.items.StreamItem.length; i++) {
          const streamItem = m3u.items.StreamItem[i];
          const streamItemBW = streamItem.get("bandwidth");
          const mediaManifestUri = url.resolve(baseUrl, streamItem.get('uri'));
          if (!this.mediaManifestURIs[streamItemBW]) {
            this.mediaManifestURIs[streamItemBW] = '';
          }
          this.mediaManifestURIs[streamItemBW] = mediaManifestUri;
        }
        debug(`[${this.sessionId}]: All Live Media Manifest URIs have been collected. (${Object.keys(this.mediaManifestURIs).length}) profiles found!`);
        resolve();
      });
      parser.on("error", err => {
        debug(`ERROR: ${Object.keys(exc)}`);
        reject(err);
      });
    });
  }

  _loadMediaManifest(bw) {
    return new Promise((resolve, reject) => {
      if (this._isEmpty(this.mediaManifestURIs)) {
        reject(`[${this.sessionId}]: Not yet ready to switch to live.`);
      }
      let recreateMseq = false;
      let createNewMseq = false;
      let mediaSeqCountDiffRaw = 0;

      debug(`[${this.sessionId}]: Trying to fetch live manifest for profile with bandwidth: ${bw}`);
      // What bandwidth is closest to the desired bw
      const liveTargetBandwidth = this._findNearestBw(bw, Object.keys(this.mediaManifestURIs));
      debug(`[${this.sessionId}]: Nearest Bandwidth is: ${liveTargetBandwidth}`);
      // Init | Clear Out -> Get New
      this.latestMediaSeqSegs[liveTargetBandwidth] = [];
      // Get the target media manifest
      const mediaManifestUri = this.mediaManifestURIs[liveTargetBandwidth];
      // Load a New Manifest
      const parser = m3u8.createStream();
      try {
        request({ uri: mediaManifestUri, gzip: true })
        .on("error", err => {
          debug(`ERROR: ${Object.keys(exc)}`);
          reject({
            message: err,
            bandwidth: liveTargetBandwidth,
            m3u8: null,
            mediaSeq: -1
          });
        })
        .pipe(parser);
      } catch (exc) {
        debug(`ERROR: ${Object.keys(exc)}`);
        reject({
          message: err,
          bandwidth: liveTargetBandwidth,
          m3u8: null,
          mediaSeq: -1,
        });
      }

      // List of all bandwidths from the VOD
      const vodBandwidths = Object.keys(this.vodSegments);
      let baseUrl = '';
      const m = mediaManifestUri.match(/^(.*)\/.*?$/);
      if (m) {
        baseUrl = m[1] + '/';
      }

      parser.on("m3u", (m3u) => {
        debug(`[${this.sessionId}]: Current RAW Mseq:  [${m3u.get("mediaSequence")}]`);
        debug(`[${this.sessionId}]: Previous RAW Mseq: [${this.lastRequestedMediaSeqRaw}]`);
        // Before anything else is done: Check if Live Source has created a new media sequence or not
        if (this.lastRequestedM3U8 && m3u.get("mediaSequence") === this.lastRequestedMediaSeqRaw && liveTargetBandwidth === this.lastRequestedM3U8.bandwidth) {
          debug(`[${this.sessionId}]: [What To Create?] Sending old manifest (Live Source does not have a new Mseq)`);
          resolve(this.lastRequestedM3U8);
          return;
        }
        if (this.lastRequestedM3U8 && m3u.get('mediaSequence') < this.lastRequestedMediaSeqRaw) {
          debug(`[${this.sessionId}]: [What To Create?] Odd case! Live Source MediaSeq is not up tp date. Aborting.`);
          resolve({
            bandwidth: liveTargetBandwidth,
            m3u8: null,
            mediaSeq: m3u.get('mediaSequence'),
          });
          return;
        } else if (this.lastRequestedM3U8 && m3u.get("mediaSequence") === this.lastRequestedMediaSeqRaw && liveTargetBandwidth !== this.lastRequestedM3U8.bandwidth) {
          // New sequence and/or New Bandwidth
          // If they are the same sequence but different bw, do not pop! rebuild the manifest with all parts.
          debug(`[${this.sessionId}]: [What To Create?] Creating An Identical Media Sequence, but for new Bandwidth!`);
          recreateMseq = true;
        } else {
          createNewMseq = true;
          // In case this Next sequence is expected to be in a different profile,
          // then we need to use the recreate code.
          if (this.lastRequestedM3U8 && liveTargetBandwidth === this.lastRequestedM3U8.bandwidth) {
            recreateMseq = false;
          } else {
            recreateMseq = true;
          }

          // Calculate difference in media sequence count between current and previous Live Manifest
          mediaSeqCountDiffRaw = this.lastRequestedM3U8 ? m3u.get("mediaSequence") - this.lastRequestedMediaSeqRaw : 1;
          this.lastRequestedMediaSeqRaw = m3u.get("mediaSequence");
          this.targetDuration = m3u.get("targetDuration");

          // Dequeue and increase mediaSeqCount
          for (let j = 0; j < mediaSeqCountDiffRaw; j++) {
            // Increase Discontinuity count if top segment is a discontinuity segment
            if (this.vodSegments[vodBandwidths[0]].length != 0 && this.vodSegments[vodBandwidths[0]][0].discontinuity) {
              this.discSeqCount++;
            }
            if (this.lastRequestedM3U8) {
              // Shift the top vod segment
              for (let i = 0; i < vodBandwidths.length; i++) {
                this.vodSegments[vodBandwidths[i]].shift();
              }
            }
            // LASTLY: Do we need to dequeue the queue?
            if (this.lastRequestedM3U8 && this.vodSegments[vodBandwidths[0]].length === 0 || this.liveSegQueue.length > this.targetNumSeg) {
              this.liveSegQueue.shift();
            }
            this.mediaSeqCount++;
          }
          debug(`[${this.sessionId}]: [What To Create?] Creating a Completely New Media Sequence`);
          debug(`[${this.sessionId}]: Time to make MEDIA-SEQUENCE number: [${this.mediaSeqCount}]`);
        }
        // Switch out relative URIs if they are used, with absolute URLs
        if (this.lastRequestedM3U8 && mediaManifestUri) {
          if (!recreateMseq || this.liveSegQueue.length === 0) {
            // CASE: Live source is more than 1 sequence ahead push all "new" segments
            let startIdx = m3u.items.PlaylistItem.length - mediaSeqCountDiffRaw;
            if (startIdx < 0) {
              startIdx = 0;
            }
            for (let i = startIdx; i < m3u.items.PlaylistItem.length; i++) {
              let seg = {};
              let playlistItem = m3u.items.PlaylistItem[i];
              let segmentUri;
              if (!playlistItem.properties.discontinuity) {
                if (playlistItem.properties.uri.match('^http')) {
                  segmentUri = playlistItem.properties.uri;
                } else {
                  segmentUri = url.resolve(baseUrl, playlistItem.properties.uri);
                }
                seg["duration"] = playlistItem.properties.duration;
                seg["uri"] = segmentUri;
                debug(`[${this.sessionId}]: A | PUSHED this segment to the QUEUE:${JSON.stringify(seg)}`);
                this.liveSegQueue.push(seg);
              } else {
                debug(`[${this.sessionId}]: A | PUSHED a DISCONTINUITY tag to the QUEUE`);
                this.liveSegQueue.push({ discontinuity: true });
              }
            }
          } else {
            /*
              -----------------
              SPECIAL TREATMENT
              -----------------
              Clear out liveSegQueue and add multiple live segments
            */
            const queueLength = this.liveSegQueue.length;
            const liveLength = m3u.items.PlaylistItem.length;

            if (queueLength >= liveLength) {
              // Then pop liveLength times, then append ALL segments from live
              // Create a liveLength sized whole in the queue
              for (let i = 0; i < liveLength; i++) {
                this.liveSegQueue.pop();
              }
              debug(`[${this.sessionId}]: Emptied a part of the QUEUE`);
              // Iterate from 0 to end, and push segment
              for (let i = 0; i < liveLength; i++) {
                let seg = {};
                let playlistItem = m3u.items.PlaylistItem[i];
                let segmentUri;
                if (!playlistItem.properties.discontinuity) {
                  if (playlistItem.properties.uri.match('^http')) {
                    segmentUri = playlistItem.properties.uri;
                  } else {
                    segmentUri = url.resolve(baseUrl, playlistItem.properties.uri);
                  }
                  seg["duration"] = playlistItem.properties.duration;
                  seg["uri"] = segmentUri;
                  debug(`[${this.sessionId}]: B | PUSHED this segment to the QUEUE:${JSON.stringify(seg)}`);
                  this.liveSegQueue.push(seg);
                } else {
                  debug(`[${this.sessionId}]: B | PUSHED a DISCONTINUITY tag to the QUEUE`);
                  this.liveSegQueue.push({ discontinuity: true });
                }
              }
            } else { // (If queueLen < liveLength)
              // Empty the queue, then append queue.length many segments from live
              this.liveSegQueue = [];
              debug(`[${this.sessionId}]: Emptied the QUEUE!`);
              // Since we might have POPPED above, compensate now
              let size = queueLength;
              if (createNewMseq) {
                size += mediaSeqCountDiffRaw;
              }
              // Iterate backwards from END to (end - queue.length), and unshift segment
              for (let i = 1; i <= size; i++) {
                let seg = {};
                let playlistItem = m3u.items.PlaylistItem[liveLength - i];
                if (!playlistItem) {
                  debug(`[${this.sessionId}]: Playlist item is ${playlistItem}`);
                }
                let segmentUri;
                if (!playlistItem.properties.discontinuity) {
                  if (playlistItem.properties.uri.match('^http')) {
                    segmentUri = playlistItem.properties.uri;
                  } else {
                    segmentUri = url.resolve(baseUrl, playlistItem.properties.uri);
                  }
                  seg["duration"] = playlistItem.properties.duration;
                  seg["uri"] = segmentUri;
                  debug(`[${this.sessionId}]: # C | PUSHED this segment to the QUEUE: ${JSON.stringify(seg)}`);
                  this.liveSegQueue.unshift(seg);
                } else {
                  debug(`[${this.sessionId}]: # C | PUSHED a DISCONTINUITY tag to the QUEUE`);
                  this.liveSegQueue.unshift({ discontinuity: true });
                }
              }
            }
          }
        }
        /*
          ----------------------
            GENERATE MANIFEST
          ----------------------
        */
        const vodTargetBandwidth = this._findNearestBw(bw, vodBandwidths);
        // Determine if VOD segments influence targetDuration
        for (let i = 0; i < this.vodSegments[vodTargetBandwidth].length; i++) {
          let vodSeg = this.vodSegments[vodTargetBandwidth][i];
          // Get max duration amongst segments
          if (vodSeg.duration > this.targetDuration) {
            this.targetDuration = Math.round(vodSeg.duration);
          }
        }

        debug(`[${this.sessionId}]: ...Start Generating the Manifest`);
        let m3u8 = "#EXTM3U\n";
        m3u8 += "#EXT-X-VERSION:6\n";
        m3u8 += m3u8Header(this.instanceId);
        m3u8 += "#EXT-X-INDEPENDENT-SEGMENTS\n";
        m3u8 += "#EXT-X-TARGETDURATION:" + this.targetDuration + "\n";
        m3u8 += "#EXT-X-MEDIA-SEQUENCE:" + this.mediaSeqCount + "\n";
        m3u8 += "#EXT-X-DISCONTINUITY-SEQUENCE:" + this.discSeqCount + "\n";

        if (vodBandwidths.length !== 0) {
          debug(`[${this.sessionId}]: Adding a Total of (${this.vodSegments[vodTargetBandwidth].length}) VOD segments to Manifest`);
          for (let i = 0; i < this.vodSegments[vodTargetBandwidth].length; i++) {
            let vodSeg = this.vodSegments[vodTargetBandwidth][i];
            this.latestMediaSeqSegs[liveTargetBandwidth].push(vodSeg);
            // Get max duration amongst segments
            if (!vodSeg.discontinuity) {
              m3u8 += "#EXTINF:" + vodSeg.duration.toFixed(3) + ",\n";
              m3u8 += vodSeg.uri + "\n";
            } else {
              m3u8 += "#EXT-X-DISCONTINUITY\n";
            }
          }
          debug(`[${this.sessionId}]: Appending Segments from Live Source. Segment QUEUE is [ ${this.liveSegQueue.length} ] large`);
          for (let i = 0; i < this.liveSegQueue.length; i++) {
            const liveSeg = this.liveSegQueue[i];
            this.latestMediaSeqSegs[liveTargetBandwidth].push(liveSeg);
            if (liveSeg.uri && liveSeg.duration) {
              m3u8 += "#EXTINF:" + liveSeg.duration.toFixed(3) + ",\n";
              m3u8 += liveSeg.uri + "\n";
            }
          }
        }
        debug(`[${this.sessionId}]: Manifest Generation Complete!`);
        resolve({
          bandwidth: liveTargetBandwidth,
          mediaSeq: this.lastRequestedMediaSeqRaw,
          m3u8: m3u8,
        });
      });
      parser.on("error", err => {
        debug(`ERROR: ${Object.keys(exc)}`);
        reject({
          message: err,
          bandwidth: liveTargetBandwidth,
          m3u8: null,
          mediaSeq: -1,
        });
      });
    });
  }

  async _resetSession() {
    debug(`[${this.sessionId}]: Resetting Live Session`);
    this.mediaSeqCount = 0;
    this.discSeqCount = 0;
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.lastRequestedM3U8 = null;
    this.vodSegments = {};
    this.mediaManifestURIs = {};
    this.liveSegQueue = [];
    this.lastRequestedMediaSeqRaw = 0;
    this.targetNumSeg = 0;
    this.latestMediaSeqSegs = {};
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

  async _getDelay() {
    const delayMs = 1000 * (this.delayFactor * this._getAverageDuration(this.latestMediaSeqSegs[this._getFirstBwWithSegmentsInList(this.latestMediaSeqSegs)]));
    debug(`[${this.sessionId}]: Current delay is: [${delayMs}ms] `);
    return delayMs;
  }
}

module.exports = SessionLive;
