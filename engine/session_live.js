const debug = require('debug')('engine-sessionLive');
const m3u8 = require('@eyevinn/m3u8');
const request = require('request');
// TODO: Don't use this package on release (deprecated)
const url = require('url');

class SessionLive {
  constructor(config) {
    this.sessionId = 0;
    this.mediaSeqCount = 0;
    this.discSeqCount = 0;
    this.lastMediaSeq = {}
    this.events = [];
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.useDemuxedAudio = false;
    this.cloudWatchLogging = false;
    this.currentMetadata = {};
    this.lastRequestedM3U8 = null;
    this.mediaSeqSubset = {}
    this.mediaManifestURIs = {};
    this.liveSegQueue = [];
    this.lastRequestedMediaseqRaw = 0;
    this.targetNumSeg = 0;
    this.latestMediaSeqSegs = {};

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

  // not currently used/tested, This is used for going LIVE->V2L
  async _resetSession() {
    this.mediaSeqCount = 0;
    this.discSeqCount = 0;
    this.lastMediaSeq = {}
    this.events = [];
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.currentMetadata = {};
    this.lastRequestedM3U8 = null;
    this.mediaSeqSubset = {},
    this.mediaManifestURIs = {};
    this.liveSegQueue = [];
    this.lastRequestedMediaseqRaw = 0;
    this.targetNumSeg = 0;
    this.latestMediaSeqSegs = {};
  }

  async setLiveUri(liveUri) {
    this.masterManifestUri = liveUri;
    // Get All media manifest inside the entered Master manifest
    await this._loadMasterManifest();
  }

  async getLiveUri() {
    return this.masterManifestUri;
  }

  // Switcher will call this
  // To use data from normal session
  /**
   *  item in array -> { duration: 10, uri: http*** }
   *  segments = {
   *    bw1: [{ }, { }, { } ... nth { }],
   *    bw2: [{ }, { }, { } ... nth { }],
   *    bw3: [{ }, { }, { } ... nth { }],
   *  }
   *
   *
   * newobj = {}
   *
   * in _loadMasterManifest
   *
   * newobj = {
   *    bw1: [],
   *    bw2: [],
   *    bw3: [],
   * }
   *
   * when in loadMediaManifest('bw1')
   * newobj['bw1'] = []
   * newobj['bw1'].push(segments_that_are_used)
   *
   * after loadMediaManifest()
   *
   *  newobj = {
   *    bw1: [{},{},{},{},{},{},{},{},{},{}],
   *    bw2: [{},{},{},{},{},{},{},{},{},{}],
   *    bw3: [{},{},{},{},{},{},{},{},{},{}]
   * }
   *
   */
  async setCurrentMediaSequenceSegments(segments) {
    this.lastMediaSeq = segments;
    const allBws = Object.keys(segments);
    for (let i = 0; i < allBws.length; i++) {
      const bw = allBws[i];
      if (!this.mediaSeqSubset[bw]) {
        this.mediaSeqSubset[bw] = [];
      }
      const segLen = segments[bw].length;
      for (let segIdx = segLen - segLen; segIdx < segLen; segIdx++) {
        this.mediaSeqSubset[bw].push(segments[bw][segIdx]);
      }
      this.mediaSeqSubset[bw].push({ discontinuity: true });
    }
    this.targetNumSeg = this.mediaSeqSubset[allBws[0]].length;
  }

  // To hand off data to normal session
  async getCurrentMediaSequenceSegments() {
    debug(`[${this.sessionId}]: getCurrentMediaSequenceSegments() STARTED`);
    const timer = ms => new Promise(res => setTimeout(res, ms));
    try {
      await this._loadAllMediaManifests();
    } catch {
      try {
        await timer(500);
        await this._loadAllMediaManifests();
      } catch {
        try {
          await timer(500);
          await this._loadAllMediaManifests();
        } catch {
          throw new Error('Failed retrying 3 times');
        }
      }
    }
    debug(`[_____]: getCurrentMediaSequenceSegments() DUNZO`);
    
    debug(`[___]: ...all bw in segs we send to session: ${Object.keys(this.latestMediaSeqSegs)}`);
    return this.latestMediaSeqSegs;
  }

  // Switcher will call this
  // To use data from normal session
  async setCurrentMediaAndDiscSequenceCount(mediaSeq, discSeq) {
    debug(`[${this.sessionId}]: Setting this.mediaSeqCount & this.discSeqCount to -> [${mediaSeq}]:[${discSeq}]`);
    this.mediaSeqCount = mediaSeq - 1;
    this.discSeqCount = discSeq;
  }

  // To hand off data to normal session
  async getCurrentMediaAndDiscSequenceCount() {
    return {
      mediaSeq: this.mediaSeqCount,
      discSeq: this.discSeqCount,
    };
  }

  // To give manifest to client
  async getCurrentMediaManifestAsync(bw) {
    debug(`[${this.sessionId}]: ...Loading selected Live Media Manifest`);
    const manifest = await this._loadMediaManifest(bw);
    // Store and update the last manifest sent to client
    this.lastRequestedM3U8 = manifest;
    debug(`[${this.sessionId}]: Updated lastRequestedM3U8 with new data`);
    debug(`[${this.sessionId}]: Sending Requested Manifest To Client`);
    return this.lastRequestedM3U8.m3u8;
  }

  // Work on this later
  async getCurrentAudioManifestAsync(audioGroupId, audioLanguage) {

  }

  _findNearestBw(bw, array) {
    const sorted = array.sort((a, b) => b - a);
    return sorted.reduce((a, b) => {
      return Math.abs(b - bw) < Math.abs(a - bw) ? b : a;
    });
  }

  async _loadAllMediaManifests() {
    debug(`[${this.sessionId}]: ...lets loop for each in=${Object.keys(this.mediaManifestURIs)}`);
    
    // To make sure... we load all profiles!
    this.lastRequestedM3U8.bandwidth = null;

    for (let i = 0; i < Object.keys(this.mediaManifestURIs).length; i++) {
      let bw = Object.keys(this.mediaManifestURIs)[i];
      await this._loadMediaManifest(bw);
      this.latestMediaSeqSegs[bw].push({ discontinuity: true });
      debug(`[${this.sessionId}]: ...I gave this.latestMediaSeqSegs segs from bw=${bw}`);
    }

    debug(`[${this.sessionId}]: ...I gave this.latestMediaSeqSegs segs from all bandwidths`);
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
        .on('error', err => {
          reject(err);
        })
        .pipe(parser);
      } catch (exc) {
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
            this.mediaManifestURIs[streamItemBW] = "";
          }
          this.mediaManifestURIs[streamItemBW] = mediaManifestUri;
        }
        debug(`[${this.sessionId}]: All Media Manifest URIs have been collected. (${Object.keys(this.mediaManifestURIs).length}) profiles found!`);
        resolve();
      });
      parser.on("error", err => {
        reject(err);
      });
    });
  }

  _loadMediaManifest(bw) {
    return new Promise((resolve, reject) => {
      let RECREATE_MSEQ = false;
      let CREATE_NEW_MSEQ = false;
      let RAW_mseq_diff = 0;


      debug(`[${this.sessionId}]: # Trying to fetch live manifest for profile with bandwidth: ${bw}`);
      // What bandwidth is closest to the desired bw
      const liveTargetBandwidth = this._findNearestBw(bw, Object.keys(this.mediaManifestURIs));
      debug(`[${this.sessionId}]: # Nearest Bandwidth is: ${liveTargetBandwidth}`);
      // Init | Clear Out -> Get New
      this.latestMediaSeqSegs[liveTargetBandwidth] = [];
      // Get the target media manifest
      const mediaManifestUri = this.mediaManifestURIs[liveTargetBandwidth];
      // Load a New Manifest
      const parser = m3u8.createStream();
      try {
        request({ uri: mediaManifestUri, gzip: true })
        .on('error', err => {
          reject(err);
        })
        .pipe(parser);
      } catch (exc) {
        reject(exc);
      }

      // List of all bandwidths from the VOD
      const vodBandwidths = Object.keys(this.mediaSeqSubset);
      let baseUrl = "";
      const m = mediaManifestUri.match(/^(.*)\/.*?$/);
      if (m) {
        baseUrl = m[1] + '/';
      }

      parser.on("m3u", m3u => {
        // BEFORE ANYTHING: Check if Live Source has created a new media sequence or not
        if (this.lastRequestedM3U8 && m3u.get("mediaSequence") === this.lastRequestedMediaseqRaw && liveTargetBandwidth === this.lastRequestedM3U8.bandwidth) {
          debug(`[${this.sessionId}]: # [What To Make?] Sending old manifest (Live Source does not have a new Mseq)`);
          resolve(this.lastRequestedM3U8);
          return;
        }
        debug(`[${this.sessionId}]: # Current RAW Mseq:  [${m3u.get("mediaSequence")}]`);
        debug(`[${this.sessionId}]: # Previous RAW Mseq: [${this.lastRequestedMediaseqRaw}]`);

        // -----------------------------------------------------------------
        // Top IF-statement is a...
        // TEMP FIX: issue when "current raw mseq" < "previous raw mseq",
        // fallback solution:  
        // -----------------------------------------------------------------
        if (this.lastRequestedM3U8 && m3u.get("mediaSequence") < this.lastRequestedMediaseqRaw) {
          debug(`[${this.sessionId}]: # [What To Make?] Odd case! Sending old manifest & copy segments from first bw to this requested bw.`);
                   
          // debug(`[${this.sessionId}]: # this.latestMediaSeqSegs[liveTargetBandwidth].length=${this.latestMediaSeqSegs[liveTargetBandwidth].length}`);
          // const copyBw = this._getFirstBwWithSegmentsInList(this.latestMediaSeqSegs);
          // this.latestMediaSeqSegs[liveTargetBandwidth] = this.latestMediaSeqSegs[copyBw];
          // debug(`[${this.sessionId}]: # this.latestMediaSeqSegs[copyBw].length=${this.latestMediaSeqSegs[copyBw].length}`);
          // debug(`[${this.sessionId}]: # this.latestMediaSeqSegs[liveTargetBandwidth].length=${this.latestMediaSeqSegs[liveTargetBandwidth].length}`);
          
          // Return with old...
          reject();
          return;
        } else if (this.lastRequestedM3U8 && m3u.get("mediaSequence") === this.lastRequestedMediaseqRaw && liveTargetBandwidth !== this.lastRequestedM3U8.bandwidth) {
          // New sequence and/or New Bandwidth
          // If they are the same sequence but different bw, do not pop! rebuild the manifest with all parts.
          debug(`[${this.sessionId}]: # [What To Make?] Creating An Identical Media Sequence, but for new Bandwidth!`);
          RECREATE_MSEQ = true;
        }
        else {
          CREATE_NEW_MSEQ = true;
          // In case this Next sequence is expected to be in a different profile,
          // then we need to use the recreate code.
          if (this.lastRequestedM3U8 && liveTargetBandwidth === this.lastRequestedM3U8.bandwidth) {
            RECREATE_MSEQ = false;
          }
          else {
            RECREATE_MSEQ = true;
          }

          // Increase Discontinuity count if top segment is a discontinuity segment.
          if (this.mediaSeqSubset[vodBandwidths[0]].length != 0 && this.mediaSeqSubset[vodBandwidths[0]][0].discontinuity) {
            this.discSeqCount++;
          }
          // Set raw diff
          RAW_mseq_diff = this.lastRequestedM3U8 ? m3u.get("mediaSequence") - this.lastRequestedMediaseqRaw : 1;
          this.lastRequestedMediaseqRaw  = m3u.get("mediaSequence");
          this.targetDuration = m3u.get("targetDuration");

          // Dequeue and increase mediaSeqCount
          for (let j = 0; j < RAW_mseq_diff; j++) {
            // Shift the top vod segment.
            for (let i = 0; i < vodBandwidths.length; i++) {
              this.mediaSeqSubset[vodBandwidths[i]].shift();
            }
            // LASTLY: Do we need to dequeue the queue?
            if (this.lastRequestedM3U8 && this.mediaSeqSubset[vodBandwidths[0]].length === 0 || this.liveSegQueue.length > this.targetNumSeg) {
              this.liveSegQueue.shift();
            }
            this.mediaSeqCount++;
          }
          debug(`[${this.sessionId}]: # [What To Make?] Creating a Completely New Media Sequence`);
          debug(`[${this.sessionId}]: # Time to make MEDIA-SEQUENCE number: [${this.mediaSeqCount}]`);
        }
        // Switch out relative URLs if they are used, with absolute URLs
        if (mediaManifestUri) {
          if (!RECREATE_MSEQ || this.liveSegQueue.length === 0) {
            // CASE: Live source is more than 1 sequence ahead push all "new" segments
            const startIdx = m3u.items.PlaylistItem.length - RAW_mseq_diff;
            for (let i = startIdx;  i < m3u.items.PlaylistItem.length; i++) {
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
                debug(`[${this.sessionId}]: # A | PUSHED this segment to the QUEUE:${JSON.stringify(seg)}`);
                this.liveSegQueue.push(seg);
              } else {
                debug(`[${this.sessionId}]: # A | PUSHED a DISCONTINUITY tag to the QUEUE`);
                this.liveSegQueue.push({ discontinuity: true });
              }
            }
          }
          else {
            /*
              -----------------
              SPECIAL TREATMENT
              -----------------
              Here we clear out liveSegQueue and add multiple live segments.
            */
            const queueLength = this.liveSegQueue.length;
            const liveLength = m3u.items.PlaylistItem.length;

            if (queueLength >= liveLength) {
              // Then pop liveLength times, then append ALL segments from live
              // Create a liveLength sized whole in the queue
              for (let i = 0;  i < liveLength; i++) {
                this.liveSegQueue.pop();
              }
              debug(`[${this.sessionId}]: # Emptied a part of the QUEUE!`);
              // Iterate from 0 to end, and push segment
              for (let i = 0;  i < liveLength; i++) {
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
                  seg["uri"] = segmentUri ;
                  debug(`[${this.sessionId}]: # B | PUSHED this segment to the QUEUE:${JSON.stringify(seg)}`);
                  this.liveSegQueue.push(seg);
                } else {
                  debug(`[${this.sessionId}]: # B | PUSHED a DISCONTINUITY tag to the QUEUE`);
                  this.liveSegQueue.push({ discontinuity: true });
                }
              }
            }
            else { // (If queueLen < liveLength)
              // Empty the queue, then append queue.length many segments from live
              this.liveSegQueue = [];
              debug(`[${this.sessionId}]: # Emptied the QUEUE!`);
              // Since we might have POPPED above, compensate now
              let size = queueLength;
              if (CREATE_NEW_MSEQ) {
                size += RAW_mseq_diff;
              }
              // Iterate backwards from END to (end - queue.length), and unshift segment
              for (let i = 1;  i <= size; i++) {
                let seg = {};
                let playlistItem = m3u.items.PlaylistItem[liveLength - i];
                let segmentUri;
                if (!playlistItem.properties.discontinuity) {
                  if (playlistItem.properties.uri.match('^http')) {
                    segmentUri = playlistItem.properties.uri;
                  } else {
                    segmentUri = url.resolve(baseUrl, playlistItem.properties.uri);
                  }
                  seg["duration"] = playlistItem.properties.duration;
                  seg["uri"] = segmentUri;
                  //debug(`[${this.sessionId}]: # C | PUSHED this segment to the QUEUE: ${JSON.stringify(seg)}`);
                  this.liveSegQueue.unshift(seg);
                } else {
                  //debug(`[${this.sessionId}]: # C | PUSHED a DISCONTINUITY tag to the QUEUE`);
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
        for (let i = 0; i < this.mediaSeqSubset[vodTargetBandwidth].length; i++) {
          let vodSeg = this.mediaSeqSubset[vodTargetBandwidth][i];
          // Get max duration amongst segments
          if (vodSeg.duration > this.targetDuration) {
            this.targetDuration = Math.round(vodSeg.duration);
          }
        }

        debug(`[${this.sessionId}]: ...Start Generating the Manifest`);
        let m3u8 = "#EXTM3U\n";
        m3u8 += "#EXT-X-VERSION:6\n";
        m3u8 += "# Transitioning from VOD -> Live stream (a SessionLive Manifest)\n" // put header here
        m3u8 += "#EXT-X-INDEPENDENT-SEGMENTS\n";
        m3u8 += "#EXT-X-TARGETDURATION:" + this.targetDuration + "\n";
        m3u8 += "#EXT-X-MEDIA-SEQUENCE:" + this.mediaSeqCount + "\n";
        m3u8 += "#EXT-X-DISCONTINUITY-SEQUENCE:" + this.discSeqCount + "\n";

        if (vodBandwidths.length !== 0) {
          debug(`[${this.sessionId}]: # Adding a Total of (${this.mediaSeqSubset[vodTargetBandwidth].length}) VOD segments to Manifest`);
          for (let i = 0; i < this.mediaSeqSubset[vodTargetBandwidth].length; i++) {
            let vodSeg = this.mediaSeqSubset[vodTargetBandwidth][i];
            this.latestMediaSeqSegs[liveTargetBandwidth].push(vodSeg);
            // Get max duration amongst segments
            if (!vodSeg.discontinuity) {
              m3u8 += "#EXTINF:" + vodSeg.duration.toFixed(3) + ",\n";
              m3u8 += vodSeg.uri + "\n";
            } else {
              m3u8 += "#EXT-X-DISCONTINUITY\n";
            }
          }
          debug(`[${this.sessionId}]: # Appending Segments from Live Source. Segment QUEUE is [ ${this.liveSegQueue.length} ] large`);
          for (let i = 0; i < this.liveSegQueue.length; i++) {
            const live_seg = this.liveSegQueue[i];
            this.latestMediaSeqSegs[liveTargetBandwidth].push(live_seg);
            m3u8 += "#EXTINF:" + live_seg.duration.toFixed(3) + ",\n";
            m3u8 += live_seg.uri + "\n";
          }
        }
        const latestM3U8 = {
          bandwidth: liveTargetBandwidth,
          m3u8: m3u8,
        };
        debug(`{${this.sessionId}]: Manifest Generation Complete!`);
        resolve(latestM3U8);
      });
      parser.on("error", err => {
        reject(err);
      });
    });
  }

  
  _getFirstBwWithSegmentsInList(allSegments) {
    const bandwidths = Object.keys(allSegments)
    for (let i = 0; i < bandwidths; i++) {
      let bw = bandwidths[i];
      if (allSegments[bw].length > 0) {
        return bw;
      } 
      else {
        console.log('ERROR: could not find any bw with segments');
        return null;
      }
    }
  }

}

module.exports = SessionLive;
