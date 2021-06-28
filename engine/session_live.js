const crypto = require('crypto');
const debug = require('debug')('engine-sessionLive');
const HLSVod = require('@eyevinn/hls-vodtolive');
const m3u8 = require('@eyevinn/m3u8');
const HLSRepeatVod = require('@eyevinn/hls-repeat');
const HLSTruncateVod = require('@eyevinn/hls-truncate');
const Readable = require('stream').Readable;
const request = require('request');
const url = require('url');

const { SessionState } = require('./session_state.js');
const { PlayheadState } = require('./playhead_state.js');

const { applyFilter, cloudWatchLog, m3u8Header, logerror } = require('./util.js');
const ChaosMonkey = require('./chaos_monkey.js');

const AVERAGE_SEGMENT_DURATION = 3000;
const DEFAULT_PLAYHEAD_DIFF_THRESHOLD = 1000;
const DEFAULT_MAX_TICK_INTERVAL = 10000;
const DEFAULT_MAX_SEG = 3;

class SessionLive {
  constructor(config) {
    this.sessionId = 0;
    this.mediaSeqCount = 0;
    this.discSeqCount = 0;
    this.lastMediaSeq = {}
    this.events = [];
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.use_demuxed_audio = false;
    this.cloudWatchLogging = false;
    this.currentMetadata = {};
    this.lastRequestedRawM3U8 = null;
    this.lastRequestedM3U8 = null;
    this.mediaSeqSubset = {}
    this.mediaManifestURIs = {};
    this.liveSegQueue = [];
    this.last_requested_mediasequence_raw = 0;
    this.target_num_segs = 0;

    if (config) {
      if (config.sessionId) {
        this.sessionId = config.sessionId;
      }
      if (config.useDemuxedAudio) {
        this.use_demuxed_audio = true;
      }
      if (config.cloudWatchMetrics) {
        this.cloudWatchLogging = true;
      }
    }
  }

  // not tested
  async _resetSession() {
    this.mediaSeqCount = 0;
    this.discSeqCount = 0;
    this.lastMediaSeq = {}
    this.events = [];
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.currentMetadata = {};
    this.lastRequestedRawM3U8 = null;
    this.lastRequestedM3U8 = null;
    this.mediaSeqSubset = {},
    this.mediaManifestURIs = {};
    this.liveSegQueue = [];
    this.last_requested_mediasequence_raw = 0;
    this.target_num_segs = 0;
  }

  async setLiveUri(liveUri) {
    this.masterManifestUri = liveUri;
    // Get All media manifest inside the entered Master manifest.
    await this.loadMasterManifest();
  }

  async getLiveUri() {
    return this.masterManifestUri;
  }

  // Switcher will call this. To use data from normal session
  /**
   *  item in array -> { duration: 10, uri: http*** }
   * segments = {
   *  bw1: [{ }, { }, { } ... nth { }],
   *  bw2: [{ }, { }, { } ... nth { }],
   *  bw3: [{ }, { }, { } ... nth { }],
   * }
   */
  async setCurrentMediaSequenceSegments(segments) {
    this.lastMediaSeq = segments;
    const allBws = Object.keys(segments); 
    for (let i = 0; i < allBws.length; i++){
      const bw = allBws[i];
      if(!this.mediaSeqSubset[bw]){
          this.mediaSeqSubset[bw] = [];
          this.mediaSeqSubset[bw].push({ duration:1, uri: "dum dum" });
      }
      const segLen = segments[bw].length;
      for (let segIdx = segLen - segLen; segIdx < segLen; segIdx++){
        this.mediaSeqSubset[bw].push(segments[bw][segIdx]);
      }
      this.mediaSeqSubset[bw].push({ discontinuity: true })
    }
    this.target_num_segs = this.mediaSeqSubset[allBws[0]].length;
  }

  // To hand off data to normal session
  async getCurrentMediaSequenceSegments() {

  }

  // Switcher will call this. To use data from normal session
  async setCurrentMediaAndDiscSequenceCount(mseq, dseq) {
    debug(`#SETTING this.mediaSeqCount AND this.discSeqCount to -> [${mseq}]:[${dseq}]`);
    this.mediaSeqCount = mseq - 1;
    this.discSeqCount = dseq;
  }

  // To hand off data to normal session
  async getCurrentMediaAndDiscSequenceCount() {

  }

  // To give manifest to client
  async getCurrentMediaManifestAsync(bw) {
    debug(`# ...Loading selected Live Media Manifest`);
    await this._loadMediaManifest(bw);
    debug("# SENDING THE LIVE MANIFEST");
    return this.lastRequestedM3U8.m3u8;
  }

  // Work on this later :P
  async getCurrentAudioManifestAsync(audioGroupId, audioLanguage) {

  }

  _findNearestBw(bw, array) {
    const sorted = array.sort((a, b) => b - a);
    return sorted.reduce((a, b) => {
      return Math.abs(b - bw) < Math.abs(a - bw) ? b : a;
    });
  }

  /**
   * 
   * @returns Loads the uris to the different media playlists from the given master playlist
   */
  loadMasterManifest() {
    return new Promise((resolve, reject) => {
      const parser = m3u8.createStream();
      try {
        request({ uri: this.masterManifestUri, gzip: true })
        .on('error', err => {
          reject(err);
        })
        .pipe(parser)
      } catch (exc) {
        reject(exc);
      }
      parser.on('m3u', m3u => {

        debug(`New Live Master Manifest: ${this.masterManifestUri}`); 

        let baseUrl = "";
        const m = this.masterManifestUri.match(/^(.*)\/.*?$/);
        if (m) {
          baseUrl = m[1] + '/';
        }
        
        // Get all Profile manifest uris in the Live Master Manifest.
        for (let i = 0; i < m3u.items.StreamItem.length; i++) {
          const streamItem = m3u.items.StreamItem[i];
          const streamItemBW = streamItem.get("bandwidth");
          const mediaManifestUri = url.resolve(baseUrl, streamItem.get('uri'));
          if(!this.mediaManifestURIs[streamItemBW]){
            this.mediaManifestURIs[streamItemBW] = "";
          }
          this.mediaManifestURIs[streamItemBW] = mediaManifestUri;
        }
        debug(`[${this.sessionId}]: All Media Manifest URIs Collected. (${Object.keys(this.mediaManifestURIs).length}) profiles found!`);
        resolve();
      });
      parser.on('error', err => {
        reject(err);
      });
    });
  }

  _loadMediaManifest(bw) {
    return new Promise((resolve, reject) => {
      let createSameM3U8NewBW = true;
      debug(`# Trying to fetch live manifest for profile with bandwidth=${bw}`);
      // What bandwidth is closest to the desired bw
      const liveTargetBandwidth = this._findNearestBw(bw, Object.keys(this.mediaManifestURIs));
      debug(`# Nearest Bandwidth is: ${liveTargetBandwidth}`);
      // Get the target media manifest.
      const mediaManifestUri = this.mediaManifestURIs[liveTargetBandwidth]; 
      // LOAD a New Manifest
      const parser = m3u8.createStream();
      // ---- PING THE LIVE SOURCE
      try {
        request({ uri: mediaManifestUri, gzip: true })
        .on('error', err => {
          reject(err);
        })
        .pipe(parser)
      } catch (exc) {
        reject(exc);
      }
  
      // List of all bandwidths from the vod.
      const vodBandwidths = Object.keys(this.mediaSeqSubset);
      // In Case we need to extract the last segment.
      let lastLiveSegObj = {};
      // Extract updated BaseURL
      let baseUrl = "";
      const m = mediaManifestUri.match(/^(.*)\/.*?$/);
      if (m) {
        baseUrl = m[1] + '/';
      }

      parser.on('m3u', m3u => {
        // --BEFORE ANYTHING!-- Check if Live Source has made a new Media Sequence or not.
        if(this.lastRequestedM3U8 && m3u.get("mediaSequence") === this.last_requested_mediasequence_raw && liveTargetBandwidth === this.lastRequestedM3U8.bandwidth){
          debug("# +_+_+_+_+_+[What To Make?] No New Live Media Sequence... Sending old manifest");
          resolve();
          return;
        }

        debug(`# ¤¤¤¤ ¤¤¤¤ ¤¤¤¤ ¤¤¤¤ current RAW Mseq:[${m3u.get("mediaSequence")}] ____ previous RAW Mseq:[${this.last_requested_mediasequence_raw}] `);
        // NEW SEQUENCE and/or New Bandwidth
        if(this.lastRequestedM3U8 && m3u.get("mediaSequence") === this.last_requested_mediasequence_raw && liveTargetBandwidth !== this.lastRequestedM3U8.bandwidth){
          // If they are the same sequence but different, DON'T pop! rebuild the manifest with all parts.
          debug("# +_+_+_+_+_+[What To Make?] Creating Same Media Sequence, but for new Bandwidth! ");
          createSameM3U8NewBW = true;
        } else {
          // WE ARE BUILDING A COMPLETELY NEW MEDIA SEQUENCE
          // Finally, Pop and update the mediaSeqSubset for all variants.
          // Is what we are going to pop a disc-tag? Then increment discSeq. 
          // HEY What is stream has disc in it??
          if(this.mediaSeqSubset[vodBandwidths[0]].length != 0 && this.mediaSeqSubset[vodBandwidths[0]][0].discontinuity){
            this.discSeqCount++;
          }
          // Shift the top vod segment.
          for (let i = 0; i < vodBandwidths.length; i++) {
            this.mediaSeqSubset[vodBandwidths[i]].shift();
          }
          this.last_requested_mediasequence_raw  = m3u.get("mediaSequence");
          this.targetDuration = m3u.get("targetDuration");
          this.mediaSeqCount++;
          debug("# +_+_+_+_+_+[What To Make?] Creating Completely New Media Sequence");
          debug(`# ooOOOooOOOoooOOooOOOOooOOOoo Time to make MEDIA-SEQUENCE number ->: [${this.mediaSeqCount}] oooOOOooOOOOoooOOOOOOOOOooooo `)
        }

        // STEP 0: Enqueue to THE QUEUE the latest Live Segment 
        // -=URI SWAP WHERE IT IS NEEDED=-
        //-----
        // Switch out relative urls if they are used, with absolute urls
        if (mediaManifestUri) {
          if (!createSameM3U8NewBW) { // dumb variable name... work in progress
            let lastLivePlaylistItem = m3u.items.PlaylistItem[m3u.items.PlaylistItem.length - 1];
            let segmentUri;
            if(!lastLivePlaylistItem.properties.discontinuity){
              if (lastLivePlaylistItem.properties.uri.match('^http')) {
                segmentUri = lastLivePlaylistItem.properties.uri;
              } else {
                segmentUri = url.resolve(baseUrl, lastLivePlaylistItem.properties.uri);
              }
              lastLiveSegObj["duration"] = lastLivePlaylistItem.properties.duration;
              lastLiveSegObj["uri"] = segmentUri;
  
              debug(`# Should Be Last Segment:${JSON.stringify(lastLiveSegObj)}`);
              this.liveSegQueue.push(lastLiveSegObj);
            } else {
              debug(`# Should Be Last Segment is a DISCONTINUITY tag`);
              this.liveSegQueue.push({ discontinuity: true });
            }
          } 
          else {
            // -----------------
            // SPECIAL TREATMENT
            // -----------------
            const queueLength = this.liveSegQueue.length; // MAX BECOME VOD LENGTH
            const liveLength = m3u.items.PlaylistItem.length; // CAN BE SMALLER OR GREATER
            let start = Math.abs(liveLength - queueLength) - 1;
            if (start > liveLength) {
              start = liveLength - 1;
            }
            if (start < 0) {
              start = 0;
            }
            const timesToLoop = queueLength < liveLength ? queueLength : liveLength;
            // for (let i = 0; i < timesToLoop; i++) {
            //   this.liveSegQueue.pop();
            // }
            this.liveSegQueue = [];

            // Append new segments to the QUEUE.
            debug(`# \n\nSTART:${start} : TIMESTOLOOP: ${timesToLoop} QUEUELENGTH: ${queueLength}`);
            for (let i = start;  i < m3u.items.PlaylistItem.length; i++) {
              let seg = {};
              let lastLivePlaylistItem = m3u.items.PlaylistItem[i];
              let segmentUri = "";
              if(!lastLivePlaylistItem.properties.discontinuity){
                if (lastLivePlaylistItem.properties.uri.match('^http')) {
                  segmentUri = lastLivePlaylistItem.properties.uri;
                } else {
                  segmentUri = url.resolve(baseUrl, lastLivePlaylistItem.properties.uri);
                }
                seg["duration"] = lastLivePlaylistItem.properties.duration;
                seg["uri"] = segmentUri ;
    
                debug(`# Should Be Last Segment:${JSON.stringify(seg)}`);
                this.liveSegQueue.push(seg);
              } else {
                debug(`# Should Be Last Segment is a DISCONTINUITY tag`);
                this.liveSegQueue.push({ discontinuity: true });
              }
            }
          }
        }

        //------------------------------------------------------------
        // -=MANIPULATE MANIFEST PART 1=- (until no more vod segs)
        //------------------------------------------------------------

        // STEP1: New Target Duration?
        const vodTargetBandwidth = this._findNearestBw(bw, vodBandwidths);
        for (let i = 0; i < this.mediaSeqSubset[vodTargetBandwidth].length; i++) {
          let vodSeg = this.mediaSeqSubset[vodTargetBandwidth][i];
          // Get max duration amongst segments
          if(vodSeg.duration > this.targetDuration){
            this.targetDuration = vodSeg.duration;
          }
        }

        let m3u8 = "#EXTM3U\n";
        m3u8 += "#EXT-X-VERSION:6\n";
        m3u8 += "#EXT-X-INDEPENDENT-SEGMENTS\n";
        m3u8 += "#EXT-X-TARGETDURATION:" + this.targetDuration + "\n";
        m3u8 += "#EXT-X-MEDIA-SEQUENCE:" + this.mediaSeqCount + "\n";
        m3u8 += "#EXT-X-DISCONTINUITY-SEQUENCE:" + this.discSeqCount + "\n";

        // STEP2: Add all vod segments, including the disc-tag.
        if(vodBandwidths.length !== 0){
          debug(`### this.mediaSeqSubset[bandwidth].length ->: ${this.mediaSeqSubset[vodTargetBandwidth].length}`);
          debug("# Now we are actually rewriting the manifest!");
          for (let i = 0; i < this.mediaSeqSubset[vodTargetBandwidth].length; i++) {
            let vodSeg = this.mediaSeqSubset[vodTargetBandwidth][i];
            // Get max duration amongst segments
            if (!vodSeg.discontinuity) {
              m3u8 += "#EXTINF:" + vodSeg.duration.toFixed(3) + ",\n";
              m3u8 += vodSeg.uri + "\n";
            }
            else {
              m3u8 += "#EXT-X-DISCONTINUITY\n";
            }
          }

          // STEP3: Append the queue of Live Segments.
          debug("# ------------------------------------ Appending Current LiveManifest's Last Segment!"," QUEUE is [",this.liveSegQueue.length,"] large");
          debug(`# --- ADD THESE TO OUR MANIFEST -> ${JSON.stringify(this.liveSegQueue)}`);
          //m3u.addPlaylistItem(lastLiveSegObj_b4);
          for (let i = 0; i < this.liveSegQueue.length; i++) {
            
            const live_seg = this.liveSegQueue[i];
            m3u8 += "#EXTINF:" + live_seg.duration.toFixed(3) + ",\n";
            m3u8 += live_seg.uri + "\n";
          }

          // LASTLY: Do we need to dequeue the queue?
          if(this.liveSegQueue.length > this.target_num_segs){
            // DEQUEUE the Queue.
            this.liveSegQueue.shift();
          }
        }

        // Store and update the last manifest sent to client
        if (!this.lastRequestedM3U8) {
          this.lastRequestedM3U8 = { bandwidth: null, m3u8: null };
        }
        this.lastRequestedM3U8.bandwidth = liveTargetBandwidth;
        this.lastRequestedM3U8.m3u8 = m3u8;
        debug("Updated lastRequestedM3U8 with new data");
        resolve();
      }); // parser.on(...)
      parser.on('error', err => {
        reject(err);
      });
    });// the return promise
  }// the function 
}

module.exports = SessionLive;