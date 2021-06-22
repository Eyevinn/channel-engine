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
  async resetSession() {
    this.mediaSeqCount = 0;
    this.discSeqCount = 0;
    this.lastMediaSeq = {}
    this.events = [];
    this.targetDuration = 0;
    this.masterManifestUri = null;
    this.currentMetadata = {};
    this.lastRequestedRawM3U8 = null;
    this.lastRequestedM3U8 = null;
    this.mediaSeqSubset = {}
  }

  async setLiveUri(liveUri) {
    this.masterManifestUri = liveUri;
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
      }
      const segLen = segments[bw].length;
      for (let segIdx = segLen - DEFAULT_MAX_SEG; segIdx < segLen; segIdx++){
        this.mediaSeqSubset[bw].push(segments[bw][segIdx]);
      }
      this.mediaSeqSubset[bw].push({ discontinuity: true })
    }
  }

  // To hand off data to normal session
  async getCurrentMediaSequenceSegments() {

  }

  // Switcher will call this. To use data from normal session
  async setCurrentMediaAndDiscSequenceCount(mseq, dseq) {
    debug(`#SETTING this.mediaSeqCount AND this.discSeqCount to -> [${mseq}]:[${dseq}]`);
    this.mediaSeqCount = mseq;
    this.discSeqCount = dseq;
  }

  // To hand off data to normal session
  async getCurrentMediaAndDiscSequenceCount() {

  }

  // To give manifest to client
  async getCurrentMediaManifestAsync(bw) {
    debug("Trying to fetch live manifest");
    debug(`MASTER MANIFEST URI: ${this.masterManifestUri}`)
    await this.loadLive(bw);
    debug("Got live manifest");
    return this.lastRequestedM3U8.toString();
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

  // step 1: fetch live-master manifest using the this.liveUri.
  // step 2: Find nearest bw in master manifest.
  // step 3: fetch live-profile manifest using uri found in step 1's master manifest.
  // step 4: rewrite stuff in the live-profile manifest (mseq, dseq, seg urls, target duration).
  loadLive(bw, injectMasterManifest, injectMediaManifest) {
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
      let manifestStream = new Readable();
      //manifestStream.push(manifest);
      manifestStream.push(null);
      //manifestStream.pipe(parser);
      parser.on('m3u', m3u => {
        // Only rewrite if fetched live manifest is new
        if (this.lastRequestedRawM3U8 && m3u.get("mediaSequence") === this.lastRequestedRawM3U8.get("mediaSequence")) {
          // Skip manifest manipulation step
          resolve();
        }
        this.lastRequestedRawM3U8 = m3u;
        let baseUrl = "";
        let bandwidths = [];
        let targetBandwidth = 0;
        const m = this.masterManifestUri.match(/^(.*)\/.*?$/);
        if (m) {
          baseUrl = m[1] + '/';
        }
        debug(`StreamItem.length: ${m3u.items.StreamItem.length}`);
        // Get all bandwidths from LiveMasterManifest
        for (let i = 0; i < m3u.items.StreamItem.length; i++) {
          bandwidths.push(m3u.items.StreamItem[i].get("bandwidth"));
        }
        // What bandwidth is closest to the desired bw
        targetBandwidth = this._findNearestBw(bw, bandwidths);
        // Get desired media manifest using baseURL
        for (let i = 0; i < m3u.items.StreamItem.length; i++) {
          const streamItem = m3u.items.StreamItem[i];
          if (streamItem.get('bandwidth') === targetBandwidth) {
            const mediaManifestUri = url.resolve(baseUrl, streamItem.get('uri'));
            debug(` We are going into _loadMediaManifest: [${mediaManifestUri}]:[${streamItem.get('bandwidth')}]`);
            this._loadMediaManifest(mediaManifestUri, bw, injectMediaManifest)
            .then(resolve)
            .catch(reject)
          }
        }
      });
      parser.on('error', err => {
        reject(err);
      });
    });
  }

  _loadMediaManifest(mediaManifestUri, bw, injectMediaManifest) {
    return new Promise((resolve, reject) => {
      const parser = m3u8.createStream();
      try {
        request({ uri: mediaManifestUri, gzip: true })
        .on('error', err => {
          reject(err);
        })
        .pipe(parser)
      } catch (exc) {
        reject(exc);
      }
      let manifestStream = new Readable();
      //manifestStream.push(manifest);
      manifestStream.push(null);
      parser.on('m3u', m3u => {
        // Switch out relative urls with absolute urls
        if (mediaManifestUri) {
          debug("We have a mediamanifest uri");
          for (let i = 0; i <  m3u.items.PlaylistItem.length; i++) {
            let playlistItem = m3u.items.PlaylistItem[i];
            let segmentUri;
            if (playlistItem.properties.uri.match('^http')) {
              segmentUri = playlistItem.properties.uri;
            } else {
              segmentUri = url.resolve(baseUrl, playlistItem.properties.uri);
            }
            playlistItem.set('uri', segmentUri);
            debug(`MEDIA URI:${segmentUri}`);
          }
        }
        // -=MANIPULATE MANIFEST PART 1=- (until we don't need to do this anymore ;-) )
        // ANOTHER FIND NEAREST BW OPERATION
        const bandwidths = Object.keys(this.mediaSeqSubset);
        if(bandwidths.length !== 0){
          const targetBandwidth = this._findNearestBw(bw, bandwidths);

          debug(`### this.mediaSeqSubset[bandwidth].length ->: ${this.mediaSeqSubset[targetBandwidth].length}`);
          debug("Now we are Actually rewriting the manifest!");
          for (let i = 0; i < this.mediaSeqSubset[targetBandwidth].length; i++) {
            let vodSeg = this.mediaSeqSubset[targetBandwidth][i];
            // Get max duration amongst segments
            if(vodSeg.duration > this.targetDuration){
              this.targetDuration = vodSeg.duration;
            }
            if (!vodSeg.discontinuity) {
              m3u.items.PlaylistItem[i].set("duration", vodSeg.duration);
              m3u.items.PlaylistItem[i].set("uri", vodSeg.uri);
            }
            else {
              m3u.items.PlaylistItem[i].set("discontinuity", true);
            }
          }
          // Pop and update the mediaSeqSubset for all variants.
          for (let i = 0; i < bandwidths.length; i++){
            this.mediaSeqSubset[bandwidths[i]].shift();
          }
        }

        m3u.set("mediaSequence", this.mediaSeqCount++);
        debug(`# SETTING THE MEDIA-SEQUENCE COUNT: ${this.mediaSeqCount}`);
        debug(`---- # TOP PL-ITEM: ${m3u.items.PlaylistItem[0]}`);
        // Increment discontinuity sequence counter if top segment has the discontinuity tag
        if (m3u.items.PlaylistItem[0].properties.discontinuity) {
          m3u.set("EXT-X-DISCONTINUITY-SEQUENCE", this.discSeqCount);
          this.discSeqCount++;
        } else {
          m3u.set("EXT-X-DISCONTINUITY-SEQUENCE", this.discSeqCount);
        }
        debug(`# SETTING THE DISCONTINUITY-SEQUENCE COUNT: ${this.discSeqCount}`);
        const targetDuration = m3u.get('targetDuration');
        if (targetDuration > this.targetDuration) {
          this.targetDuration = targetDuration;
          debug(`TargetDuration is: ${this.targetDuration}`);
        }
        m3u.set('targetDuration', this.targetDuration);

        // Store and update the last manifest sent to client
        this.lastRequestedM3U8 = m3u;
        debug("Updated lastRequestedM3U8 with new data")
        resolve();
      });
      parser.on('error', err => {
        reject(err);
      });
    });
  }
/** BEFORE
 * ww.vv.com/bw/liveseg1.ts
 * liveseg2.ts
 * liveseg3.ts
 * liveseg4.ts
 * liveseg5.ts
 * liveseg6.ts
 */

/** AFTER
 *  DUR: 10
 *  vodseg5.ts
 *  DUR: 10
 *  vodseg6.ts
 *  DUR: 10
 *  vodseg7.ts
 *  discontinuity
 *  DUR: 2
 *  liveseg5.ts 
 */

/** AFTER
 *  vodseg5.ts <- player is playing
 *  vodseg6.ts
 *  discontinuity
 *  liveseg6.ts 
 *  liveseg7.ts
 */

}

module.exports = SessionLive;