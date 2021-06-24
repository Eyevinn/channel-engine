const debug = require('debug')('engine-sessionLive');
const m3u8 = require('@eyevinn/m3u8');
const request = require('request');
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
    this.use_demuxed_audio = false;
    this.cloudWatchLogging = false;
    this.currentMetadata = {};
    this.lastRequestedRawM3U8 = null;
    this.lastRequestedM3U8 = null;
    this.mediaSeqSubset = {}
    this.allMediaManifestUri = null;
    this.allBandwidths = null;
    this.lastRequestedSegments = {};

    if (config && config.sessionId) {
      this.sessionId = config.sessionId;
    }
    if (config && config.useDemuxedAudio) {
      this.use_demuxed_audio = true;
    }
    if (config && config.cloudWatchMetrics) {
      this.cloudWatchLogging = true;
    }
  }

  // not yet tested
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
    this.mediaSeqSubset = {}
    this.allMediaManifestUri = null;
    this.allBandwidths = null;
    this.lastRequestedSegments = {};
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
   * };
   */
  async setCurrentMediaSequenceSegments(segments) {
    this.lastMediaSeq = segments;
    const allBws = Object.keys(segments); 
    for (let i = 0; i < allBws.length; i++){
      const bw = allBws[i];
      if (!this.mediaSeqSubset[bw]) {
          this.mediaSeqSubset[bw] = [];
      }
      const segLen = segments[bw].length;
      for (let segIdx = 0; segIdx < segLen; segIdx++){
        this.mediaSeqSubset[bw].push(segments[bw][segIdx]);
      }
      this.mediaSeqSubset[bw].push({ discontinuity: true })
    }
  }

  // To hand off data to normal session
  async getCurrentMediaSequenceSegments() {
    debug(`# GETTING the current mseq segments from Live Session`);
    // Get desired media manifest using baseURL
    await this._getSegmentsFromLiveManifest();
    const segments = this.lastRequestedSegments;
    await this._resetSession();
    return segments;
  }

  // Switcher will call this. To use data from normal session
  async setCurrentMediaAndDiscSequenceCount(mseq, dseq) {
    debug(`# SETTING this.mediaSeqCount AND this.discSeqCount to -> [${mseq}]:[${dseq}]`);
    this.mediaSeqCount = mseq;
    this.discSeqCount = dseq;
  }

  // To hand off data to normal session
  async getCurrentMediaAndDiscSequenceCount() {
    const mseqCount = this.lastRequestedM3U8.get("mediaSequence");
    const dseqCount = this.lastRequestedM3U8.get("discontinuitySequence");
    debug(`mseqCount: ${mseqCount}`);
    debug(`dseqCount: ${dseqCount}`);
    return {
      "mediaSeq": mseqCount,
      "discSeq": dseqCount
    };
  }

  // To give manifest to client
  async getCurrentMediaManifestAsync(bw) {
    debug(`Master manifest URI: ${this.masterManifestUri}`)
    await this._loadLive(bw);
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
  _loadLive(bw, injectMasterManifest, injectMediaManifest) {
    return new Promise((resolve, reject) => {
      const parser = m3u8.createStream();
      try {
        request({ uri: this.masterManifestUri, gzip: true })
        .on('error', err => {
          reject(err);
        })
        .pipe(parser)
      } catch (exc) {
        debug("Error: resetting live session");
        this._resetSession();
        reject(exc);
      }
      parser.on('m3u', m3u => {
        // Only rewrite if fetched live manifest is new
        if (this.lastRequestedRawM3U8 && m3u.get("mediaSequence") === this.lastRequestedRawM3U8.get("mediaSequence")) {
          resolve();
        }
        this.lastRequestedRawM3U8 = m3u;
        let baseUrl = "";
        let bandwidths = [];
        let allUris = [];
        let targetBandwidth = 0;
        const m = this.masterManifestUri.match(/^(.*)\/.*?$/);
        if (m) {
          baseUrl = m[1] + '/';
        }
        debug(`StreamItem.length: ${m3u.items.StreamItem.length}`);
        // Get all bandwidths from LiveMasterManifest
        for (let i = 0; i < m3u.items.StreamItem.length; i++) {
          bandwidths.push(m3u.items.StreamItem[i].get("bandwidth"));
          // Save paths to every profile in Live Manifest.
          allUris.push(url.resolve(baseUrl, m3u.items.StreamItem[i].get('uri')));
        }
        // Save these for when we go back to V2L
        if (!this.allMediaManifestUri) {
          this.allMediaManifestUri = allUris;
        }
        if (!this.allBandwidths) {
          this.allBandwidths = bandwidths;
        }
        targetBandwidth = this._findNearestBw(bw, bandwidths);
        // Get desired media manifest using baseURL
        for (let i = 0; i < m3u.items.StreamItem.length; i++) {
          const streamItem = m3u.items.StreamItem[i];
          if (streamItem.get('bandwidth') === targetBandwidth) {
            const mediaManifestUri = url.resolve(baseUrl, streamItem.get('uri'));
            debug(` We are going into _loadMediaManifest: [${mediaManifestUri}]:[${streamItem.get('bandwidth')}]`);
            this._loadMediaManifest(mediaManifestUri, bw, injectMediaManifest)
            .then(resolve)
            .catch(reject);
          }
        }
      });
      parser.on('error', err => {
        debug("Error: resetting live session");
        this._resetSession();
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
        debug("Error: resetting live session");
        this._resetSession();
        reject(exc);
      }
      // Extract updated BaseURL
      let baseUrl = "";
      const m = mediaManifestUri.match(/^(.*)\/.*?$/);
      if (m) {
        baseUrl = m[1] + '/';
      }
      parser.on('m3u', m3u => {
        // Switch out relative urls with absolute urls
        if (mediaManifestUri) {
          for (let i = 0; i <  m3u.items.PlaylistItem.length; i++) {
            let playlistItem = m3u.items.PlaylistItem[i];
            let segmentUri;
            if (playlistItem.properties.uri.match('^http')) {
              segmentUri = playlistItem.properties.uri;
            } else {
              segmentUri = url.resolve(baseUrl, playlistItem.properties.uri);
            }
            playlistItem.set('uri', segmentUri);
            debug(`Media URI:${segmentUri}`);
          }
        }
        // SAVE new RAW playlistItems #deep
        // Hack to make a deep copy
        const items = JSON.parse(JSON.stringify(m3u.items.PlaylistItem));
        const bandwidths = Object.keys(this.mediaSeqSubset);
        if (bandwidths.length !== 0) {
          const targetBandwidth = this._findNearestBw(bw, bandwidths);
          debug(`-------------------------- BANDWIDTHS: ${targetBandwidth}`);
          // Manipulate the Manifest: Inject VOD segments
          debug(`# this.mediaSeqSubset[bandwidth].length ->: ${this.mediaSeqSubset[targetBandwidth].length}`);
          for (let i = 0; i < this.mediaSeqSubset[targetBandwidth].length; i++) {
            let vodSeg = this.mediaSeqSubset[targetBandwidth][i];
            // Get max duration amongst segments
            if (vodSeg.duration > this.targetDuration) {
              this.targetDuration = vodSeg.duration;
            }
            if (!vodSeg.discontinuity) {
              // Set or Add actual playlist Item
              if (i < m3u.items.PlaylistItem.length) {
                m3u.items.PlaylistItem[i].set("duration", vodSeg.duration);
                m3u.items.PlaylistItem[i].set("uri", vodSeg.uri);
              } else {
                m3u.addPlaylistItem({
                  duration: vodSeg.duration,
                  uri: vodSeg.uri
                });
              }
            }
            else {
              if (i < m3u.items.PlaylistItem.length) {
                m3u.items.PlaylistItem[i].set("discontinuity", true);
              } else {
                m3u.addPlaylistItem({
                  discontinuity: true,
                });
              }
            }
          }
          // original -> ori&newUri -> save ori&newUri -> rewrite ori with vodSeg -> append ori&newUri
          // Append new RAW playlistItems
          for (let i = 0; i < items.length; i++){
            let item = items[i];
            m3u.addPlaylistItem({
              duration: item.properties.duration,
              uri: item.properties.uri
            });
          }

          // Pop and update the mediaSeqSubset for all variants.
          for (let i = 0; i < bandwidths.length; i++){
            this.mediaSeqSubset[bandwidths[i]].shift();
          }
        }
        // Manipulate the Manifest: Change Counts in Headers.
        m3u.set("mediaSequence", this.mediaSeqCount++);
        debug(`# SETTING THE MEDIA-SEQUENCE COUNT: ${this.mediaSeqCount}`);
        debug(`# TOP PL-ITEM: ${m3u.items.PlaylistItem[0]}`);
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
        debug("Error: resetting live session");
        this._resetSession();
        reject(err);
      });
    });
  }

  _getSegmentsFromLiveManifest() {
    return new Promise((resolve, reject) => {
      let collectSegmentsPromises = [];
      for (let i = 0; i < this.allMediaManifestUri.length; i++) {
        let a_promise = new Promise((resolve, reject) => {
          const parser = m3u8.createStream();
          let mediaManifestUri = this.allMediaManifestUri[i];
          try {
            request({ uri: mediaManifestUri, gzip: true })
            .on('error', err => {
              reject(err);
            })
            .pipe(parser)
          } catch (exc) {
            debug("Error: resetting live session");
            this._resetSession();
            reject(exc);
          }
          // Extract updated BaseURL
          let baseUrl = "";
          const m = mediaManifestUri.match(/^(.*)\/.*?$/);
          if (m) {
            baseUrl = m[1] + '/';
          }
          parser.on('m3u', m3u => {
            // Switch out relative urls with absolute urls
            for (let i = 0; i <  m3u.items.PlaylistItem.length; i++) {
              let playlistItem = m3u.items.PlaylistItem[i];
              let segmentUri = null;
              let segmentDuration = 0;

              if (!this.lastRequestedSegments[this.allBandwidths[i]]) {
                this.lastRequestedSegments[this.allBandwidths[i]] = [];
              }
              // PUSH disc. object OR segment object?
              if (playlistItem.properties.discontinuity) {
                this.lastRequestedSegments[this.allBandwidths[i]].push({ "discontinuity": true });
              } else {
                playlistItem.properties.duration;
                if (playlistItem.properties.uri.match('^http')) {
                  segmentUri = playlistItem.properties.uri;
                } else {
                  segmentUri = url.resolve(baseUrl, playlistItem.properties.uri);
                }
                this.lastRequestedSegments[this.allBandwidths[i]].push({ "duration": segmentDuration, "uri": segmentUri });
              }
            }
            resolve();
          });
          parser.on('error', err => {
            debug("Error: resetting live session");
            this._resetSession();
            reject(err);
          });
        });
        collectSegmentsPromises.push(a_promise);
      }
      Promise.all(collectSegmentsPromises)
      .then(resolve)
      .catch(err => {
        debug("Error: resetting live session");
        this._resetSession();
        reject(err);
      });
    });
  }
}

module.exports = SessionLive;