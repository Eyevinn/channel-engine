const crypto = require('crypto');
const request = require('request');
const debug = require('debug')('engine-session');
const HLSVod = require('vod-to-live.js');
const AdRequest = require('./ad_request.js');

const SessionState = Object.freeze({
  VOD_INIT: 1,
  VOD_PLAYING: 2,
  VOD_NEXT_INIT: 3,
  VOD_NEXT_INITIATING: 4,
});

class Session {
  constructor(assetMgrUri, adCopyMgrUri, adXchangeUri, playlist, startWithId) {
    this._assetMgrUri = assetMgrUri;
    this._adCopyMgrUri = adCopyMgrUri;
    this._adXchangeUri = adXchangeUri;
    this._playlist = playlist;
    this._sessionId = crypto.randomBytes(20).toString('hex');
    this._state = {
      mediaSeq: 0,
      discSeq: 0,
      vodMediaSeq: 0,
      state: SessionState.VOD_INIT,
      lastM3u8: null,
      playlistPosition: 0,
      tsLastRequest: null,
    };
    this.currentVod;
    this.currentMetadata = {};
    this._events = [];
    if (startWithId) {
      this._state.state = SessionState.VOD_INIT_BY_ID;
      this._state.assetId = startWithId;
    }
  }

  get sessionId() {
    return this._sessionId;
  }

  get currentPlaylist() {
    return this._playlist;
  }

  getMediaManifest(bw) {
    return new Promise((resolve, reject) => {
      this._tick().then(() => {
        if (this._state.state === SessionState.VOD_NEXT_INITIATING) {
          // Serve from cache
          this._state.state = SessionState.VOD_PLAYING;
          debug(`[${this._sessionId}]: serving m3u8 from cache`);
          resolve(this._state.lastM3u8);
        } else {
          const m3u8 = this.currentVod.getLiveMediaSequences(this._state.mediaSeq, bw, this._state.vodMediaSeq, this._state.discSeq);
          debug(`[${this._sessionId}]: bandwidth=${bw} vodMediaSeq=${this._state.vodMediaSeq}`);
          this._state.lastM3u8 = m3u8;
          if (this._state.tsLastRequest != null && (Date.now() - this._state.tsLastRequest) < 3000) {
            debug(`Last request less than 3 seconds ago, not increasing mediaseq counter`)
          } else {
            this._state.vodMediaSeq++;
          }
          this._state.tsLastRequest = Date.now();
          resolve(m3u8);
        }
      }).catch(reject);
    });
  }

  getMasterManifest() {
    return new Promise((resolve, reject) => {
      this._tick().then(() => {
        let m3u8 = "#EXTM3U\n";
        m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.session.id",VALUE="${this._sessionId}"\n`;
        m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.eventstream",VALUE="/eventstream/${this._sessionId}"\n`;
        this.currentVod.getUsageProfiles().forEach(profile => {
          m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + ',RESOLUTION=' + profile.resolution + ',CODECS="' + profile.codecs + '"\n';
          m3u8 += "master" + profile.bw + ".m3u8;session=" + this._sessionId + "\n";
        });
        this.produceEvent({
          type: 'NOW_PLAYING',
          data: {
            id: this.currentMetadata.id,
            title: this.currentMetadata.title,
          }
        });
        this._state.tsLastRequest = Date.now();
        resolve(m3u8);
      }).catch(reject);
    });
  }

  consumeEvent() {
    return this._events.shift();
  }

  produceEvent(event) {
    this._events.push(event);
  }

  _tick() {
    return new Promise((resolve, reject) => {
      // State machine
      let newVod;
      let splices = null;

      switch(this._state.state) {
        case SessionState.VOD_INIT:
        case SessionState.VOD_INIT_BY_ID:
          let nextVodPromise;
          if (this._state.state === SessionState.VOD_INIT) {
            debug(`[${this._sessionId}]: state=VOD_INIT`);
            nextVodPromise = this._getNextVod();
          } else if (this._state.state === SessionState.VOD_INIT_BY_ID) {
            debug(`[${this._sessionId}]: state=VOD_INIT_BY_ID ${this._state.assetId}`);
            nextVodPromise = this._getNextVodById(this._state.assetId);
          }
          nextVodPromise.then(uri => {
            debug(`[${this._sessionId}]: got first VOD uri=${uri}`);
            //newVod = new HLSVod(uri, [], Date.now());
            newVod = new HLSVod(uri, []);
            this.currentVod = newVod;
            return this.currentVod.load();
          }).then(() => {
            debug(`[${this._sessionId}]: first VOD loaded`);
            debug(newVod);
            this._state.vodMediaSeq = this.currentVod.getLiveMediaSequencesCount() - 5;
            if (this._state.vodMediaSeq < 0 || this._playlist !== 'random' || this._state.state === SessionState.VOD_INIT_BY_ID) {
              this._state.vodMediaSeq = 0;
            }
            this.produceEvent({
              type: 'NOW_PLAYING',
              data: {
                id: this.currentMetadata.id,
                title: this.currentMetadata.title,
              }
            });
            this._state.state = SessionState.VOD_PLAYING;
            resolve();
          }).catch(reject);
          break;
        case SessionState.VOD_PLAYING:
          debug(`[${this._sessionId}]: state=VOD_PLAYING (${this._state.vodMediaSeq}, ${this.currentVod.getLiveMediaSequencesCount()})`);
          if (this._state.vodMediaSeq === this.currentVod.getLiveMediaSequencesCount() - 1) {
            this._state.state = SessionState.VOD_NEXT_INIT;
          }
          resolve();
          break;
        case SessionState.VOD_NEXT_INITIATING:
          debug(`[${this._sessionId}]: state=VOD_NEXT_INITIATING`);
          resolve();
          break;
        case SessionState.VOD_NEXT_INIT:
          debug(`[${this._sessionId}]: state=VOD_NEXT_INIT`);
          const length = this.currentVod.getLiveMediaSequencesCount();
          const lastDiscontinuity = this.currentVod.getLastDiscontinuity();
          this._state.state = SessionState.VOD_NEXT_INITIATING;
          let vodPromise;
          if (this._adCopyMgrUri) {
            const adRequest = new AdRequest(this._adCopyMgrUri, this._adXchangeUri);
            vodPromise = new Promise((resolve, reject) => {
              adRequest.resolve().then(_splices => {
                debug(`[${this._sessionId}]: got splices=${_splices.length}`);
                if (_splices.length > 0) {
                  splices = _splices;
                  debug(splices);
                }
                return this._getNextVod();
              }).then(resolve);
            });
          } else {
            vodPromise = this._getNextVod();
          }

          vodPromise.then(uri => {
            debug(`[${this._sessionId}]: got next VOD uri=${uri}`);
            newVod = new HLSVod(uri, splices);
            this.produceEvent({
              type: 'NEXT_VOD_SELECTED',
              data: {
                id: this.currentMetadata.id,
                uri: uri,
                title: this.currentMetadata.title || '',
              }
            });
            return newVod.loadAfter(this.currentVod);
          }).then(() => {
            debug(`[${this._sessionId}]: next VOD loaded`);
            debug(newVod);
            this.currentVod = newVod;
            debug(`[${this._sessionId}]: msequences=${this.currentVod.getLiveMediaSequencesCount()}`);
            this._state.vodMediaSeq = 0;
            this._state.mediaSeq += length;
            this._state.discSeq += lastDiscontinuity;
            this.produceEvent({
              type: 'NOW_PLAYING',
              data: {
                id: this.currentMetadata.id,
                title: this.currentMetadata.title,
              }
            });            
            resolve();
          }).catch(reject);
          break;
        default:
          reject("Invalid state: " + this.state.state);
      }

    });
  }

  _getNextVod() {
    return new Promise((resolve, reject) => {
      this._state.playlistPosition++;
      const nextVodUri = this._assetMgrUri + '/nextVod/' + this._playlist + '?position=' + this._state.playlistPosition;
      request.get(nextVodUri, (err, resp, body) => {
        const data = JSON.parse(body);
        if (data.playlistPosition !== undefined) {
          this._state.playlistPosition = data.playlistPosition;
        }
        debug(`[${this._sessionId}]: nextVod=${data.uri} new position=${this._state.playlistPosition}`);
        debug(data);
        this.currentMetadata = {
          id: data.id,
          title: data.title || '',
        };
        resolve(data.uri);
      }).on('error', err => {
        reject(err);
      });
    });
  }

  _getNextVodById(id) {
    return new Promise((resolve, reject) => {
      const assetUri = this._assetMgrUri + '/vod/' + id;
      request.get(assetUri, (err, resp, body) => {
        const data = JSON.parse(body);
        this._state.playlistPosition = 0;
        debug(`[${this._sessionId}]: nextVod=${data.uri} new position=${this._state.playlistPosition}`)
        debug(data);
        this.currentMetadata = {
          id: data.id,
          title: data.title || '',
        };
        resolve(data.uri);
      }).on('error', err => {
        reject(err);
      });
    });
  }

  _getNearestBandwidth(bandwidth) {
    const availableBandwidths = this.currentVod.getBandwidths().sort((a,b) => b - a);
    for (let i = 0; i < availableBandwidths.length; i++) {
      if (bandwidth >= availableBandwidths[i]) {
        return availableBandwidths[i];
      }
    }
    return availableBandwidths[availableBandwidths.length - 1];
  }
}

module.exports = Session;
