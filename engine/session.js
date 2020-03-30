const crypto = require('crypto');
const debug = require('debug')('engine-session');
const HLSVod = require('vod-to-live.js');
const AdRequest = require('./ad_request.js');
const m3u8 = require('@eyevinn/m3u8');
const Readable = require('stream').Readable;

const SessionState = Object.freeze({
  VOD_INIT: 1,
  VOD_PLAYING: 2,
  VOD_NEXT_INIT: 3,
  VOD_NEXT_INITIATING: 4,
});

const PlayheadState = Object.freeze({
  RUNNING: 1,
  STOPPED: 2,
  CRASHED: 3,
  IDLE: 4
});

const AVERAGE_SEGMENT_DURATION = 3000;

const timer = ms => new Promise(res => setTimeout(res, ms));

class Session {
  /**
   * 
   * config: {
   *   startWithId,
   * }
   * 
   */
  constructor(assetManager, config) {
    this._assetManager = assetManager;
    this._sessionId = crypto.randomBytes(20).toString('hex');
    this._state = {
      mediaSeq: 0,
      discSeq: 0,
      vodMediaSeq: {
        video: 0,
        audio: 0, // assume only one audio group now
      },
      state: SessionState.VOD_INIT,
      lastM3u8: {},
      tsLastRequest: {
        video: null,
        master: null,
        audio: null
      },
      playhead: {
        state: PlayheadState.IDLE,
      }
    };
    this.currentVod;
    this.currentMetadata = {};
    this._events = [];
    this.averageSegmentDuration = AVERAGE_SEGMENT_DURATION;
    this.use_demuxed_audio = false;
    if (config) { 
      if (config.sessionId) {
        this._sessionId = config.sessionId;
      }
      if (config.startWithId) {
        this._state.state = SessionState.VOD_INIT_BY_ID;
        this._state.assetId = config.startWithId;
      }
      if (config.category) {
        this._category = config.category;
      }
      if (config.averageSegmentDuration) {
        this.averageSegmentDuration = config.averageSegmentDuration;
      }
      if (config.useDemuxedAudio) {
        this.use_demuxed_audio = true;
      }
    }
  }

  get sessionId() {
    return this._sessionId;
  }

  startPlayhead() {
    const loop = () => {
      return this.increment()
      .then(manifest => {
        if ([SessionState.VOD_NEXT_INIT, SessionState.VOD_NEXT_INITIATING].indexOf(this._state.state) !== -1) {
          return loop();
        } else {
          this._getFirstDuration(manifest)
          .then(firstDuration => {
            debug(`[${this._sessionId}]: Next tick in ${firstDuration} seconds`)
            return timer((firstDuration * 1000) - 50).then(() => {
              return loop();
            });  
          }).catch(err => {
            console.error(err);
            debug(`[${this._sessionId}]: Playhead consumer crashed (1)`);
            this._state.playhead.state = PlayheadState.CRASHED;
          });
        }  
      }).catch(err => {
        console.error(err);
        debug(`[${this._sessionId}]: Playhead consumer crashed (2)`);
        this._state.playhead.state = PlayheadState.CRASHED;
      });
    }
    loop().then(final => {
      if (this._state.playhead.state !== PlayheadState.CRASHED) {
        debug(`[${this._sessionId}]: Playhead consumer started`);
        this._state.playhead.state = PlayheadState.RUNNING;
      }
    }).catch(err => {
      console.error(err);
      debug(`[${this._sessionId}]: Playhead consumer crashed (2)`);
      this._state.playhead.state = PlayheadState.CRASHED;
    });
  }

  restartPlayhead() {
    this._state.state = SessionState.VOD_NEXT_INIT;
    debug(`[${this._sessionId}]: Restarting playhead consumer`);
    this.startPlayhead();
  }

  stopPlayhead() {

  }

  getStatus() {
    return new Promise((resolve, reject) => {
      const playheadStateMap = {};
      playheadStateMap[PlayheadState.IDLE] = 'idle';
      playheadStateMap[PlayheadState.RUNNING] = 'running';
      playheadStateMap[PlayheadState.CRASHED] = 'crashed';
  
      const status = {
        sessionId: this._sessionId,
        playhead: {
          state: playheadStateMap[this._state.playhead.state]
        }
      };
      resolve(status);
    });    
  }

  getCurrentMediaManifest(bw, playbackSessionId) {
    return new Promise((resolve, reject) => {
      if (this.currentVod) {
        const m3u8 = this.currentVod.getLiveMediaSequences(this._state.playhead.mediaSeq, bw, this._state.playhead.vodMediaSeq.video, this._state.discSeq);
        debug(`[${playbackSessionId}]: [${this._state.playhead.mediaSeq + this._state.playhead.vodMediaSeq.video}] Current media manifest for ${bw} requested`);
        resolve(m3u8);
      } else {
        resolve("Engine not ready");
      }
    });
  }

  getCurrentAudioManifest(audioGroupId, playbackSessionId) {
    return new Promise((resolve, reject) => {
      if (this.currentVod) {
        const m3u8 = this.currentVod.getLiveMediaAudioSequences(this._state.playhead.mediaSeq, audioGroupId, this._state.playhead.vodMediaSeq.audio, this._state.discSeq);
        debug(`[${playbackSessionId}]: [${this._state.playhead.mediaSeq + this._state.playhead.vodMediaSeq.audio}] Current audio manifest for ${bw} requested`);
        resolve(m3u8);
      } else {
        resolve("Engine not ready");
      }
    });
  }

  increment() {
    return new Promise((resolve, reject) => {
      this._tick().then(() => {
        if (this._state.state === SessionState.VOD_NEXT_INITIATING) {
          this._state.state = SessionState.VOD_PLAYING;
        } else {
          this._state.vodMediaSeq.video += 1;
          this._state.vodMediaSeq.audio += 1;
        }
        if (this._state.vodMediaSeq.video >= this.currentVod.getLiveMediaSequencesCount() - 1) {
          this._state.vodMediaSeq.video = this._state.vodMediaSeq.audio = this.currentVod.getLiveMediaSequencesCount() - 1;
          this._state.state = SessionState.VOD_NEXT_INIT;
        }
        this._state.playhead.mediaSeq = this._state.mediaSeq;
        this._state.playhead.vodMediaSeq = this._state.vodMediaSeq;
        debug(`[${this._sessionId}]: INCREMENT (mseq=${this._state.playhead.mediaSeq + this._state.playhead.vodMediaSeq.video}) vodMediaSeq=(${this._state.playhead.vodMediaSeq.video}_${this._state.playhead.vodMediaSeq.audio})`);
        let m3u8 = this.currentVod.getLiveMediaSequences(this._state.playhead.mediaSeq, 180000, this._state.playhead.vodMediaSeq.video, this._state.discSeq);
        resolve(m3u8);
      });
    })
  }

  getMediaManifest(bw, opts) {
    return new Promise((resolve, reject) => {
      this._tick().then(() => {
        let timeSinceLastRequest = (this._state.tsLastRequest.video === null) ? 0 : Date.now() - this._state.tsLastRequest.video;

        if (this._state.state === SessionState.VOD_NEXT_INITIATING) {
          this._state.state = SessionState.VOD_PLAYING;
        } else {
          let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);
          this._state.vodMediaSeq.video += sequencesToIncrement;
        }
        if (this._state.vodMediaSeq.video >= this.currentVod.getLiveMediaSequencesCount() - 1) {
          this._state.vodMediaSeq.video = this.currentVod.getLiveMediaSequencesCount() - 1;
          this._state.state = SessionState.VOD_NEXT_INIT;
        }

        debug(`[${this._sessionId}]: VIDEO ${timeSinceLastRequest} (${this.averageSegmentDuration}) bandwidth=${bw} vodMediaSeq=(${this._state.vodMediaSeq.video}_${this._state.vodMediaSeq.audio})`);
        let m3u8;
        try {
          m3u8 = this.currentVod.getLiveMediaSequences(this._state.mediaSeq, bw, this._state.vodMediaSeq.video, this._state.discSeq);
        } catch (exc) {
          if (this._state.lastM3u8[bw]) {
            m3u8 = this._state.lastM3u8[bw]
          } else {
            reject('Failed to generate media manifest');
          }
        }
        this._state.lastM3u8[bw] = m3u8;
        this._state.lastServedM3u8 = m3u8;
        this._state.tsLastRequest.video = Date.now();
        if (this._state.state === SessionState.VOD_NEXT_INIT) {
          this._tick().then(() => {
            timeSinceLastRequest = (this._state.tsLastRequest.video === null) ? 0 : Date.now() - this._state.tsLastRequest.video;
            if (this._state.state === SessionState.VOD_NEXT_INITIATING) {
              this._state.state = SessionState.VOD_PLAYING;
            }
            debug(`[${this._sessionId}]: VIDEO ${timeSinceLastRequest} (${this.averageSegmentDuration}) bandwidth=${bw} vodMediaSeq=(${this._state.vodMediaSeq.video}_${this._state.vodMediaSeq.audio})`);
            try {
              m3u8 = this.currentVod.getLiveMediaSequences(this._state.mediaSeq, bw, this._state.vodMediaSeq.video, this._state.discSeq);
            } catch (exc) {
              if (this._state.lastM3u8[bw]) {
                m3u8 = this._state.lastM3u8[bw]
              } else {
                reject('Failed to generate media manifest');
              }
            }
            this._state.lastM3u8[bw] = m3u8;
            this._state.lastServedM3u8 = m3u8;
            this._state.tsLastRequest.video = Date.now();    
            resolve(m3u8);
          });
        } else {
          resolve(m3u8);
        }
      }).catch(reject);
    });
  }

  getAudioManifest(audioGroupId, opts) {
    return new Promise((resolve, reject) => {
      let timeSinceLastRequest = (this._state.tsLastRequest.audio === null) ? 0 : Date.now() - this._state.tsLastRequest.audio;
      if (this._state.state !== SessionState.VOD_NEXT_INITIATING) {
        let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);
    
        if (this._state.vodMediaSeq.audio < this._state.vodMediaSeq.video) {
          this._state.vodMediaSeq.audio += sequencesToIncrement;
          if (this._state.vodMediaSeq.audio >= this.currentVod.getLiveMediaSequencesCount() - 1) {
            this._state.vodMediaSeq.audio = this.currentVod.getLiveMediaSequencesCount() - 1;
          }
        }
      }

      debug(`[${this._sessionId}]: AUDIO ${timeSinceLastRequest} (${this.averageSegmentDuration}) audioGroupId=${audioGroupId} vodMediaSeq=(${this._state.vodMediaSeq.video}_${this._state.vodMediaSeq.audio})`);
      let m3u8;
      try {
        m3u8 = this.currentVod.getLiveMediaAudioSequences(this._state.mediaSeq, audioGroupId, this._state.vodMediaSeq.audio, this._state.discSeq);
      } catch (exc) {
        if (this._state.lastM3u8[audioGroupId]) {
          m3u8 = this._state.lastM3u8[audioGroupId];
        } else {
          reject('Failed to generate audio manifest');
        }
      }
      this._state.lastM3u8[audioGroupId] = m3u8;
      this._state.tsLastRequest.audio = Date.now();
      resolve(m3u8);
    });
  }

  getMasterManifest() {
    return new Promise((resolve, reject) => {
      this._tick().then(() => {
        let m3u8 = "#EXTM3U\n";
        m3u8 += "#EXT-X-VERSION:4\n";
        m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.session.id",VALUE="${this._sessionId}"\n`;
        m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.eventstream",VALUE="/eventstream/${this._sessionId}"\n`;
        let audioGroupIds = this.currentVod.getAudioGroups();
        let defaultAudioGroupId;
        if (this.use_demuxed_audio === true) {
          if (audioGroupIds.length > 0) {
            m3u8 += "# AUDIO groups\n";
            for (let i = 0; i < audioGroupIds.length; i++) {
              let audioGroupId = audioGroupIds[i];
              m3u8 += `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="${audioGroupId}",NAME="audio",AUTOSELECT=YES,DEFAULT=YES,CHANNELS="2",URI="master-${audioGroupId}.m3u8;session=${this._sessionId}"\n`;
            }
            defaultAudioGroupId = audioGroupIds[0];
          }
        }
        this.currentVod.getUsageProfiles().forEach(profile => {
          m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + ',RESOLUTION=' + profile.resolution + ',CODECS="' + profile.codecs + '"' + (defaultAudioGroupId ? `,AUDIO="${defaultAudioGroupId}"` : '') + '\n';
          m3u8 += "master" + profile.bw + ".m3u8;session=" + this._sessionId + "\n";
        });
        if (this.use_demuxed_audio === true) {
          for (let i = 0; i < audioGroupIds.length; i++) {
            let audioGroupId = audioGroupIds[i];
            m3u8 += `#EXT-X-STREAM-INF:BANDWIDTH=97000,CODECS="mp4a.40.2",AUDIO="${audioGroupId}"\n`;
            m3u8 += `master-${audioGroupId}.m3u8;session=${this._sessionId}\n`;
          }
        }
        this.produceEvent({
          type: 'NOW_PLAYING',
          data: {
            id: this.currentMetadata.id,
            title: this.currentMetadata.title,
          }
        });
        this._state.tsLastRequest.master = Date.now();
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
          nextVodPromise.then(vodResponse => {
            debug(`[${this._sessionId}]: got first VOD uri=${vodResponse.uri}:${vodResponse.offset || 0}`);
            //newVod = new HLSVod(uri, [], Date.now());
            newVod = new HLSVod(vodResponse.uri, [], null, vodResponse.offset * 1000);
            this.currentVod = newVod;
            return this.currentVod.load();
          }).then(() => {
            debug(`[${this._sessionId}]: first VOD loaded`);
            //debug(newVod);
            this._state.vodMediaSeq.video = 0;
            this._state.vodMediaSeq.audio = 0;
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
          debug(`[${this._sessionId}]: state=VOD_PLAYING (${this._state.vodMediaSeq.video}_${this._state.vodMediaSeq.audio}, ${this.currentVod.getLiveMediaSequencesCount()})`);
          /*
          if (this._state.vodMediaSeq.video >= this.currentVod.getLiveMediaSequencesCount() - 1) {
            this._state.state = SessionState.VOD_NEXT_INIT;
          }
          */         
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

          vodPromise.then(vodResponse => {
            debug(`[${this._sessionId}]: got next VOD uri=${vodResponse.uri}:${vodResponse.offset}`);
            newVod = new HLSVod(vodResponse.uri, splices, null, vodResponse.offset * 1000);
            this.produceEvent({
              type: 'NEXT_VOD_SELECTED',
              data: {
                id: this.currentMetadata.id,
                uri: vodResponse.uri,
                title: this.currentMetadata.title || '',
              }
            });
            return newVod.loadAfter(this.currentVod);
          }).then(() => {
            debug(`[${this._sessionId}]: next VOD loaded`);
            //debug(newVod);
            this.currentVod = newVod;
            debug(`[${this._sessionId}]: msequences=${this.currentVod.getLiveMediaSequencesCount()}`);
            this._state.vodMediaSeq.video = 0;
            this._state.vodMediaSeq.audio = 0;
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
          }).catch(err => {
            console.error("Failed to initiate next VOD: ", err);
            reject(err);
          });
          break;
        default:
          reject("Invalid state: " + this.state.state);
      }

    });
  }

  _getNextVod() {
    return new Promise((resolve, reject) => {
      this._assetManager.getNextVod({ 
        sessionId: this._sessionId, 
        category: this._category, 
        playlistId: this._sessionId
      })
      .then(nextVod => {
        if (nextVod && nextVod.uri) {
          this.currentMetadata = {
            id: nextVod.id,
            title: nextVod.title || '',
          };
          resolve(nextVod);
        } else {
          console.error("Invalid VOD:", nextVod);
          reject("Invalid VOD from asset manager")
        }
      })
      .catch(reject);
    });
  }

  _getNextVodById(id) {
    return new Promise((resolve, reject) => {
      this._assetManager.getNextVodById(this._sessionId, id)
      .then(nextVod => {
        //debug(nextVod);
        this.currentMetadata = {
          id: nextVod.id,
          title: nextVod.title || '',
        };
        resolve(nextVod);
      })
      .catch(reject);
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

  _getFirstDuration(manifest) {
    return new Promise((resolve, reject) => {
      try {
        const parser = m3u8.createStream();
        let manifestStream = new Readable();
        manifestStream.push(manifest);
        manifestStream.push(null);

        manifestStream.pipe(parser);
        parser.on('m3u', m3u => {
          if (m3u.items.PlaylistItem[0]) {
            let firstDuration = m3u.items.PlaylistItem[0].get("duration");
            resolve(firstDuration);
          } else {
            console.error("Empty media playlist");
            console.error(manifest);
            reject('Empty media playlist!')
          }
        });
      } catch (err) {
        reject(err);
      }
    });
  }
}

module.exports = Session;
