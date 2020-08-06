const crypto = require('crypto');
const debug = require('debug')('engine-session');
const HLSVod = require('@eyevinn/hls-vodtolive');
const m3u8 = require('@eyevinn/m3u8');
const HLSRepeatVod = require('@eyevinn/hls-repeat');
const Readable = require('stream').Readable;

const { SessionState } = require('./session_state.js');
const { PlayheadState } = require('./playhead_state.js');

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
  constructor(assetManager, config, sessionStore) {
    this._assetManager = assetManager;
    this._sessionId = crypto.randomBytes(20).toString('hex');

    this._sessionStateStore = sessionStore.sessionStateStore;
    this._playheadStateStore = sessionStore.playheadStateStore;

    this._sessionStateStore.create(this._sessionId);

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
        this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_INIT_BY_ID);
        this._sessionStateStore.set(this._sessionId, "assetId", config.startWithId);
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
      if (config.profile) {
        this._sessionProfile = config.profile;
      }
      if (config.slateUri) {
        this.slateUri = config.slateUri;
        this.slateRepetitions = config.slateRepetitions || 10;
        debug(`Will use slate URI ${this.slateUri} (${this.slateRepetitions})`);
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
        const sessionState = this._sessionStateStore.get(this._sessionId);
        if ([SessionState.VOD_NEXT_INIT, SessionState.VOD_NEXT_INITIATING].indexOf(sessionState.state) !== -1) {
          return loop();
        } else if (this._playheadStateStore.get(this._sessionId).state == PlayheadState.STOPPED) {
          debug(`[${this._sessionId}]: Stopping playhead`);
          return;
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
            this._playheadStateStore.set(this._sessionId, "state", PlayheadState.CRASHED);
          });
        }  
      }).catch(err => {
        console.error(err);
        debug(`[${this._sessionId}]: Playhead consumer crashed (2)`);
        this._playheadStateStore.set(this._sessionId, "state", PlayheadState.CRASHED);
      });
    }
    loop().then(final => {
      if (this._playheadStateStore.get(this._sessionId).state !== PlayheadState.CRASHED) {
        debug(`[${this._sessionId}]: Playhead consumer started`);
        this._playheadStateStore.set(this._sessionId, "state", PlayheadState.RUNNING);
      }
    }).catch(err => {
      console.error(err);
      debug(`[${this._sessionId}]: Playhead consumer crashed (2)`);
      this._playheadStateStore.set(this._sessionId, "state", PlayheadState.CRASHED);
    });
  }

  restartPlayhead() {
    this._sessionStateStore.set(this._sessionId, state, SessionState.VOD_NEXT_INIT);
    debug(`[${this._sessionId}]: Restarting playhead consumer`);
    this.startPlayhead();
  }

  stopPlayhead() {
    this._playheadStateStore.set(this._sessionId, "state", PlayheadState.STOPPED);
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
          state: playheadStateMap[this._playheadStateStore.get(this._sessionId).state]
        }
      };
      resolve(status);
    });    
  }

  getCurrentMediaManifest(bw, playbackSessionId) {
    return new Promise((resolve, reject) => {
      const sessionState = this._sessionStateStore.get(this._sessionId);
      if (this.currentVod) {
        const m3u8 = this.currentVod.getLiveMediaSequences(this._playheadStateStore.get(this._sessionId).mediaSeq, bw, this._playheadStateStore.get(this._sessionId).vodMediaSeqVideo, sessionState.discSeq);
        debug(`[${playbackSessionId}]: [${this._playheadStateStore.get(this._sessionId).mediaSeq + this._playheadStateStore.get(this._sessionId).vodMediaSeqVideo}] Current media manifest for ${bw} requested`);
        resolve(m3u8);
      } else {
        resolve("Engine not ready");
      }
    });
  }

  getCurrentAudioManifest(audioGroupId, playbackSessionId) {
    return new Promise((resolve, reject) => {
      const sessionState = this._sessionStateStore.get(this._sessionId);
      if (this.currentVod) {
        const m3u8 = this.currentVod.getLiveMediaAudioSequences(this._playheadStateStore.get(this._sessionId).mediaSeq, audioGroupId, this._playheadStateStore.get(this._sessionId).vodMediaSeqAudio, sessionState.discSeq);
        debug(`[${playbackSessionId}]: [${this._playheadStateStore.get(this._sessionId).mediaSeq + this._playheadStateStore.get(this._sessionId).vodMediaSeqAudio}] Current audio manifest for ${bw} requested`);
        resolve(m3u8);
      } else {
        resolve("Engine not ready");
      }
    });
  }

  increment() {
    return new Promise((resolve, reject) => {
      this._tick().then(() => {
        const sessionState = this._sessionStateStore.get(this._sessionId);
        if (sessionState.state === SessionState.VOD_NEXT_INITIATING) {
          this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
        } else {
          this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo + 1);
          this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", this._sessionStateStore.get(this._sessionId).vodMediaSeqAudio + 1);
        }
        if (this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo >= this.currentVod.getLiveMediaSequencesCount() - 1) {
          this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", this.currentVod.getLiveMediaSequencesCount() - 1);
          this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", this.currentVod.getLiveMediaSequencesCount() - 1);
          this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INIT);
        }
        this._playheadStateStore.set(this._sessionId, "mediaSeq", this._sessionStateStore.get(this._sessionId).mediaSeq);
        this._playheadStateStore.set(this._sessionId, "vodMediaSeqVideo", this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo);
        this._playheadStateStore.set(this._sessionId, "vodMediaSeqAudio", this._sessionStateStore.get(this._sessionId).vodMediaSeqAudio);
        debug(`[${this._sessionId}]: INCREMENT (mseq=${this._playheadStateStore.get(this._sessionId).mediaSeq + this._playheadStateStore.get(this._sessionId).vodMediaSeqVideo}) vodMediaSeq=(${this._playheadStateStore.get(this._sessionId).vodMediaSeqVideo}_${this._playheadStateStore.get(this._sessionId).vodMediaSeqAudio})`);
        let m3u8 = this.currentVod.getLiveMediaSequences(this._playheadStateStore.get(this._sessionId).mediaSeq, 180000, this._playheadStateStore.get(this._sessionId).vodMediaSeqVideo, this._sessionStateStore.get(this._sessionId).discSeq);
        resolve(m3u8);
      });
    })
  }

  getMediaManifest(bw, opts) {
    return new Promise((resolve, reject) => {
      this._tick().then(() => {
        const tsLastRequestVideo = this._sessionStateStore.get(this._sessionId).tsLastRequestVideo;
        let timeSinceLastRequest = (tsLastRequestVideo === null) ? 0 : Date.now() - tsLastRequestVideo;

        if (this._sessionStateStore.get(this._sessionId).state === SessionState.VOD_NEXT_INITIATING) {
          this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
        } else {
          let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);
          this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo + sequencesToIncrement);
        }
        if (this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo >= this.currentVod.getLiveMediaSequencesCount() - 1) {
          this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", this.currentVod.getLiveMediaSequencesCount() - 1);
          this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INIT);
        }

        debug(`[${this._sessionId}]: VIDEO ${timeSinceLastRequest} (${this.averageSegmentDuration}) bandwidth=${bw} vodMediaSeq=(${this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo}_${this._sessionStateStore.get(this._sessionId).vodMediaSeqAudio})`);
        let m3u8;
        try {
          m3u8 = this.currentVod.getLiveMediaSequences(this._sessionStateStore.get(this._sessionId).mediaSeq, bw, this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo, this._sessionStateStore.get(this._sessionId).discSeq);
        } catch (exc) {
          if (this._sessionStateStore.get(this._sessionId).lastM3u8[bw]) {
            m3u8 = this._sessionStateStore.get(this._sessionId).lastM3u8[bw]
          } else {
            reject('Failed to generate media manifest');
          }
        }
        let lastM3u8 = this._sessionStateStore.get(this._sessionId).lastM3u8;
        lastM3u8[bw] = m3u8;
        this._sessionStateStore.set(this._sessionId, "lastM3u8", lastM3u8);
        this._sessionStateStore.set(this._sessionId, "lastServedM3u8", m3u8);
        this._sessionStateStore.set(this._sessionId, "tsLastRequestVideo", Date.now());

        if (this._sessionStateStore.get(this._sessionId).state === SessionState.VOD_NEXT_INIT) {
          this._tick().then(() => {
            const tsLastRequestVideo = this._sessionStateStore.get(this._sessionId).tsLastRequestVideo;
            let timeSinceLastRequest = (tsLastRequestVideo === null) ? 0 : Date.now() - tsLastRequestVideo;
                
            if (this._sessionStateStore.get(this._sessionId).state === SessionState.VOD_NEXT_INITIATING) {
              this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
            }
            debug(`[${this._sessionId}]: VIDEO ${timeSinceLastRequest} (${this.averageSegmentDuration}) bandwidth=${bw} vodMediaSeq=(${this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo}_${this._sessionStateStore.get(this._sessionId).vodMediaSeqAudio})`);
            try {
              m3u8 = this.currentVod.getLiveMediaSequences(this._sessionStateStore.get(this._sessionId).mediaSeq, bw, this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo, this._sessionStateStore.get(this._sessionId).discSeq);
            } catch (exc) {
              if (this._sessionStateStore.get(this._sessionId).lastM3u8[bw]) {
                m3u8 = this._sessionStateStore.get(this._sessionId).lastM3u8[bw]
              } else {
                reject('Failed to generate media manifest');
              }
            }
            let lastM3u8 = this._sessionStateStore.get(this._sessionId).lastM3u8;
            lastM3u8[bw] = m3u8;
            this._sessionStateStore.set(this._sessionId, "lastM3u8", lastM3u8);
            this._sessionStateStore.set(this._sessionId, "lastServedM3u8", m3u8);
            this._sessionStateStore.set(this._sessionId, "tsLastRequestVideo", Date.now());
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
      const tsLastRequestAudio = this._sessionStateStore.get(this._sessionId).tsLastRequestAudio;
      let timeSinceLastRequest = (tsLastRequestAudio === null) ? 0 : Date.now() - tsLastRequestAudio;
      if (this._sessionStateStore.get(this._sessionId).state !== SessionState.VOD_NEXT_INITIATING) {
        let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);
    
        if (this._sessionStateStore.get(this._sessionId).vodMediaSeqAudio < this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo) {
          this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", this._sessionStateStore.get(this._sessionId),vodMediaSeqAudio + sequencesToIncrement);
          if (this._sessionStateStore.get(this._sessionId).vodMediaSeqAudio >= this.currentVod.getLiveMediaSequencesCount() - 1) {
            this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", this.currentVod.getLiveMediaSequencesCount() - 1);
          }
        }
      }

      debug(`[${this._sessionId}]: AUDIO ${timeSinceLastRequest} (${this.averageSegmentDuration}) audioGroupId=${audioGroupId} vodMediaSeq=(${this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo}_${this._sessionStateStore.get(this._sessionId).vodMediaSeqAudio})`);
      let m3u8;
      try {
        m3u8 = this.currentVod.getLiveMediaAudioSequences(this._sessionStateStore.get(this._sessionId).mediaSeq, audioGroupId, this._sessionStateStore.get(this._sessionId).vodMediaSeqAudio, this._sessionStateStore.get(this._sessionId).discSeq);
      } catch (exc) {
        if (this._sessionStateStore.get(this._sessionId).lastM3u8[audioGroupId]) {
          m3u8 = this._sessionStateStore.get(this._sessionId).lastM3u8[audioGroupId];
        } else {
          reject('Failed to generate audio manifest');
        }
      }
      let lastM3u8 = this._sessionStateStore.get(this._sessionId).lastM3u8;
      lastM3u8[audioGroupId] = m3u8;
      this._sessionStateStore.set(this._sessionId, "lastM3u8", lastM3u8);
      this._sessionStateStore.set(this._sessionId, "tsLastRequestAudio", Date.now());
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
        if (this._sessionProfile) {
          this._sessionProfile.forEach(profile => {
            m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + ',RESOLUTION=' + profile.resolution[0] + 'x' + profile.resolution[1] + ',CODECS="' + profile.codecs + '"' + (defaultAudioGroupId ? `,AUDIO="${defaultAudioGroupId}"` : '') + '\n';
            m3u8 += "master" + profile.bw + ".m3u8;session=" + this._sessionId + "\n";
          });
        } else {
          this.currentVod.getUsageProfiles().forEach(profile => {
            m3u8 += '#EXT-X-STREAM-INF:BANDWIDTH=' + profile.bw + ',RESOLUTION=' + profile.resolution + ',CODECS="' + profile.codecs + '"' + (defaultAudioGroupId ? `,AUDIO="${defaultAudioGroupId}"` : '') + '\n';
            m3u8 += "master" + profile.bw + ".m3u8;session=" + this._sessionId + "\n";
          });
        }
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
        this._sessionStateStore.set(this._sessionId, "tsLastRequestMaster", Date.now());
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

      switch(this._sessionStateStore.get(this._sessionId).state) {
        case SessionState.VOD_INIT:
        case SessionState.VOD_INIT_BY_ID:
          let nextVodPromise;
          if (this._sessionStateStore.get(this._sessionId).state === SessionState.VOD_INIT) {
            debug(`[${this._sessionId}]: state=VOD_INIT`);
            nextVodPromise = this._getNextVod();
          } else if (this._sessionStateStore.get(this._sessionId).state === SessionState.VOD_INIT_BY_ID) {
            debug(`[${this._sessionId}]: state=VOD_INIT_BY_ID ${this._sessionStateStore.get(this._sessionId).assetId}`);
            nextVodPromise = this._getNextVodById(this._sessionStateStore.get(this._sessionId).assetId);
          }
          nextVodPromise.then(vodResponse => {
            if (!vodResponse.type) {
              debug(`[${this._sessionId}]: got first VOD uri=${vodResponse.uri}:${vodResponse.offset || 0}`);
              //newVod = new HLSVod(uri, [], Date.now());
              newVod = new HLSVod(vodResponse.uri, [], null, vodResponse.offset * 1000);
              this.currentVod = newVod;
              return this.currentVod.load();
            } else {
              if (vodResponse.type === 'gap') {
                return new Promise((resolve, reject) => {
                  this._fillGap(null, vodResponse.desiredDuration)
                  .then(gapVod => {
                    this.currentVod = gapVod;
                    resolve(gapVod);
                  }).catch(reject);  
                });
              }
            }
          }).then(() => {
            debug(`[${this._sessionId}]: first VOD loaded`);
            //debug(newVod);
            this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
            this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
            this.produceEvent({
              type: 'NOW_PLAYING',
              data: {
                id: this.currentMetadata.id,
                title: this.currentMetadata.title,
              }
            });
            this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
            resolve();
          }).catch(e => {
            console.error("Failed to init first VOD");
            if(this.slateUri) {
              console.error("Will insert slate");
              this._loadSlate()
              .then(slateVod => {
                this.currentVod = slateVod;
                debug(`[${this._sessionId}]: slate loaded`);
                this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
                this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
                this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
                resolve();    
              })
              .catch(reject);
            } else {
              debug('No slate to load');
            }
          });
          break;
        case SessionState.VOD_PLAYING:
          debug(`[${this._sessionId}]: state=VOD_PLAYING (${this._sessionStateStore.get(this._sessionId).vodMediaSeqVideo}_${this._sessionStateStore.get(this._sessionId).vodMediaSeqAudio}, ${this.currentVod.getLiveMediaSequencesCount()})`);
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
          this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INITIATING);
          let vodPromise = this._getNextVod();

          vodPromise.then(vodResponse => {
            if (!vodResponse.type) {
              debug(`[${this._sessionId}]: got next VOD uri=${vodResponse.uri}:${vodResponse.offset}`);
              newVod = new HLSVod(vodResponse.uri, null, null, vodResponse.offset * 1000);
              this.produceEvent({
                type: 'NEXT_VOD_SELECTED',
                data: {
                  id: this.currentMetadata.id,
                  uri: vodResponse.uri,
                  title: this.currentMetadata.title || '',
                }
              });
              return newVod.loadAfter(this.currentVod);
            } else {
              if (vodResponse.type === 'gap') {
                return new Promise((resolve, reject) => {
                  this._fillGap(this.currentVod, vodResponse.desiredDuration)
                  .then(gapVod => {
                    newVod = gapVod;
                    resolve(newVod);
                  }).catch(reject);  
                })
              }
            }
          })
          .then(() => {
            debug(`[${this._sessionId}]: next VOD loaded`);
            //debug(newVod);
            this.currentVod = newVod;
            debug(`[${this._sessionId}]: msequences=${this.currentVod.getLiveMediaSequencesCount()}`);
            this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
            this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
            this._sessionStateStore.set(this._sessionId, "mediaSeq", this._sessionStateStore.get(this._sessionId).mediaSeq + length);
            this._sessionStateStore.set(this._sessionId, "discSeq", this._sessionStateStore.get(this._sessionId).discSeq + lastDiscontinuity);
            this.produceEvent({
              type: 'NOW_PLAYING',
              data: {
                id: this.currentMetadata.id,
                title: this.currentMetadata.title,
              }
            });            
            resolve();
          })
          .catch(err => {
            console.error("Failed to init next VOD");
            debug(err);
            if(this.slateUri) {
              console.error("Will insert slate");
              this._loadSlate(this.currentVod)
              .then(slateVod => {
                this.currentVod = slateVod;
                debug(`[${this._sessionId}]: slate loaded`);
                this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
                this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
                this._sessionStateStore.set(this._sessionId, "mediaSeq", this._sessionStateStore.get(this._sessionId).mediaSeq + length);
                this._sessionStateStore.set(this._sessionId, "discSeq", this._sessionStateStore.get(this._sessionId).discSeq + lastDiscontinuity);    
                this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INITIATING);
                resolve();    
              })
              .catch(reject);
            } else {
              debug('No slate to load');
              reject(err);
            }
          }) 
          break;
        default:
          reject("Invalid state: " + his._sessionStateStore.get(this._sessionId).state);
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
        } else if (nextVod && nextVod.type === 'gap') {
          this.currentMetadata = {
            id: 'GAP',
            title: 'GAP of ' + Math.floor(nextVod.desiredDuration) + ' sec',
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

  _loadSlate(afterVod, reps) {
    return new Promise((resolve, reject) => {
      try {
        const slateVod = new HLSRepeatVod(this.slateUri, reps || this.slateRepetitions);
        let hlsVod;

        slateVod.load()
        .then(() => {
          hlsVod = new HLSVod(this.slateUri);
          const slateMediaManifestLoader = (bw) => {
            let mediaManifestStream = new Readable();
            mediaManifestStream.push(slateVod.getMediaManifest(bw));
            mediaManifestStream.push(null);
            return mediaManifestStream;
          };
          if (afterVod) {
            return hlsVod.loadAfter(afterVod, null, slateMediaManifestLoader);
          } else {
            return hlsVod.load(null, slateMediaManifestLoader);
          }
        })
        .then(() => {
          resolve(hlsVod);
        })
        .catch(err => {
          debug(err);
          reject(err);
        });
      } catch(err) {
        reject(err);
      }
    });
  }

  _fillGap(afterVod, desiredDuration) {
    return new Promise((resolve, reject) => {
      const reps = Math.floor(desiredDuration / 4000);
      debug(`[${this._sessionId}]: Trying to fill a gap of ${desiredDuration} milliseconds (${reps} repetitions)`);
      this._loadSlate(afterVod, reps).then(hlsVod => {
        resolve(hlsVod);
      }).catch(reject);
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
