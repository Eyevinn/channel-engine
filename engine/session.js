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
    this._playheadStateStore.create(this._sessionId);

    //this.currentVod;
    this.currentMetadata = {};
    this._events = [];
    this.averageSegmentDuration = AVERAGE_SEGMENT_DURATION;
    this.use_demuxed_audio = false;
    if (config) { 
      if (config.sessionId) {
        this._sessionId = config.sessionId;
      }
      if (config.startWithId) {
        (async () => {
          await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_INIT_BY_ID);
          await this._sessionStateStore.set(this._sessionId, "assetId", config.startWithId);  
        })();
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

  getCurrentVod(sessionState) {
    if (sessionState.currentVod) {
      let hlsVod = new HLSVod();
      hlsVod.fromJSON(sessionState.currentVod);
      return hlsVod;
    }
  }

  async setCurrentVod(sessionState, hlsVod) {
    return await this._sessionStateStore.set(this._sessionId, "currentVod", hlsVod.toJSON());
  }

  startPlayhead() {
    const loop = () => {
      return this.increment()
      .then(async (manifest) => {
        const sessionState = await this._sessionStateStore.get(this._sessionId);
        const playheadState = await this._playheadStateStore.get(this._sessionId);
        if ([SessionState.VOD_NEXT_INIT, SessionState.VOD_NEXT_INITIATING].indexOf(sessionState.state) !== -1) {
          return loop();
        } else if (playheadState.state == PlayheadState.STOPPED) {
          debug(`[${this._sessionId}]: Stopping playhead`);
          return;
        } else {
          this._getFirstDuration(manifest)
          .then(firstDuration => {
            debug(`[${this._sessionId}]: Next tick in ${firstDuration} seconds`)
            return timer((firstDuration * 1000) - 50).then(() => {
              return loop();
            });  
          }).catch(async (err) => {
            console.error(err);
            debug(`[${this._sessionId}]: Playhead consumer crashed (1)`);
            await this._playheadStateStore.set(this._sessionId, "state", PlayheadState.CRASHED);
          });
        }  
      }).catch(async (err) => {
        console.error(err);
        debug(`[${this._sessionId}]: Playhead consumer crashed (2)`);
        await this._playheadStateStore.set(this._sessionId, "state", PlayheadState.CRASHED);
      });
    }
    loop().then(async (final) => {
      const playheadState = await this._playheadStateStore.get(this._sessionId);
      if (playheadState.state !== PlayheadState.CRASHED) {
        debug(`[${this._sessionId}]: Playhead consumer started`);
        await this._playheadStateStore.set(this._sessionId, "state", PlayheadState.RUNNING);
      }
    }).catch(async (err) => {
      console.error(err);
      debug(`[${this._sessionId}]: Playhead consumer crashed (2)`);
      await this._playheadStateStore.set(this._sessionId, "state", PlayheadState.CRASHED);
    });
  }

  restartPlayhead() {
    (async () => {
      await this._sessionStateStore.set(this._sessionId, state, SessionState.VOD_NEXT_INIT);
      debug(`[${this._sessionId}]: Restarting playhead consumer`);
      this.startPlayhead();  
    })();
  }

  stopPlayhead() {
    (async () => {
      await this._playheadStateStore.set(this._sessionId, "state", PlayheadState.STOPPED);
    })();
  }

  getStatus() {
    return new Promise((resolve, reject) => {
      (async () => {
        const playheadState = await this._playheadStateStore.get(this._sessionId);
        const playheadStateMap = {};
        playheadStateMap[PlayheadState.IDLE] = 'idle';
        playheadStateMap[PlayheadState.RUNNING] = 'running';
        playheadStateMap[PlayheadState.CRASHED] = 'crashed';
    
        const status = {
          sessionId: this._sessionId,
          playhead: {
            state: playheadStateMap[playheadState.state]
          }
        };
        resolve(status);  
      })();
    });    
  }

  getCurrentMediaManifest(bw, playbackSessionId) {
    return new Promise((resolve, reject) => {
      (async () => {
        const sessionState = await this._sessionStateStore.get(this._sessionId);
        const playheadState = await this._playheadStateStore.get(this._sessionId);
        const currentVod = this.getCurrentVod(sessionState);
        if (currentVod) {
          const m3u8 = currentVod.getLiveMediaSequences(playheadState.mediaSeq, bw, playheadState.vodMediaSeqVideo, sessionState.discSeq);
          debug(`[${playbackSessionId}]: [${playheadState.mediaSeq + playheadState.vodMediaSeqVideo}] Current media manifest for ${bw} requested`);
          resolve(m3u8);
        } else {
          resolve("Engine not ready");
        }  
      })();
    });
  }

  getCurrentAudioManifest(audioGroupId, playbackSessionId) {
    return new Promise((resolve, reject) => {
      (async () => {
        const sessionState = await this._sessionStateStore.get(this._sessionId);
        const playheadState = await this._playheadStateStore.get(this._sessionId);
        const currentVod = this.getCurrentVod(sessionState);
        if (currentVod) {
          const m3u8 = currentVod.getLiveMediaAudioSequences(playheadState.mediaSeq, audioGroupId, playheadState.vodMediaSeqAudio, sessionState.discSeq);
          debug(`[${playbackSessionId}]: [${playheadState.mediaSeq + playheadState.vodMediaSeqAudio}] Current audio manifest for ${bw} requested`);
          resolve(m3u8);
        } else {
          resolve("Engine not ready");
        }  
      })();
    });
  }

  increment() {
    return new Promise((resolve, reject) => {
      this._tick().then(async () => {
        let sessionState = await this._sessionStateStore.get(this._sessionId);
        let playheadState = await this._playheadStateStore.get(this._sessionId);
        const currentVod = this.getCurrentVod(sessionState);
        if (sessionState.state === SessionState.VOD_NEXT_INITIATING) {
          sessionState = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
        } else {
          sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", sessionState.vodMediaSeqVideo + 1);
          sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", sessionState.vodMediaSeqAudio + 1);
        }
        if (sessionState.vodMediaSeqVideo >= currentVod.getLiveMediaSequencesCount() - 1) {
          sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", currentVod.getLiveMediaSequencesCount() - 1);
          sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", currentVod.getLiveMediaSequencesCount() - 1);
          sessionState = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INIT);
        }
        playheadState = await this._playheadStateStore.set(this._sessionId, "mediaSeq", sessionState.mediaSeq);
        playheadState = await this._playheadStateStore.set(this._sessionId, "vodMediaSeqVideo", sessionState.vodMediaSeqVideo);
        playheadState = await this._playheadStateStore.set(this._sessionId, "vodMediaSeqAudio", sessionState.vodMediaSeqAudio);
        debug(`[${this._sessionId}]: INCREMENT (mseq=${playheadState.mediaSeq + playheadState.vodMediaSeqVideo}) vodMediaSeq=(${playheadState.vodMediaSeqVideo}_${playheadState.vodMediaSeqAudio})`);
        let m3u8 = currentVod.getLiveMediaSequences(playheadState.mediaSeq, 180000, playheadState.vodMediaSeqVideo, sessionState.discSeq);
        resolve(m3u8);
      });
    })
  }

  getMediaManifest(bw, opts) {
    return new Promise((resolve, reject) => {
      this._tick().then(async () => {
        const tsLastRequestVideo = await this._sessionStateStore.get(this._sessionId).tsLastRequestVideo;
        let timeSinceLastRequest = (tsLastRequestVideo === null) ? 0 : Date.now() - tsLastRequestVideo;

        let sessionState = await this._sessionStateStore.get(this._sessionId);
        const currentVod = this.getCurrentVod(sessionState);
        if (sessionState.state === SessionState.VOD_NEXT_INITIATING) {
          sessionState = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
        } else {
          let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);
          sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", sessionState.vodMediaSeqVideo + sequencesToIncrement);
        }
        if (sessionState.vodMediaSeqVideo >= currentVod.getLiveMediaSequencesCount() - 1) {
          sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", currentVod.getLiveMediaSequencesCount() - 1);
          sessionState = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INIT);
        }

        debug(`[${this._sessionId}]: VIDEO ${timeSinceLastRequest} (${this.averageSegmentDuration}) bandwidth=${bw} vodMediaSeq=(${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio})`);
        let m3u8;
        try {
          m3u8 = currentVod.getLiveMediaSequences(sessionState.mediaSeq, bw, sessionState.vodMediaSeqVideo, sessionState.discSeq);
        } catch (exc) {
          if (sessionState.lastM3u8[bw]) {
            m3u8 = sessionState.lastM3u8[bw]
          } else {
            reject('Failed to generate media manifest');
          }
        }
        let lastM3u8 = sessionState.lastM3u8;
        lastM3u8[bw] = m3u8;
        sessionState = await this._sessionStateStore.set(this._sessionId, "lastM3u8", lastM3u8);
        sessionState = await this._sessionStateStore.set(this._sessionId, "lastServedM3u8", m3u8);
        sessionState = await this._sessionStateStore.set(this._sessionId, "tsLastRequestVideo", Date.now());

        if (sessionState.state === SessionState.VOD_NEXT_INIT) {
          this._tick().then(async () => {
            const tsLastRequestVideo = await this._sessionStateStore.get(this._sessionId).tsLastRequestVideo;
            let timeSinceLastRequest = (tsLastRequestVideo === null) ? 0 : Date.now() - tsLastRequestVideo;

            let sessionState = await this._sessionStateStore.get(this._sessionId);
            if (sessionState.state === SessionState.VOD_NEXT_INITIATING) {
              sessionState = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
            }
            debug(`[${this._sessionId}]: VIDEO ${timeSinceLastRequest} (${this.averageSegmentDuration}) bandwidth=${bw} vodMediaSeq=(${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio})`);
            try {
              m3u8 = currentVod.getLiveMediaSequences(sessionState.mediaSeq, bw, sessionState.vodMediaSeqVideo, sessionState.discSeq);
            } catch (exc) {
              if (sessionState.lastM3u8[bw]) {
                m3u8 = sessionState.lastM3u8[bw]
              } else {
                reject('Failed to generate media manifest');
              }
            }
            let lastM3u8 = sessionState.lastM3u8;
            lastM3u8[bw] = m3u8;
            sessionState = await this._sessionStateStore.set(this._sessionId, "lastM3u8", lastM3u8);
            sessionState = await this._sessionStateStore.set(this._sessionId, "lastServedM3u8", m3u8);
            sessionState = await this._sessionStateStore.set(this._sessionId, "tsLastRequestVideo", Date.now());
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
      (async () => {
        const tsLastRequestAudio = await this._sessionStateStore.get(this._sessionId).tsLastRequestAudio;
        let timeSinceLastRequest = (tsLastRequestAudio === null) ? 0 : Date.now() - tsLastRequestAudio;

        let sessionState = await this._sessionStateStore.get(this._sessionId);
        const currentVod = this.getCurrentVod(sessionState);
        if (sessionState.state !== SessionState.VOD_NEXT_INITIATING) {
          let sequencesToIncrement = Math.ceil(timeSinceLastRequest / this.averageSegmentDuration);
      
          if (sessionState.vodMediaSeqAudio < sessionState.vodMediaSeqVideo) {
            sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", sessionState,vodMediaSeqAudio + sequencesToIncrement);
            if (sessionState.vodMediaSeqAudio >= currentVod.getLiveMediaSequencesCount() - 1) {
              sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", currentVod.getLiveMediaSequencesCount() - 1);
            }
          }
        }
  
        debug(`[${this._sessionId}]: AUDIO ${timeSinceLastRequest} (${this.averageSegmentDuration}) audioGroupId=${audioGroupId} vodMediaSeq=(${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio})`);
        let m3u8;
        try {
          m3u8 = currentVod.getLiveMediaAudioSequences(sessionState.mediaSeq, audioGroupId, sessionState.vodMediaSeqAudio, sessionState.discSeq);
        } catch (exc) {
          if (sessionState.lastM3u8[audioGroupId]) {
            m3u8 = sessionState.lastM3u8[audioGroupId];
          } else {
            reject('Failed to generate audio manifest');
          }
        }
        let lastM3u8 = sessionState.lastM3u8;
        lastM3u8[audioGroupId] = m3u8;
        sessionState = await this._sessionStateStore.set(this._sessionId, "lastM3u8", lastM3u8);
        sessionState = await this._sessionStateStore.set(this._sessionId, "tsLastRequestAudio", Date.now());
        resolve(m3u8);
      })();
    });
  }

  getMasterManifest() {
    return new Promise((resolve, reject) => {
      this._tick().then(async () => {
        let m3u8 = "#EXTM3U\n";
        m3u8 += "#EXT-X-VERSION:4\n";
        m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.session.id",VALUE="${this._sessionId}"\n`;
        m3u8 += `#EXT-X-SESSION-DATA:DATA-ID="eyevinn.tv.eventstream",VALUE="/eventstream/${this._sessionId}"\n`;
        const sessionState = await this._sessionStateStore.get(this._sessionId);
        const currentVod = this.getCurrentVod(sessionState);
        let audioGroupIds = currentVod.getAudioGroups();
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
          currentVod.getUsageProfiles().forEach(profile => {
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
    return new Promise(async (resolve, reject) => {
      // State machine
      let newVod;

      let sessionState = await this._sessionStateStore.get(this._sessionId);
      let currentVod = this.getCurrentVod(sessionState);
      
      switch(sessionState.state) {
        case SessionState.VOD_INIT:
        case SessionState.VOD_INIT_BY_ID:
          let nextVodPromise;
          if (sessionState.state === SessionState.VOD_INIT) {
            debug(`[${this._sessionId}]: state=VOD_INIT`);
            nextVodPromise = this._getNextVod();
          } else if (sessionState.state === SessionState.VOD_INIT_BY_ID) {
            debug(`[${this._sessionId}]: state=VOD_INIT_BY_ID ${sessionState.assetId}`);
            nextVodPromise = this._getNextVodById(sessionState.assetId);
          }
          nextVodPromise.then(vodResponse => {
            if (!vodResponse.type) {
              debug(`[${this._sessionId}]: got first VOD uri=${vodResponse.uri}:${vodResponse.offset || 0}`);
              //newVod = new HLSVod(uri, [], Date.now());
              newVod = new HLSVod(vodResponse.uri, [], null, vodResponse.offset * 1000);
              currentVod = newVod;
              return currentVod.load();
            } else {
              if (vodResponse.type === 'gap') {
                return new Promise((resolve, reject) => {
                  this._fillGap(null, vodResponse.desiredDuration)
                  .then(gapVod => {
                    currentVod = gapVod;
                    resolve(gapVod);
                  }).catch(reject);  
                });
              }
            }
          }).then(async () => {
            debug(`[${this._sessionId}]: first VOD loaded`);
            //debug(newVod);
            sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
            sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
            this.produceEvent({
              type: 'NOW_PLAYING',
              data: {
                id: this.currentMetadata.id,
                title: this.currentMetadata.title,
              }
            });
            sessionState = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
            sessionState = await this.setCurrentVod(sessionState, currentVod);
            resolve();
          }).catch(e => {
            console.error("Failed to init first VOD");
            if(this.slateUri) {
              console.error("Will insert slate");
              this._loadSlate()
              .then(async (slateVod) => {
                currentVod = slateVod;
                debug(`[${this._sessionId}]: slate loaded`);
                sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
                sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
                sessionState = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_PLAYING);
                sessionState = await this.setCurrentVod(sessionState, currentVod);
                resolve();    
              })
              .catch(reject);
            } else {
              debug('No slate to load');
            }
          });
          break;
        case SessionState.VOD_PLAYING:
          debug(`[${this._sessionId}]: state=VOD_PLAYING (${sessionState.vodMediaSeqVideo}_${sessionState.vodMediaSeqAudio}, ${currentVod.getLiveMediaSequencesCount()})`);
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
          const length = currentVod.getLiveMediaSequencesCount();
          const lastDiscontinuity = currentVod.getLastDiscontinuity();
          sessionState = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INITIATING);
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
              return newVod.loadAfter(currentVod);
            } else {
              if (vodResponse.type === 'gap') {
                return new Promise((resolve, reject) => {
                  this._fillGap(currentVod, vodResponse.desiredDuration)
                  .then(gapVod => {
                    newVod = gapVod;
                    resolve(newVod);
                  }).catch(reject);  
                })
              }
            }
          })
          .then(async () => {
            debug(`[${this._sessionId}]: next VOD loaded`);
            //debug(newVod);
            currentVod = newVod;
            debug(`[${this._sessionId}]: msequences=${currentVod.getLiveMediaSequencesCount()}`);
            sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
            sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
            sessionState = await this._sessionStateStore.set(this._sessionId, "mediaSeq", sessionState.mediaSeq + length);
            sessionState = await this._sessionStateStore.set(this._sessionId, "discSeq", sessionState.discSeq + lastDiscontinuity);
            sessionState = await this.setCurrentVod(sessionState, currentVod);
            this.produceEvent({
              type: 'NOW_PLAYING',
              data: {
                id: this.currentMetadata.id,
                title: this.currentMetadata.title,
              }
            });            
            resolve();
          })
          .catch(async (err) => {
            console.error("Failed to init next VOD");
            debug(err);
            if(this.slateUri) {
              console.error("Will insert slate");
              this._loadSlate(currentVod)
              .then(async (slateVod) => {
                currentVod = slateVod;
                debug(`[${this._sessionId}]: slate loaded`);
                sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqVideo", 0);
                sessionState = await this._sessionStateStore.set(this._sessionId, "vodMediaSeqAudio", 0);
                sessionState = await this._sessionStateStore.set(this._sessionId, "mediaSeq", sessionState.mediaSeq + length);
                sessionState = await this._sessionStateStore.set(this._sessionId, "discSeq", sessionState.discSeq + lastDiscontinuity);    
                sessionState = await this._sessionStateStore.set(this._sessionId, "state", SessionState.VOD_NEXT_INITIATING);
                sessionState = await this.setCurrentVod(sessionState, currentVod);
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
          reject("Invalid state: " + sessionState.state);
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

  /*
  _getNearestBandwidth(bandwidth) {
    const availableBandwidths = this.currentVod.getBandwidths().sort((a,b) => b - a);
    for (let i = 0; i < availableBandwidths.length; i++) {
      if (bandwidth >= availableBandwidths[i]) {
        return availableBandwidths[i];
      }
    }
    return availableBandwidths[availableBandwidths.length - 1];
  }
  */

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
