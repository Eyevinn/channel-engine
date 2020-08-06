const { get } = require("request");

const SessionState = Object.freeze({
  VOD_INIT: 1,
  VOD_PLAYING: 2,
  VOD_NEXT_INIT: 3,
  VOD_NEXT_INITIATING: 4,
});

class SessionStateStore {
  constructor() {
    this.sessionStates = {};
  }

  create(sessionId) {
    if (!this.sessionStates[sessionId]) {
      this.sessionStates[sessionId] = {
        mediaSeq: 0,
        discSeq: 0,
        vodMediaSeqVideo: 0,
        vodMediaSeqAudio: 0, // assume only one audio group now
        state: SessionState.VOD_INIT,
        lastM3u8: {},
        tsLastRequestVideo: null,
        tsLastRequestMaster: null,
        tsLastRequestAudio: null,
      }
    }
    return this.sessionStates[sessionId];
  }

  get(sessionId) {
    if (!this.sessionStates[sessionId]) {
      this.create(sessionId);  
    }
    return this.sessionStates[sessionId];
  }

  set(sessionId, key, value) {
    if (!this.sessionStates[sessionId]) {
      this.create(sessionId);
    }
    this.sessionStates[sessionId][key] = value;
  }
}

module.exports = {
  SessionState,
  SessionStateStore
}