const SharedStateStore = require('./shared_state_store.js');

const SessionState = Object.freeze({
  VOD_INIT: 1,
  VOD_PLAYING: 2,
  VOD_NEXT_INIT: 3,
  VOD_NEXT_INITIATING: 4,
});

class SessionStateStore extends SharedStateStore {
  constructor(opts) {
    super("session", opts, {
      mediaSeq: 0,
      discSeq: 0,
      vodMediaSeqVideo: 0,
      vodMediaSeqAudio: 0, // assume only one audio group now
      state: SessionState.VOD_INIT,
      lastM3u8: {},
      tsLastRequestVideo: null,
      tsLastRequestMaster: null,
      tsLastRequestAudio: null,
      currentVod: null,
      slateCount: 0,
    });
  }

  create(sessionId) {
    return this.init(sessionId);
  }
}

module.exports = {
  SessionState,
  SessionStateStore
}