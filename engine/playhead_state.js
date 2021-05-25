const SharedStateStore = require('./shared_state_store.js');

const PlayheadState = Object.freeze({
  RUNNING: 1,
  STOPPED: 2,
  CRASHED: 3,
  IDLE: 4
});

class PlayheadStateStore extends SharedStateStore {
  constructor(opts) {
    super("playhead", opts, { 
      state: PlayheadState.IDLE, 
      tickInterval: 3, 
      mediaSeq: 0,
      vodMediaSeqVideo: 0,
      vodMediaSeqAudio: 0, 
    });
  }
  
  async create(sessionId) {
    await this.init(sessionId);
  }
}

module.exports = {
  PlayheadState,
  PlayheadStateStore
}