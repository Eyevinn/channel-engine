const SharedStateStore = require('./shared_state_store.js');

const PlayheadState = Object.freeze({
  RUNNING: 1,
  STOPPED: 2,
  CRASHED: 3,
  IDLE: 4
});

class SharedPlayheadState {
  constructor(store, sessionId, opts) {
    this.sessionId = sessionId;
    this.store = store;
  }

  async get(key) {
    return await this.store.get(this.sessionId, key);
  }

  async getValues(keys) {
    return await this.store.getValues(this.sessionId, keys);
  }

  async set(key, value) {
    return await this.store.set(this.sessionId, key, value);
  }  
}

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
    return new SharedPlayheadState(this, sessionId);
  }
}

module.exports = {
  PlayheadState,
  PlayheadStateStore
}