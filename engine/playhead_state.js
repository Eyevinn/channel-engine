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
    this.state = PlayheadState.IDLE;
    this.lastM3u8 = null;
  }

  async get(key) {
    return await this.store.get(this.sessionId, key);
  }

  async getState() {
    return this.state;
  }

  async getLastM3u8() {
    return this.lastM3u8;
  }

  async getValues(keys) {
    return await this.store.getValues(this.sessionId, keys);
  }

  async set(key, value, isLeader) {
    if (isLeader) {
      return await this.store.set(this.sessionId, key, value);
    } else {
      return await this.store.get(this.sessionId, key);
    }
  }

  async setValues(keyValues, isLeader) {
    if (isLeader) {
      return await this.store.setValues(this.sessionId, keyValues);
    } else {
      return await this.store.getValues(this.sessionId, Object.keys(keyValues));
    }
  }

  async setState(newState) {
    this.state = newState;
    return this.state;
  }

  async setLastM3u8(m3u8) {
    this.lastM3u8 = m3u8;
    return this.lastM3u8;
  }
}

class PlayheadStateStore extends SharedStateStore {
  constructor(opts) {
    super("playhead", opts, { 
      state: PlayheadState.IDLE, 
      tickInterval: opts ? (opts.averageSegmentDuration/1000) : 3,  
      mediaSeq: 0,
      vodMediaSeqVideo: 0,
      vodMediaSeqAudio: 0,
      vodMediaSeqSubtitle: 0,
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