const SharedStateStore = require('./shared_state_store.js');

const PlayheadState = Object.freeze({
  RUNNING: 1,
  STOPPED: 2,
  CRASHED: 3,
  IDLE: 4
});

class PlayheadStateStore extends SharedStateStore {
  constructor() {
    super({ state: PlayheadState.IDLE });
  }
  
  create(sessionId) {
    return this.init(sessionId);
  }
}

module.exports = {
  PlayheadState,
  PlayheadStateStore
}