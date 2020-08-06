const PlayheadState = Object.freeze({
  RUNNING: 1,
  STOPPED: 2,
  CRASHED: 3,
  IDLE: 4
});

class PlayheadStateStore {
  constructor() {
    this.playheadStates = {};
  }
  
  create(sessionId) {
    if (!this.playheadStates[sessionId]) {
      this.playheadStates[sessionId] = {
        state: PlayheadState.IDLE,
      }
    }
    return this.playheadStates[sessionId];    
  }

  get(sessionId) {
    if (!this.playheadStates[sessionId]) {
      this.create(sessionId);  
    }
    return this.playheadStates[sessionId];
  }

  set(sessionId, key, value) {
    if (!this.playheadStates[sessionId]) {
      this.create(sessionId);
    }
    this.playheadStates[sessionId][key] = value;
  }
}

module.exports = {
  PlayheadState,
  PlayheadStateStore
}