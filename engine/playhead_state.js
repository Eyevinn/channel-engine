const PlayheadState = Object.freeze({
  RUNNING: 1,
  STOPPED: 2,
  CRASHED: 3,
  IDLE: 4
});

class PlayheadStateStore {
  
}

module.exports = {
  PlayheadState,
  PlayheadStateStore
}