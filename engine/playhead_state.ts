const SharedStateStore = require('./shared_state_store.js');

// Type-only migration (#373): a faithful port of the original JS.
// `SharedStateStore` is loaded via CommonJS `require` (erasable syntax) so the
// still-`.js` consumers keep resolving it unchanged; the base type is `any` at
// this interop boundary.

const PlayheadState = Object.freeze({
  RUNNING: 1,
  STOPPED: 2,
  CRASHED: 3,
  IDLE: 4
});

class SharedPlayheadState {
  sessionId: string;
  store: PlayheadStateStore;
  state: number;
  lastM3u8: any;

  constructor(store: PlayheadStateStore, sessionId: string, opts?: any) {
    this.sessionId = sessionId;
    this.store = store;
    this.state = PlayheadState.IDLE;
    this.lastM3u8 = null;
  }

  async get(key: string): Promise<any> {
    return await this.store.get(this.sessionId, key);
  }

  async getState(): Promise<number> {
    return this.state;
  }

  async getLastM3u8(): Promise<any> {
    return this.lastM3u8;
  }

  async getValues(keys: string[]): Promise<Record<string, any>> {
    return await this.store.getValues(this.sessionId, keys);
  }

  async set(key: string, value: any, isLeader?: boolean): Promise<any> {
    if (isLeader) {
      return await this.store.set(this.sessionId, key, value);
    } else {
      return await this.store.get(this.sessionId, key);
    }
  }

  async setValues(keyValues: Record<string, any>, isLeader?: boolean): Promise<Record<string, any>> {
    if (isLeader) {
      return await this.store.setValues(this.sessionId, keyValues);
    } else {
      return await this.store.getValues(this.sessionId, Object.keys(keyValues));
    }
  }

  async setState(newState: number): Promise<number> {
    this.state = newState;
    return this.state;
  }

  async setLastM3u8(m3u8: any): Promise<any> {
    this.lastM3u8 = m3u8;
    return this.lastM3u8;
  }
}

class PlayheadStateStore extends SharedStateStore {
  constructor(opts?: any) {
    super("playhead", opts, {
      state: PlayheadState.IDLE,
      tickInterval: opts ? (opts.averageSegmentDuration/1000) : 3,
      mediaSeq: 0,
      vodMediaSeqVideo: 0,
      vodMediaSeqAudio: 0,
      vodMediaSeqSubtitle: 0,
    });
  }

  async create(sessionId: string): Promise<SharedPlayheadState> {
    await this.init(sessionId);
    return new SharedPlayheadState(this, sessionId);
  }
}

module.exports = {
  PlayheadState,
  PlayheadStateStore
};
