const HLSVod = require('@eyevinn/hls-vodtolive');
const SharedStateStore = require('./shared_state_store.js');
const debug = require("debug")("session-state-store");

const SessionState = Object.freeze({
  VOD_INIT: 1,
  VOD_PLAYING: 2,
  VOD_NEXT_INIT: 3,
  VOD_NEXT_INITIATING: 4,
});

class SharedSessionState {
  constructor(store, sessionId, opts) {
    this.sessionId = sessionId;
    this.cache = {
      currentVod: {
        ts: 0,
        value: null
      }
    };
    if (opts && opts.cacheTTL) {
      this.cacheTTL = opts.cacheTTL;
    } else {
      throw new Error("need to specify cache TTL");
    }
    this.store = store;
  }

  async getCurrentVod() {
    if (!this.sessionId) {
      throw new Error("shared session state store has not been initialized");
    }

    if (Date.now() < this.cache.currentVod.ts + this.cacheTTL) {
      debug(`${this.sessionId}: reading 'currentVod' from cache`);
      return this.cache.currentVod.value;
    }

    const currentVod = await this.get("currentVod");
    let hlsVod = null;
    if (currentVod) {
      if (this.store.isShared()) {
        debug(`${this.sessionId}: reading ${currentVod.length} characters from shared store (${Date.now()} < ${this.cache.currentVod.ts + this.cacheTTL})`);
        hlsVod = new HLSVod();
        hlsVod.fromJSON(currentVod);
      } else {
        hlsVod = currentVod;
      }
    }
    this.cache.currentVod.ts = Date.now();
    return hlsVod;
  }

  async setCurrentVod(hlsVod) {
    if (!this.sessionId) {
      throw new Error("shared session state store has not been initialized");
    }

    if (this.store.isShared()) {
      await this.set("currentVod", hlsVod.toJSON());
    } else {
      await this.set("currentVod", hlsVod);
    }
    if (this.cache.currentVod) {
      this.cache.currentVod.ts = Date.now();
      this.cache.currentVod.value = hlsVod;
    }
    return this.cache.currentVod.value;
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
      assetId: ""
    });
    if (opts && opts.cacheTTL) {
      this.cacheTTL = opts.cacheTTL;
    }
  }

  async create(sessionId) {
    await this.init(sessionId);
    return new SharedSessionState(this, sessionId, { cacheTTL: this.cacheTTL || 5000 });
  }
}

module.exports = {
  SessionState,
  SessionStateStore
}