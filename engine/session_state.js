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
  constructor(store, sessionId, instanceId, opts) {
    this.sessionId = sessionId;
    this.instanceId = instanceId;
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
    this.cache.currentVod.value = hlsVod;
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
    const values = await this.store.getValues(this.sessionId, keys);
    //debug(values);
    return values;
  }

  async set(key, value) {
    if (await this.isLeader()) {
      return await this.store.set(this.sessionId, key, value);
    } else {
      return await this.store.get(this.sessionId, key);
    }
  }
  
  async remove(key) {
    await this.store.remove(this.sessionId, key);
  }

  async increment(key, inc) {
    let value = await this.get(key);
    if (await this.isLeader()) {
      let valueToIncrement = inc || 1;
      debug(`[${this.sessionId}]: I am incrementing key ${key} with ${valueToIncrement}`);
      value += valueToIncrement;
      return await this.store.set(this.sessionId, key, value);
    } else {
      return value;
    }
  }

  async ping() {
    await this.store.setVolatile(this.sessionId, this.instanceId, Date.now());
  }

  async isLeader() {
    let leader = await this.store.get(this.sessionId, "leader");
    if (!leader) {      
      leader = this.instanceId;
      debug(`[${this.sessionId}]: We have a new leader! ${this.instanceId}`)
      await this.store.set(this.sessionId, "leader", this.instanceId);
    }
    // Check whether leader is actually alive
    const lastSeen = await this.store.get(this.sessionId, leader);
    if (!lastSeen) {
      leader = this.instanceId;
      debug(`[${this.sessionId}]: Current leader is missing, taking the lead! ${this.instanceId}`);
      await this.store.set(this.sessionId, "leader", this.instanceId);
    }
    return leader === this.instanceId;
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

  async create(sessionId, instanceId) {
    await this.init(sessionId);
    return new SharedSessionState(this, sessionId, instanceId, { cacheTTL: this.cacheTTL || 5000 });
  }
}

module.exports = {
  SessionState,
  SessionStateStore
}