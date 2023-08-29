const HLSVod = require('@eyevinn/hls-vodtolive');
const SharedStateStore = require('./shared_state_store.js');
const debug = require("debug")("session-state-store");
const { timeLeft } = require('./util.js');

const SessionState = Object.freeze({
  VOD_INIT: 1,
  VOD_PLAYING: 2,
  VOD_NEXT_INIT: 3,
  VOD_NEXT_INITIATING: 4,
  VOD_RELOAD_INIT: 5,
  VOD_RELOAD_INITIATING: 6
});

const CURRENTVOD_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

class SharedSessionState {
  constructor(store, sessionId, instanceId, opts) {
    this.sessionId = sessionId;
    this.instanceId = instanceId;
    this.cache = {
      currentVod: {
        ts: 0,
        ttl: CURRENTVOD_CACHE_TTL,
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

  async clearCurrentVodCache() {
    debug(`[${this.sessionId}]: clearing 'currentVod' cache`);
    this.cache.currentVod.value = null;
  }

  async getCurrentVod() {
    if (!this.sessionId) {
      throw new Error("shared session state store has not been initialized");
    }

    if (this.cache.currentVod.value && Date.now() < this.cache.currentVod.ts + this.cache.currentVod.ttl) {
      debug(
        `[${this.sessionId}]: reading 'currentVod' from cache. Expires in ${timeLeft(
          this.cache.currentVod.ts + this.cache.currentVod.ttl,
          Date.now()
        )}`
      );
      return this.cache.currentVod.value;
    }

    const currentVod = await this.get("currentVod");
    let hlsVod = null;
    if (currentVod) {
      if (this.store.isShared()) {
        const strToMB = (str) => {
          const bytesPerCharacter = 2; // Assuming each character takes around 2 bytes in memory
          const stringSizeBytes = str.length * bytesPerCharacter;
          const sizeInMegabytes = stringSizeBytes / (1024 * 1024);
          return sizeInMegabytes.toFixed(1);
        }
        debug(`[${this.sessionId}]: reading ${currentVod.length} characters or (${strToMB(currentVod)} MB) from shared store (${Date.now()} < ${this.cache.currentVod.ts + this.cache.currentVod.ttl})`);
        hlsVod = new HLSVod();
        hlsVod.fromJSON(currentVod);
        if (hlsVod.skipSerializeMediaSequences) {
          await hlsVod.generateMediaSequences();
        }
      } else {
        hlsVod = currentVod;
      }
    }
    this.cache.currentVod.ts = Date.now();
    this.cache.currentVod.value = hlsVod;
    return hlsVod;
  }

  async setCurrentVod(hlsVod, opts) {
    if (!this.sessionId) {
      throw new Error("shared session state store has not been initialized");
    }
    if (this.store.isShared()) {
      await this.store.clearLeaderCache();
      const isLeader = await this.store.isLeader(this.instanceId);
      if (isLeader) {
        await this.set("currentVod", hlsVod.toJSON());
      } else {
        debug(`[${this.sessionId}]: Not a leader. Will not overwrite. Getting currentVod in shared store`);
        const currentVod = await this.get("currentVod");
        hlsVod = new HLSVod();
        hlsVod.fromJSON(currentVod);
        if (hlsVod.skipSerializeMediaSequences) {
          await hlsVod.generateMediaSequences();
        }
      }
    } else {
      await this.set("currentVod", hlsVod);
    }
    if (this.cache.currentVod) {
      this.cache.currentVod.ts = Date.now();
      this.cache.currentVod.ttl = CURRENTVOD_CACHE_TTL;
      if (opts && opts.ttl) {
        this.cache.currentVod.ttl = opts.ttl;
      }
      debug(`[${this.sessionId}]: TTL for current VOD is ${this.cache.currentVod.ttl}ms`);
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
    if (await this.store.isLeader(this.instanceId)) {
      return await this.store.set(this.sessionId, key, value);
    } else {
      return await this.store.get(this.sessionId, key);
    }
  }

  async setValues(keyValues) {
    if (await this.store.isLeader(this.instanceId)) {
      return await this.store.setValues(this.sessionId, keyValues);
    } else {
      return await this.store.getValues(this.sessionId, Object.keys(keyValues));
    }
  }
  
  async remove(key) {
    await this.store.remove(this.sessionId, key);
  }

  async increment(key, inc) {
    let value = await this.get(key);
    if (await this.store.isLeader(this.instanceId)) {
      let valueToIncrement = inc === 0 ? 0 : inc ? inc : 1;
      debug(`[${this.sessionId}]: I am incrementing key ${key} with ${valueToIncrement}`);
      value += valueToIncrement;
      return await this.store.set(this.sessionId, key, value);
    } else {
      return value;
    }
  }
}

class SessionStateStore extends SharedStateStore {
  constructor(opts) {
    super("session", opts, {
      mediaSeq: 0,
      discSeq: 0,
      mediaSeqAudio: 0,
      discSeqAudio: 0,
      mediaSeqSubtitle: 0,
      discSeqSubtitle: 0,
      vodMediaSeqVideo: 0,
      vodMediaSeqAudio: 0, // assume only one audio group now
      vodMediaSeqSubtitle: 0, // assume only one subtitle group now
      state: SessionState.VOD_INIT,
      lastM3u8: {},
      tsLastRequestVideo: null,
      tsLastRequestMaster: null,
      tsLastRequestAudio: null,
      tsLastRequestSubtitle: null,
      currentVod: null,
      slateCount: 0,
      assetId: "",
      vodReloaded: 0,
    });
    if (opts && opts.cacheTTL) {
      this.cacheTTL = opts.cacheTTL;
    }
    this.cache = {
      leader: {
        ts: 0,
        value: null
      }
    };
  }

  async ping(instanceId) {
    let t = Date.now();
    await this.setVolatile("", instanceId, t);
  }

  async clearLeaderCache() {
    debug(`[${this.instanceId}]: clearing 'leader' cache`);
    this.cache.leader.value = null;
  }

  async isLeader(instanceId) {
    if (!instanceId) {
      throw new Error("Cannot determine leader without instance id");
    }
    let leader;
    if (this.cache.leader.value && Date.now() < this.cache.leader.ts + this.cacheTTL) {
      leader = this.cache.leader.value;
      debug(`[${instanceId}]: reading 'leader' from cache: I am${leader === instanceId ? "" : " NOT"} the leader!`);
      return leader === instanceId;
    }
    leader = await this.get("", "leader");
    if (!leader) {
      leader = instanceId;
      debug(`[${instanceId}]: We have a new leader! ${instanceId}`)
      await this.set("", "leader", instanceId);
    }
    // Check whether leader is actually alive only if I am not the leader
    if (leader !== instanceId) {
      debug(`[${instanceId}]: Checking whether leader ${leader} is alive`);
      const lastSeen = await this.get("", leader); // we don't have per session pings
      if (!lastSeen) {
        leader = instanceId;
        debug(`[${instanceId}]: Current leader is missing, taking the lead! ${leader}`);
        await this.set("", "leader", leader);
      } else {
        if (Date.now() - lastSeen > 30000) {
          leader = instanceId;
          debug(`[${instanceId}]: Current leader hasn't been seen for the last 30 sec, taking the lead! ${leader}`);
          await this.set("", "leader", leader);
        }
      }
    }
    debug(`[${instanceId}]: I am${leader === instanceId ? "" : " NOT"} the leader!`);
    this.cache.leader.ts = Date.now();
    this.cache.leader.value = leader;
    return leader === instanceId;
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