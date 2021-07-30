const SharedStateStore = require('./shared_state_store.js');
const debug = require("debug")("sessionLive-state-store");


const CURRENT_LIVE_CACHE_TTL = 5 * 60 * 1000; // 5 minutes
class SharedSessionLiveState {
  constructor(store, sessionId, instanceId, opts) {
    this.sessionId = sessionId;
    this.instanceId = instanceId;
    this.cache = {
      liveSourceM3U8s: {
        ts: 0,
        ttl: CURRENT_LIVE_CACHE_TTL,
        value: {},
      }
    };
    if (opts && opts.cacheTTL) {
      this.cacheTTL = opts.cacheTTL;
    } else {
      throw new Error("need to specify cache TTL");
    }
    this.store = store;
  }

  async clearCurrentLiveCache() {
    debug(`[${this.sessionId}]: clearing 'liveSourceM3U8s' cache`);
    this.cache.liveSourceM3U8s.value = [];
  }

  async get(key) {
    return await this.store.get(this.sessionId, key);
  }

  async getLiveSourceM3U8s() {
    if (!this.sessionId) {
      throw new Error("shared session state store has not been initialized");
    }

    if (this.cache.liveSourceM3U8s.value && Date.now() < this.cache.liveSourceM3U8s.ts + this.cache.liveSourceM3U8s.ttl) {
      debug(`[${this.sessionId}]: reading 'liveSourceM3U8s' from cache`);
      return this.cache.liveSourceM3U8s.value;
    }

    const currentLiveM3U8s = await this.get("liveSourceM3U8s");
    let liveSourceM3U8s = null;
    if (currentLiveM3U8s) {
      if (this.store.isShared()) {
        debug(`[${this.sessionId}]: reading from shared store`);
        liveSourceM3U8s = currentLiveM3U8s;
      } else {
        liveSourceM3U8s = currentLiveM3U8s;
      }
    }
    this.cache.liveSourceM3U8s.ts = Date.now();
    this.cache.liveSourceM3U8s.value = liveSourceM3U8s;
    return liveSourceM3U8s;
  }


  async setLiveSourceM3U8s(liveSourceM3U8s, opts) {
    if (!this.sessionId) {
      throw new Error("shared session state store has not been initialized");
    }

    if (this.store.isShared()) {
      let newLiveSourceM3U8s = await this.set("liveSourceM3U8s", JSON.stringify(liveSourceM3U8s));
      const isLeader = await this.store.isLeader(this.instanceId);
      if (!isLeader) {
        debug(`[${this.sessionId}]: not a leader, will not overwrite liveSourceM3U8s in shared store`);
        liveSourceM3U8s = JSON.parse(newLiveSourceM3U8s);
      }
    } else {
      await this.set("liveSourceM3U8s", JSON.stringify(liveSourceM3U8s));
    }
    if (this.cache.liveSourceM3U8s) {
      this.cache.liveSourceM3U8s.ts = Date.now();
      this.cache.liveSourceM3U8s.ttl = CURRENT_LIVE_CACHE_TTL;
      if (opts && opts.ttl) {
        this.cache.liveSourceM3U8s.ttl = opts.ttl;
      }
      debug(`[${this.sessionId}]: TTL for current liveSourceM3U8s is ${this.cache.liveSourceM3U8s.ttl}ms`);
      this.cache.liveSourceM3U8s.value = liveSourceM3U8s;
    }
    return this.cache.liveSourceM3U8s.value;
  }

  async set(key, value) {
    if (await this.store.isLeader(this.instanceId)) {
      return await this.store.set(this.sessionId, key, value);
    } else {
      return await this.store.get(this.sessionId, key);
    }
  }

  async remove(key) {
    await this.store.remove(this.sessionId, key);
  }

}


class SessionLiveStateStore extends SharedStateStore {
  constructor(opts) {
    super("sessionLive", opts, {
      liveSourceM3U8s: {},
      latestMediaSeqSegs: {},
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

  async getLeader() {
    if (this.cache.leader.value) {
      return this.cache.leader.value;
    } else {
      return null;
    }
  }

  async isLeader(instanceId) {
    if (!instanceId) {
      throw new Error("Cannot determine leader without instance id");
    }
    let leader;
    if (this.cache.leader.value && Date.now() < this.cache.leader.ts + this.cacheTTL) {
      leader = this.cache.leader.value;
      debug(`[${instanceId}]: reading 'leader' from cache: I am ${leader === instanceId ? "" : "NOT"} the leader!`);
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
    debug(`[${instanceId}]: I am ${leader === instanceId ? "" : "NOT"} the leader!`);
    this.cache.leader.ts = Date.now();
    this.cache.leader.value = leader;
    return leader === instanceId;
  }

  async create(sessionId, instanceId) {
    debug(`[${sessionId}][${instanceId}]: creating SharedSessionLiveState`);
    await this.init(sessionId);
    return new SharedSessionLiveState(this, sessionId, instanceId, { cacheTTL: this.cacheTTL || 5000 });
  }
}

module.exports = {
  SessionLiveStateStore
}