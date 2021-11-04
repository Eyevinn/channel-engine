const SharedStateStore = require('./shared_state_store.js');
const debug = require("debug")("session-live-state-store");

class SharedSessionLiveState {
  constructor(store, sessionId, instanceId, opts) {
    this.sessionId = sessionId;
    this.instanceId = instanceId;
    this.store = store;
  }

  async get(key) {
    return await this.store.get(this.sessionId, key);
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
      firstCounts: {
        liveSourceMseqCount: null,
        mediaSeqCount: null,
        discSeqCount: null,
      },
      lastRequestedMediaSeqRaw: null,
      liveSegsForFollowers: null,
      transitSegs: null,
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

  // Should only be called when Channel Engine starts.
  // This is due to the fact that we want to sync leaders between Session and SessionLive.
  async setLeader(instanceId) {
    if (!instanceId) {
      throw new Error("Cannot set leader without instance id");
    }
    debug(`[${instanceId}]: Taking the lead! ${instanceId}`);
    await this.set("", "leader", instanceId);
    this.cache.leader.ts = Date.now();
    this.cache.leader.value = instanceId;
    return instanceId;
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