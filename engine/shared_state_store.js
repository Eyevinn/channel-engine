const debug = require("debug")("engine-state-store");

const RedisStateStore = require("./redis_state_store.js");
const MemcachedStateStore = require("./memcached_state_store.js");
const MemoryStateStore = require("./memory_state_store.js");

class SharedStateStore {
  constructor(type, opts, initData) {
    this.initData = initData;
    this.type = type;
    this.cache = {};
    this.cacheTTL = opts && opts.cacheTTL ? opts.cacheTTL : 1000;

    this.shared = false;
    if (opts && opts.redisUrl) {
      debug(`Using REDIS (${opts.redisUrl}) for shared state store (${type}, cacheTTL=${this.cacheTTL})`);
      this.store = new RedisStateStore(`${type}:`, opts);
      this.shared = true;
    } else if (opts && opts.memcachedUrl) {
      debug(`Using MEMCACHED (${opts.memcachedUrl}) for shared state store (${type}, cacheTTL=${this.cacheTTL})`);
      this.store = new MemcachedStateStore(`${type}:`, opts);
      this.shared = true;
    } else {
      debug(`Using MEMORY for non-shared state store (${type}, cacheTTL=${this.cacheTTL})`);
      this.store = new MemoryStateStore(`${type}:`, opts);
    }

    if (this.shared) {
      this.cacheInvalidator = setInterval(async () => {
        debug(`${this.type}: Invalidating shared store cache and writing to shared store`);
        const ids = Object.keys(this.cache);
        for (let id of ids) {
          debug(`${this.type}:${id} Writing to shared store`);
          const data = this.cache[id];
          await this.store.setAsync(id, data);
        }
        this.cache = {};
      }, this.cacheTTL);
    }
  }

  isShared() {
    return this.shared;
  }

  async init(id) {
    this.cache[id] = await this.store.initAsync(id, this.initData);
  }

  async get(id) {
    let data;
    if (this.cache[id]) {
      data = this.cache[id];
    } else {
      debug(`${this.type}:${id} Reading from shared store`);
      data = await this.store.getAsync(id);
    }
    if (!data) {
      data = await this.init(id);
    }
    this.cache[id] = data;
    return data;
  }

  async set(id, key, value) {
    let data;
    if (this.cache[id]) {
      data = this.cache[id];
    } else {
      debug(`${this.type}:${id} Reading from shared store`);
      data = await this.store.getAsync(id);
      if (!data) {
        data = await this.init(id);
      }
    }
    data[key] = value;
    this.cache[id] = data;
    return data;
  }
}

module.exports = SharedStateStore;