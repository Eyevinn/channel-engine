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
  }

  isShared() {
    return this.shared;
  }

  async init(id) {
    await this.store.initAsync(id, this.initData);
  }

  async reset(id) {
    await this.store.resetAsync(id, this.initData);
  }

  async get(id, key) {
    //debug(`${this.type}:${id}:${key} Reading from shared store`);
    let data = await this.store.getAsync(id, key);
    //debug(key !== "currentVod" ? data : (data ? "not null" : "null" ));
    return data;
  }

  async set(id, key, value) {
    //debug(`${this.type}:${id}:${key} Writing to shared store`);
    const data = await this.store.setAsync(id, key, value);
    return data;
  }

  async getValues(id, keys) {
    let data = {};
    for(const key of keys) {
      data[key] = await this.get(id, key);
    }
    return data;
  }
}

module.exports = SharedStateStore;