const MemcacheClient = require("memcache-client");
const debug = require("debug")("memcached-state-store");

class MemcachedStateStore {
  constructor(keyPrefix, opts) {
    this.keyPrefix = keyPrefix;
    if (opts.version) {
      const prependPrefix = opts.version.replace(/\./g, "X");
      this.keyPrefix = prependPrefix + this.keyPrefix;
      debug(`Prepending keyprefix with ${prependPrefix} => ${this.keyPrefix}`);
    }
    this.client = new MemcacheClient({ server: opts.memcachedUrl, cmdTimeout: 10000 });
  }

  async initAsync(id, initData) {
    const isInitiated = await this.getAsync(id, "_initiated");
    let data = {};
    if (!isInitiated) {
      for(const key of Object.keys(initData)) {
        debug(`${this.keyPrefix}:${id}: Initiating key ${key} with init data`);
        data[key] = await this.setAsync(id, key, initData[key]);
      }
      await this.setAsync(id, "_initiated", true);
    } else {
      debug(`${this.keyPrefix}:${id}: Already initiated, not initiating with init data`);
      for(const key of Object.keys(initData)) {
        debug(`${this.keyPrefix}:${id}: Initiating key ${key} with data from store`);
        data[key] = await this.getAsync(id, key);
      }
    }
    return data;
  }

  async getAsync(id, key) {
    const storeKey = "" + this.keyPrefix + id + key;
    const data = await this.client.get(storeKey);
    if (data) {
      return JSON.parse(data.value);
    }
    return null;
  }

  async setAsync(id, key, value) {
    const storeKey = "" + this.keyPrefix + id + key;
    await this.client.set(storeKey, JSON.stringify(value));
    return value;
  }
}

module.exports = MemcachedStateStore;