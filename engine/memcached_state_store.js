const MemcacheClient = require("memcache-client");
const debug = require("debug")("memcached-state-store");

class MemcachedStateStore {
  constructor(keyPrefix, opts) {
    this.keyPrefix = keyPrefix;
    this.client = new MemcacheClient({ server: opts.memcachedUrl, cmdTimeout: 10000 });
  }

  async initAsync(id, initData) {
    let data = {};
    for(const key of Object.keys(initData)) {
      debug(`${this.keyPrefix} Initiating key ${key} with init data`);
      data[key] = await this.setAsync(id, key, initData[key]);
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