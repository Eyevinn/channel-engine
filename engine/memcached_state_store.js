const MemcacheClient = require("memcache-client");
const debug = require("debug")("memcached-state-store");

class MemcachedStateStore {
  constructor(keyPrefix, opts) {
    this.keyPrefix = keyPrefix;
    this.client = new MemcacheClient({ server: opts.memcachedUrl, cmdTimeout: 10000 });
  }

  async initAsync(id, initData) {
    let data = await this.getAsync(id);
    if (data === null) {
      data = await this.setAsync(id, initData);
    }
    return data;
  }

  async getAsync(id) {
    const storeKey = "" + this.keyPrefix + id;
    const data = await this.client.get(storeKey);
    if (data) {
      return JSON.parse(data.value);
    }
    return null;
  }

  async setAsync(id, data) {
    const storeKey = "" + this.keyPrefix + id;
    await this.client.set(storeKey, JSON.stringify(data));
  }
}

module.exports = MemcachedStateStore;