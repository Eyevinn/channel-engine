const redis = require("redis");
const REDIS_URL = process.env.REDIS_URL;

class SharedStateStore {
  constructor(type, initData) {
    this.sharedStates = {};
    this.initData = initData;
    this.keyPrefix = `${type}:`;

    this.client = undefined;
    if (REDIS_URL) {
      this.client = redis.createClient(REDIS_URL);
    }
  }

  init(id) {
    if (!this.sharedStates[id]) {
      this.sharedStates[id] = this.initData;
    }
  }

  async get(id) {
    const key = "" + this.keyPrefix + id;
    if (!this.sharedStates[id]) {
      this.init(id);  
    }
    return this.sharedStates[id];
  }

  async set(id, key, value) {
    const storeKey = "" + this.keyPrefix + id;
    if (!this.sharedStates[id]) {
      this.init(id);
    }
    this.sharedStates[id][key] = value;
    return this.sharedStates[id];
  }
}

module.exports = SharedStateStore;