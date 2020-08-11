const redis = require("redis");
const debug = require("debug")("engine-state-store");

const REDIS_URL = process.env.REDIS_URL;

class SharedStateStore {
  constructor(type, initData) {
    this.sharedStates = {};
    this.initData = initData;
    this.keyPrefix = `${type}:`;

    this.client = undefined;
    if (REDIS_URL) {
      debug(`Using REDIS for shared state store (${type})`);
      this.client = redis.createClient(REDIS_URL);
    }
  }

  async redisGetAsync(id) {
    const storeKey = "" + this.keyPrefix + id;
    const getAsync = new Promise((resolve, reject) => {
      this.client.get(storeKey, (err, reply) => {
        //debug(`REDIS get ${storeKey}:${reply}`);
        if (!err) {
          resolve(JSON.parse(reply));
        } else {
          reject(err);
        }
        });
    });
    const data = await getAsync;
    return data;
  }

  async redisSetAsync(id, data) {
    const storeKey = "" + this.keyPrefix + id;
    const setAsync = new Promise((resolve, reject) => {
      this.client.set(storeKey, JSON.stringify(data), (err, res) => {
        //debug(`REDIS set ${storeKey}:${JSON.stringify(data)}`);
        if (!err) {
          resolve(data);
        } else {
          reject(err);
        }
      });
    });
    return await setAsync;
  }

  async init(id) {
    if (!this.client) {
      if (!this.sharedStates[id]) {
        this.sharedStates[id] = this.initData;
      }
      return this.sharedStates[id];
    } else {
      let data = await this.redisGetAsync(id);
      if (data === null) {
        data = await this.redisSetAsync(id, this.initData);
      }
      return data;
    }
  }

  async get(id) {
    let data = this.client ? await this.redisGetAsync(id) : this.sharedStates[id];
    if (!data) {
      data = await this.init(id);
    }
    return data;
  }

  async set(id, key, value) {
    let data = this.client ? await this.redisGetAsync(id) : this.sharedStates[id];
    if (!data) {
      data = await this.init(id);
    }
    data[key] = value;
    if (!this.client) {
      this.sharedStates[id] = data;
    } else {
      await this.redisSetAsync(id, data);
    }
    return data;
  }
}

module.exports = SharedStateStore;