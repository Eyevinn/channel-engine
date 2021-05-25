const redis = require("redis");
const debug = require("debug")("redis-state-store");

class RedisStateStore {
  constructor(keyPrefix, opts) {
    this.keyPrefix = keyPrefix;
    this.client = redis.createClient(opts.redisUrl);
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

  async setAsync(id, key, value) {
    const storeKey = "" + this.keyPrefix + id + key;
    const setAsync = new Promise((resolve, reject) => {
      this.client.set(storeKey, JSON.stringify(value), (err, res) => {
        //debug(`REDIS set ${storeKey}:${JSON.stringify(data)}`);
        if (!err) {
          resolve(value);
        } else {
          reject(err);
        }
      });
    });
    return await setAsync;
  }  
}

module.exports = RedisStateStore;