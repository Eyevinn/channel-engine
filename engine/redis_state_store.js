const redis = require("redis");
const debug = require("debug")("redis-state-store");

class RedisStateStore {
  constructor(keyPrefix, opts) {
    this.keyPrefix = keyPrefix;
    this.client = redis.createClient(opts.redisUrl);
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

  async setAsync(id, data) {
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
}

module.exports = RedisStateStore;