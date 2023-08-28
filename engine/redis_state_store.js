const Redis = require("ioredis");
const debug = require("debug")("redis-state-store");

const DEFAULT_VOLATILE_KEY_TTL = 5; // Timeout so it should not expire within one normal increment iteration (in seconds)

class RedisStateStore {
  constructor(keyPrefix, opts) {
    this.keyPrefix = keyPrefix;
    if (opts.version) {
      const prependPrefix = opts.version.replace(/\./g, "X");
      this.keyPrefix = prependPrefix + this.keyPrefix;
      debug(`Prepending keyprefix with ${prependPrefix} => ${this.keyPrefix}`);
    }
    this.volatileKeyTTL = DEFAULT_VOLATILE_KEY_TTL;
    if (opts.volatileKeyTTL) {
      debug(`Overriding default, volatileKeyTTL=${opts.volatileKeyTTL}s`);
      this.volatileKeyTTL = opts.volatileKeyTTL;
    }
    this.client = new Redis(opts.redisUrl, { enableAutoPipelining: true });
  }

  async initAsync(id, initData) {
    const isInitiated = await this.getAsync(id, "_initiated");
    let data = {};
    if (!isInitiated) {
      debug(`${this.keyPrefix}:${id}: Initiating keys ${Object.keys(initData)} with init data`);
      await this.setValues(id, initData);
      await this.setAsync(id, "_initiated", true);
    } else {
      debug(`${this.keyPrefix}:${id}: Already initiated, not initiating with init data`);
      for (const key of Object.keys(initData)) {
        debug(`${this.keyPrefix}:${id}: Initiating key ${key} with data from store`);
        data[key] = await this.getAsync(id, key);
        //debug(`${this.keyPrefix}:${id}: Key ${key} initiated with data from store: ${data[key]}`);
      }
    }
    return data;
  }

  async resetAsync(id, initData) {
      await this.setAsync(id, "_initiated", false);
      await this.initAsync(id, initData);
  }

  async resetAllAsync() {
    const resetAsync = new Promise((resolve, reject) => {
      this.client.flushall((err, reply) => {
        if (!err) {
          console.log("Flushed Redis db: ", reply);
          resolve();
        } else {
          reject(err);
        }
      });
    });
    await resetAsync;
  }

  async getValues(id, keys) {
    const pipeline = this.client.pipeline();
    let data = {};
    for (const key of keys) {
      const storeKey = "" + this.keyPrefix + id + key;
      pipeline.get(storeKey, (err, reply) => {
        if (!err) {
          debug(`REDIS get(pipeline) ${storeKey}:${reply ? reply.length + " chars" : "null"}`);
          try {
            data[key] = JSON.parse(reply);
          } catch (err) {
            console.error(`REDIS get(pipeline): Failed to parse ${storeKey} data: '${reply}'`);
          }
        }
      });
    }
    await pipeline.exec();
    return data;
  }

  async getAsync(id, key) {
    const startMs = Date.now();
    const storeKey = "" + this.keyPrefix + id + key;
    const getAsync = new Promise((resolve, reject) => {
      this.client.get(storeKey, (err, reply) => {
        const ioTimeMs = Date.now() - startMs;
        debug(`REDIS get ${storeKey}:${reply ? reply.length + " chars" : "null"} (${ioTimeMs}ms) ${ioTimeMs > 1000 ? 'REDISSLOW!' : ''}`);
        if (!err) {
          let data;
          try {
            data = JSON.parse(reply);
          } catch (err) {
            console.error(`REDIS get: Failed to parse ${storeKey} data: '${reply}'`);
          }
          resolve(data);
        } else {
          reject(err);
        }
      });
    });
    const data = await getAsync;
    return data;
  }

  async setValues(id, data) {
    const returnData = {};
    const pipeline = this.client.pipeline();
    for (const key of Object.keys(data)) {
      const storeKey = "" + this.keyPrefix + id + key;
      const value = data[key];
      pipeline.set(storeKey, JSON.stringify(value), (err, res) => {
        if (!err) {
          debug(`REDIS set(pipeline) ${storeKey}: ${res}`);
          returnData[key] = value;
        }
      });
    }
    await pipeline.exec();
    return returnData;
  }

  async setAsync(id, key, value) {
    const startMs = Date.now();
    const storeKey = "" + this.keyPrefix + id + key;
    const setAsync = new Promise((resolve, reject) => {
      this.client.set(storeKey, JSON.stringify(value), (err, res) => {
        const ioTimeMs = Date.now() - startMs;
        debug(`REDIS set ${storeKey}: ${res} (${ioTimeMs}ms) ${ioTimeMs > 1000 ? "REDISSLOW!" : ""}`);
        if (!err) {
          resolve(value);
        } else {
          reject(err);
        }
      });
    });
    return await setAsync;
  }

  async setVolatileAsync(id, key, value) {
    const data = await this.setAsync(id, key, value);
    const storeKey = "" + this.keyPrefix + id + key;
    const expireAsync = new Promise((resolve, reject) => {
      this.client.expire(storeKey, this.volatileKeyTTL, (err, res) => {
        if (!err) {
          debug(`REDIS expire ${storeKey} ${this.volatileKeyTTL}s: ${res === 1 ? "OK" : "KEY DOES NOT EXIST"}`);
          resolve();
        } else {
          reject(err);
        }
      });
    });
    await expireAsync;
    return data;
  }

  async removeAsync(id, key) {
    const startMs = Date.now();
    const storeKey = "" + this.keyPrefix + id + key;
    const delAsync = new Promise((resolve, reject) => {
      this.client.del(storeKey, (err, res) => {
        const ioTimeMs = Date.now() - startMs;
        debug(`REDIS remove ${storeKey}: (${ioTimeMs}ms) ${ioTimeMs > 1000 ? "REDISSLOW!" : ""}`);
        if (!err) {
          resolve();
        } else {
          reject(err);
        }
      });
    });
    await delAsync;
  }
}

module.exports = RedisStateStore;