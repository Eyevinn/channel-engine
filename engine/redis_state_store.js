const Redis = require("ioredis");
const debug = require("debug")("redis-state-store");
const { cloudWatchLog } = require("./util.js");

const DEFAULT_VOLATILE_KEY_TTL = 5; // Timeout so it should not expire within one normal increment iteration (in seconds)

function isTrue(s) {
  const regex = /^\s*(true|1)\s*$/i;
  return regex.test(s);
}

const REDIS_VERBOSE_LOG = process.env.REDIS_VERBOSE_LOG ? isTrue(process.env.REDIS_VERBOSE_LOG) : false;
const REDIS_POOL_SIZE = process.env.REDIS_POOL_SIZE ? parseInt(process.env.REDIS_POOL_SIZE) : 30;

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
    this.pool = this.createRedisPool(REDIS_POOL_SIZE, opts.redisUrl);
  }

  createRedisPool(size, redisUrl) {
    const pool = [];
    for (let i = 0; i < size; i++) {
      const client = new Redis(redisUrl);
      pool.push(client);
    }
    return pool;
  }

  getClientFromPool() {
    return this.pool.pop();
  }

  returnClientToPool(client) {
    this.pool.push(client);
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
      }
    }
    return data;
  }

  async resetAsync(id, initData) {
    await this.setAsync(id, "_initiated", false);
    await this.initAsync(id, initData);
  }

  async resetAllAsync() {
    const client = this.getClientFromPool();
    try {
      await client.flushall();
      console.log("Flushed Redis db");
    } catch (err) {
      console.error("Error flushing Redis db:", err);
    } finally {
      this.returnClientToPool(client);
    }
  }

  async getValues(id, keys) {
    const client = this.getClientFromPool();
    const pipeline = client.pipeline();
    let data = {};
    const startMs = Date.now();

    for (const key of keys) {
      const storeKey = `${this.keyPrefix}${id}${key}`;
      pipeline.get(storeKey);
    }

    const results = await pipeline.exec();
    const ops = pipeline.length;

    results.forEach((result, index) => {
      const reply = result[1];
      const storeKey = `${this.keyPrefix}${id}${keys[index]}`;
      debug(`REDIS get(pipeline) ${storeKey}:${reply ? reply.length + " chars" : "null"}`);
      if (reply) {
        try {
          data[keys[index]] = JSON.parse(reply);
        } catch (err) {
          console.error(`REDIS get(pipeline): Failed to parse ${storeKey} data: '${reply}'`);
        }
      }
    });

    const ioTimeMs = Date.now() - startMs;
    cloudWatchLog(!REDIS_VERBOSE_LOG, "redis", { event: "getValues", operations: ops, ioTimeMs: ioTimeMs });
    this.returnClientToPool(client);
    return data;
  }

  async getAsync(id, key) {
    const client = this.getClientFromPool();
    const startMs = Date.now();
    const storeKey = `${this.keyPrefix}${id}${key}`;
    const reply = await client.get(storeKey);
    const ioTimeMs = Date.now() - startMs;

    debug(`REDIS get ${storeKey}:${reply ? reply.length + " chars" : "null"} (${ioTimeMs}ms) ${ioTimeMs > 1000 ? "REDISSLOW!" : ""}`);
    this.returnClientToPool(client);

    if (reply) {
      try {
        cloudWatchLog(!REDIS_VERBOSE_LOG, "redis", { event: "get", operations: 1, ioTimeMs: ioTimeMs });
        return JSON.parse(reply);
      } catch (err) {
        console.error(`REDIS get: Failed to parse ${storeKey} data: '${reply}'`);
      }
    }
    return null;
  }

  async setValues(id, data) {
    const client = this.getClientFromPool();
    const returnData = {};
    const startMs = Date.now();
    const pipeline = client.pipeline();

    for (const key of Object.keys(data)) {
      const storeKey = `${this.keyPrefix}${id}${key}`;
      const value = data[key];
      pipeline.set(storeKey, JSON.stringify(value));
    }

    const results = await pipeline.exec();
    const ops = pipeline.length;

    results.forEach((result, index) => {
      const storeKey = `${this.keyPrefix}${id}${Object.keys(data)[index]}`;
      debug(`REDIS set(pipeline) ${storeKey}: ${result[1]}`);
      returnData[Object.keys(data)[index]] = data[Object.keys(data)[index]];
    });

    const ioTimeMs = Date.now() - startMs;
    cloudWatchLog(!REDIS_VERBOSE_LOG, "redis", { event: "setValues", operations: ops, ioTimeMs: ioTimeMs });
    this.returnClientToPool(client);
    return returnData;
  }

  async setAsync(id, key, value) {
    const client = this.getClientFromPool();
    const startMs = Date.now();
    const storeKey = `${this.keyPrefix}${id}${key}`;
    const res = await client.set(storeKey, JSON.stringify(value));
    const ioTimeMs = Date.now() - startMs;

    debug(`REDIS set ${storeKey}: ${res} (${ioTimeMs}ms) ${ioTimeMs > 1000 ? "REDISSLOW!" : ""}`);
    cloudWatchLog(!REDIS_VERBOSE_LOG, "redis", { event: "set", operations: 1, ioTimeMs: ioTimeMs });
    this.returnClientToPool(client);
    return value;
  }

  async setVolatileAsync(id, key, value) {
    const data = await this.setAsync(id, key, value);
    const storeKey = `${this.keyPrefix}${id}${key}`;
    const client = this.getClientFromPool();

    await client.expire(storeKey, this.volatileKeyTTL);
    debug(`REDIS expire ${storeKey} ${this.volatileKeyTTL}s`);
    this.returnClientToPool(client);
    return data;
  }

  async removeAsync(id, key) {
    const client = this.getClientFromPool();
    const startMs = Date.now();
    const storeKey = `${this.keyPrefix}${id}${key}`;
    const res = await client.del(storeKey);
    const ioTimeMs = Date.now() - startMs;

    debug(`REDIS remove ${storeKey}: (${ioTimeMs}ms) ${ioTimeMs > 1000 ? "REDISSLOW!" : ""}`);
    cloudWatchLog(!REDIS_VERBOSE_LOG, "redis", { event: "remove", operations: 1, ioTimeMs: ioTimeMs });
    this.returnClientToPool(client);
    return res;
  }
}

module.exports = RedisStateStore;
