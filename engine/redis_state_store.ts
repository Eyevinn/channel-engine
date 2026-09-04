const Redis = require("ioredis");
const debug = require("debug")("redis-state-store");
const { cloudWatchLog, timer } = require("./util.js");

import type { IStateStore } from "./state_store_types";

// Type-only migration (#373): a faithful port of the original JS.

const DEFAULT_VOLATILE_KEY_TTL = 5; // Timeout so it should not expire within one normal increment iteration (in seconds)

function isTrue(s: any): boolean {
  const regex = /^\s*(true|1)\s*$/i;
  return regex.test(s);
}

const REDIS_VERBOSE_LOG = process.env.REDIS_VERBOSE_LOG ? isTrue(process.env.REDIS_VERBOSE_LOG) : false;
const REDIS_POOL_SIZE = process.env.REDIS_POOL_SIZE ? parseInt(process.env.REDIS_POOL_SIZE) : 15;

class RedisStateStore implements IStateStore {
  keyPrefix: string;
  volatileKeyTTL: number;
  pool: any[];

  constructor(keyPrefix: string, opts: any) {
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

  createRedisPool(size: number, redisUrl: string): any[] {
    const pool: any[] = [];
    for (let i = 0; i < size; i++) {
      const client = new Redis(redisUrl);
      pool.push(client);
    }
    return pool;
  }

  async getClientFromPool(maxWaitTime: number = 5000): Promise<any> {
    const startTime = Date.now();
    while (this.pool.length === 0) {
      if (Date.now() - startTime > maxWaitTime) {
        console.error("[!] Timeout: No available Redis clients in the pool");
      }
      await timer(100);
    }
    return this.pool.pop();
  }

  returnClientToPool(client: any): void {
    this.pool.push(client);
  }

  async initAsync(id: string, initData: Record<string, any>): Promise<Record<string, any>> {
    const isInitiated = await this.getAsync(id, "_initiated");
    let data: Record<string, any> = {};
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

  async resetAsync(id: string, initData: Record<string, any>): Promise<void> {
    await this.setAsync(id, "_initiated", false);
    await this.initAsync(id, initData);
  }

  async resetAllAsync(): Promise<void> {
    const client = await this.getClientFromPool();
    try {
      await client.flushall();
      console.log("Flushed Redis db");
    } catch (err) {
      console.error("Error flushing Redis db:", err);
    } finally {
      this.returnClientToPool(client);
    }
  }

  async getValues(id: string, keys: string[]): Promise<Record<string, any>> {
    const client = await this.getClientFromPool();
    const pipeline = client.pipeline();
    let data: Record<string, any> = {};
    const startMs = Date.now();

    for (const key of keys) {
      const storeKey = `${this.keyPrefix}${id}${key}`;
      pipeline.get(storeKey);
    }

    const results = await pipeline.exec();
    const ops = pipeline.length;

    results.forEach((result: any, index: number) => {
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

  async getAsync(id: string, key: string): Promise<any> {
    const client = await this.getClientFromPool();
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

  async setValues(id: string, data: Record<string, any>): Promise<Record<string, any>> {
    const client = await this.getClientFromPool();
    const returnData: Record<string, any> = {};
    const startMs = Date.now();
    const pipeline = client.pipeline();

    for (const key of Object.keys(data)) {
      const storeKey = `${this.keyPrefix}${id}${key}`;
      const value = data[key];
      pipeline.set(storeKey, JSON.stringify(value));
    }

    const results = await pipeline.exec();
    const ops = pipeline.length;

    results.forEach((result: any, index: number) => {
      const storeKey = `${this.keyPrefix}${id}${Object.keys(data)[index]}`;
      debug(`REDIS set(pipeline) ${storeKey}: ${result[1]}`);
      returnData[Object.keys(data)[index]] = data[Object.keys(data)[index]];
    });

    const ioTimeMs = Date.now() - startMs;
    cloudWatchLog(!REDIS_VERBOSE_LOG, "redis", { event: "setValues", operations: ops, ioTimeMs: ioTimeMs });
    this.returnClientToPool(client);
    return returnData;
  }

  async setAsync(id: string, key: string, value: any): Promise<any> {
    const client = await this.getClientFromPool();
    const startMs = Date.now();
    const storeKey = `${this.keyPrefix}${id}${key}`;
    const res = await client.set(storeKey, JSON.stringify(value));
    const ioTimeMs = Date.now() - startMs;

    debug(`REDIS set ${storeKey}: ${res} (${ioTimeMs}ms) ${ioTimeMs > 1000 ? "REDISSLOW!" : ""}`);
    cloudWatchLog(!REDIS_VERBOSE_LOG, "redis", { event: "set", operations: 1, ioTimeMs: ioTimeMs });
    this.returnClientToPool(client);
    return value;
  }

  async setVolatileAsync(id: string, key: string, value: any): Promise<any> {
    const data = await this.setAsync(id, key, value);
    const storeKey = `${this.keyPrefix}${id}${key}`;
    const client = await this.getClientFromPool();

    await client.expire(storeKey, this.volatileKeyTTL);
    debug(`REDIS expire ${storeKey} ${this.volatileKeyTTL}s`);
    this.returnClientToPool(client);
    return data;
  }

  async removeAsync(id: string, key: string): Promise<any> {
    const client = await this.getClientFromPool();
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
