const debug = require("debug")("engine-state-store");

const RedisStateStore = require("./redis_state_store.js");
const MemcachedStateStore = require("./memcached_state_store.js");
const MemoryStateStore = require("./memory_state_store.js");
import type { IStateStore, ISharedStateStoreOpts } from "./state_store_types";

// Type-only migration (#373): a faithful port of the original JS. The public
// API (constructor + methods) and runtime behavior are unchanged.

class SharedStateStore {
  initData: Record<string, any> | undefined;
  type: string;
  cache: Record<string, any>;
  cacheTTL: number;
  shared: boolean;
  hasPipeline: boolean;
  store: IStateStore;

  constructor(type: string, opts: ISharedStateStoreOpts | undefined, initData?: Record<string, any>) {
    this.initData = initData;
    this.type = type;
    this.cache = {};
    this.cacheTTL = opts && opts.cacheTTL ? opts.cacheTTL : 1000;

    this.shared = false;
    this.hasPipeline = false;
    if (opts && opts.redisUrl) {
      debug(`Using REDIS (${opts.redisUrl}) for shared state store (${type}, cacheTTL=${this.cacheTTL})`);
      this.store = new RedisStateStore(`${type}:`, opts);
      this.shared = true;
      this.hasPipeline = true;
    } else if (opts && opts.memcachedUrl) {
      debug(`Using MEMCACHED (${opts.memcachedUrl}) for shared state store (${type}, cacheTTL=${this.cacheTTL})`);
      this.store = new MemcachedStateStore(`${type}:`, opts);
      this.shared = true;
    } else {
      debug(`Using MEMORY for non-shared state store (${type}, cacheTTL=${this.cacheTTL})`);
      this.store = new MemoryStateStore(`${type}:`, opts);
    }
  }

  isShared(): boolean {
    return this.shared;
  }

  canPipeline(): boolean {
    return this.hasPipeline;
  }

  async init(id: string): Promise<void> {
    await this.store.initAsync(id, this.initData as Record<string, any>);
  }

  async reset(id: string): Promise<void> {
    await this.store.resetAsync(id, this.initData as Record<string, any>);
  }

  async resetAll(): Promise<void> {
    await this.store.resetAllAsync();
  }

  async get(id: string, key: string): Promise<any> {
    //debug(`${this.type}:${id}:${key} Reading from shared store`);
    let data = await this.store.getAsync(id, key);
    //debug(key !== "currentVod" ? data : (data ? "not null" : "null" ));
    return data;
  }

  async set(id: string, key: string, value: any): Promise<any> {
    //debug(`${this.type}:${id}:${key} Writing to shared store`);
    const data = await this.store.setAsync(id, key, value);
    return data;
  }

  async setVolatile(id: string, key: string, value: any): Promise<any> {
    const data = await this.store.setVolatileAsync(id, key, value);
    return data;
  }

  async getValues(id: string, keys: string[]): Promise<Record<string, any>> {
    let data: Record<string, any> = {};
    if (this.hasPipeline) {
      data = await (this.store.getValues as NonNullable<IStateStore["getValues"]>)(id, keys);
    } else {
      for(const key of keys) {
        data[key] = await this.get(id, key);
      }
    }
    return data;
  }

  async setValues(id: string, data: Record<string, any>): Promise<Record<string, any>> {
    let returnData: Record<string, any> = {};
    if (this.hasPipeline) {
      returnData = await (this.store.setValues as NonNullable<IStateStore["setValues"]>)(id, data);
    } else {
      for (const key of Object.keys(data)) {
        returnData[key] = await this.set(id, key, data[key]);
      }
    }
    return returnData;
  }

  async remove(id: string, key: string): Promise<void> {
    await this.store.removeAsync(id, key);
  }
}

module.exports = SharedStateStore;
