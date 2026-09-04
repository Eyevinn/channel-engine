const MemcacheClient = require("memcache-client");
const debug = require("debug")("memcached-state-store");

import type { IStateStore } from "./state_store_types";

// Type-only migration (#373): a faithful port of the original JS.

class MemcachedStateStore implements IStateStore {
  keyPrefix: string;
  client: any;

  constructor(keyPrefix: string, opts: any) {
    this.keyPrefix = keyPrefix;
    if (opts.version) {
      const prependPrefix = opts.version.replace(/\./g, "X");
      this.keyPrefix = prependPrefix + this.keyPrefix;
      debug(`Prepending keyprefix with ${prependPrefix} => ${this.keyPrefix}`);
    }
    this.client = new MemcacheClient({ server: opts.memcachedUrl, cmdTimeout: 10000 });
  }

  async initAsync(id: string, initData: Record<string, any>): Promise<Record<string, any>> {
    const isInitiated = await this.getAsync(id, "_initiated");
    let data: Record<string, any> = {};
    if (!isInitiated) {
      for(const key of Object.keys(initData)) {
        debug(`${this.keyPrefix}:${id}: Initiating key ${key} with init data`);
        data[key] = await this.setAsync(id, key, initData[key]);
      }
      await this.setAsync(id, "_initiated", true);
    } else {
      debug(`${this.keyPrefix}:${id}: Already initiated, not initiating with init data`);
      for(const key of Object.keys(initData)) {
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
    console.error("Shared Storage Reset Failed.\nMemcache-client: Flush All Command Not Implemented Yet");
  }

  async getAsync(id: string, key: string): Promise<any> {
    const storeKey = "" + this.keyPrefix + id + key;
    const data = await this.client.get(storeKey);
    if (data) {
      return JSON.parse(data.value);
    }
    return null;
  }

  async setAsync(id: string, key: string, value: any): Promise<any> {
    const storeKey = "" + this.keyPrefix + id + key;
    await this.client.set(storeKey, JSON.stringify(value));
    return value;
  }

  async setVolatileAsync(id: string, key: string, value: any): Promise<any> {
    throw new Error("memcached: setVolatileAsync not implemented yet");
  }

  async removeAsync(id: string, key: string): Promise<any> {
    throw new Error("memcached: removeAsync not implemented yet");
  }
}

module.exports = MemcachedStateStore;
