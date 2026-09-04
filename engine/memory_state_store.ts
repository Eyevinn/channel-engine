const debug = require("debug")("memory-state-store");

import type { IStateStore, SharedStates } from "./state_store_types";

// Type-only migration (#373): a faithful port of the original JS. Runtime
// behavior and the exported constructor signature are unchanged.

class MemoryStateStore implements IStateStore {
  sharedStates: SharedStates;
  globalSharedStates: { [key: string]: any };

  constructor(type?: string, opts?: any) {
    this.sharedStates = {};
    this.globalSharedStates = {};
  }

  async initAsync(id: string, initData: Record<string, any>): Promise<any> {
    if (!this.sharedStates[id]) {
      this.sharedStates[id] = {};
      Object.keys(initData).forEach((key) => {
        (this.sharedStates[id] as { [key: string]: any })[key] = initData[key];
      });
    }
    return this.sharedStates[id];
  }

  async resetAsync(id: string, initData: Record<string, any>): Promise<void> {
    this.sharedStates[id] = null;
    await this.initAsync(id, initData);
  }

  async resetAllAsync(): Promise<void> {
      this.sharedStates = {};
      this.globalSharedStates = {};
  }

  async getAsync(id: string, key: string): Promise<any> {
    let value;
    if (id === "" || id === null) {
      value = this.globalSharedStates[key];
    } else {
      if (!this.sharedStates[id]) {
        return null;
      }
      value = (this.sharedStates[id] as { [key: string]: any })[key];
    }
    return value;
  }

  async setAsync(id: string, key: string, value: any): Promise<any> {
    if (id === "" || id === null) {
      this.globalSharedStates[key] = value;
    } else {
      if (!this.sharedStates[id]) {
        this.sharedStates[id] = {};
      }
      (this.sharedStates[id] as { [key: string]: any })[key] = value;
      return (this.sharedStates[id] as { [key: string]: any })[key];
    }
  }

  async setVolatileAsync(id: string, key: string, value: any): Promise<any> {
    return await this.setAsync(id, key, value);
  }

  async removeAsync(id: string, key: string): Promise<void> {
    delete (this.sharedStates[id] as { [key: string]: any })[key];
  }
}

module.exports = MemoryStateStore;
