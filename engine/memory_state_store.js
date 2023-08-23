const debug = require("debug")("memory-state-store");

class MemoryStateStore {
  constructor(type, opts) {
    this.sharedStates = {};
    this.globalSharedStates = {};
  }

  async initAsync(id, initData) {
    if (!this.sharedStates[id]) {
      this.sharedStates[id] = {};
      Object.keys(initData).forEach((key) => {
        this.sharedStates[id][key] = initData[key];
      });
    }
    return this.sharedStates[id];
  }

  async resetAsync(id, initData) {
    this.sharedStates[id] = null;
    await this.initAsync(id, initData);
  }

  async resetAllAsync() {
      this.sharedStates = {};
      this.globalSharedStates = {};
  }

  async getAsync(id, key) {
    let value;
    if (id === "" || id === null) {
      value = this.globalSharedStates[key];
    } else {
      value = this.sharedStates[id][key];
    }
    return value;
  }

  async setAsync(id, key, value) {
    if (id === "" || id === null) {
      this.globalSharedStates[key] = value;
    } else {
      this.sharedStates[id][key] = value;
      return this.sharedStates[id][key];
    }
  }

  async setVolatileAsync(id, key, value) {
    return await this.setAsync(id, key, value);
  }

  async removeAsync(id, key) {
    delete this.sharedStates[id][key];
  }
}

module.exports = MemoryStateStore;