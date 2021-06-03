const debug = require("debug")("memory-state-store");

class MemoryStateStore {
  constructor(type, opts) {
    this.sharedStates = {};

  }

  async initAsync(id, initData) {
    if (!this.sharedStates[id]) {
      this.sharedStates[id] = {};
      Object.keys(initData).forEach(key => {
        this.sharedStates[id][key] = initData[key];
      });
    }
    return this.sharedStates[id];
  }

  async getAsync(id, key) {
    let value = this.sharedStates[id][key];
    return value;
  }

  async setAsync(id, key, value) {
    this.sharedStates[id][key] = value;
    return this.sharedStates[id][key];
  }

  async setVolatileAsync(id, key, value) {
    return await this.setAsync(id, key, value);
  }

  async removeAsync(id, key) {
    delete this.sharedStates[id][key];
  }  
}

module.exports = MemoryStateStore;