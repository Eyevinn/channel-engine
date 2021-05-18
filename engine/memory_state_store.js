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

  async getAsync(id) {
    let data = this.sharedStates[id];
    return data;
  }

  async setAsync(id, data) {
    this.sharedStates[id] = data;
  }
}

module.exports = MemoryStateStore;