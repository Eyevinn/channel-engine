class SharedStateStore {
  constructor(initData) {
    this.sharedStates = {};
    this.initData = initData
  }

  init(id) {
    if (!this.sharedStates[id]) {
      this.sharedStates[id] = this.initData;
    }
  }

  get(id) {
    if (!this.sharedStates[id]) {
      this.init(id);  
    }
    return this.sharedStates[id];
  }

  set(id, key, value) {
    if (!this.sharedStates[id]) {
      this.init(id);
    }
    this.sharedStates[id][key] = value;
  }
}

module.exports = SharedStateStore;