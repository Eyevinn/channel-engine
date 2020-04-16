const ChannelEngine = require('../../index.js');

class TestChannelManager {
  constructor() {
    this._tick = 0;
  }

  _increment() {
    this._tick++;
  }

  getChannels() {
    if (this._tick < 1) {
      return [];
    } else if (this._tick >= 1 && this._tick < 2) {
      return [ { id: '1', profile: this._getProfile() } ];
    } else if (this._tick >= 2 && this._tick < 3) {
      return [ 
        { id: '1', profile: this._getProfile() },
        { id: '2', profile: this._getProfile() } 
      ];
    } else if (this._tick >= 3) {
      return [ 
        { id: '2', profile: this._getProfile() } 
      ];
    }
  }

  _getProfile() {
    return [
      { bw: 6134000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 1024, 458 ] },
      { bw: 2323000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 640, 286 ] },
      { bw: 1313000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 480, 214 ] }
    ];
  }
}

class TestAssetManager {
  constructor() {

  }

  getNextVod(vodRequest) {
    return new Promise((resolve, reject) => {
      resolve({})
    });
  }
}

describe("Channel Engine", () => {
  beforeEach(() => {
    jasmine.clock().install();
  });

  afterEach(() => {
    jasmine.clock().uninstall();
  });

  it("is updated when new channels are added", done => {
    const testAssetManager = new TestAssetManager();
    const testChannelManager = new TestChannelManager();

    const engine = new ChannelEngine(testAssetManager, { channelManager: testChannelManager});
    engine.start();
    testChannelManager._increment();
    jasmine.clock().tick((60 * 1000) + 1);
    jasmine.clock().tick(5001);
    
    expect(engine.getSessionCount()).toEqual(1);

    engine.getStatusForSession("1")
    .then(status => {
      expect(status.playhead.state).toEqual("idle");
      testChannelManager._increment();
      jasmine.clock().tick((60 * 1000) + 1);
      expect(engine.getSessionCount()).toEqual(2);
      done();
    });
  });

  it("is updated when channels are removed", done => {
    const testAssetManager = new TestAssetManager();
    const testChannelManager = new TestChannelManager();

    const engine = new ChannelEngine(testAssetManager, { channelManager: testChannelManager});
    engine.start();
    testChannelManager._increment();
    jasmine.clock().tick((60 * 1000) + 1);
    jasmine.clock().tick(5001);
    
    expect(engine.getSessionCount()).toEqual(1);

    engine.getStatusForSession("1")
    .then(status => {
      expect(status.playhead.state).toEqual("idle");
      testChannelManager._increment();
      jasmine.clock().tick((60 * 1000) + 1);
      expect(engine.getSessionCount()).toEqual(2);
      testChannelManager._increment();
      jasmine.clock().tick((60 * 1000) + 1);
      expect(engine.getSessionCount()).toEqual(1);
      done();
    });
  });
});