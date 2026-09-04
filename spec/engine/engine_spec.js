const { ChannelEngine } = require('../../dist/index.js');

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

  describe("ad-break config validation (issue #367)", () => {
    let testAssetManager;
    let testChannelManager;

    beforeEach(() => {
      testAssetManager = new TestAssetManager();
      testChannelManager = new TestChannelManager();
    });

    // NOTE: the module-level fastify instance is a singleton, so a *successful*
    // ChannelEngine construction registers routes on it and cannot be repeated
    // within a single process. These specs therefore only cover the validation
    // paths that throw before any route registration happens (issue #367).

    it("throws when an enabled ad-break config is missing the endpoint URL", () => {
      expect(() => {
        new ChannelEngine(testAssetManager, {
          channelManager: testChannelManager,
          adBreak: { enabled: true }
        });
      }).toThrowError(/adServerUri/);
    });

    it("throws when the ad-break endpoint URL is malformed", () => {
      expect(() => {
        new ChannelEngine(testAssetManager, {
          channelManager: testChannelManager,
          adBreak: { enabled: true, adServerUri: "not-a-url" }
        });
      }).toThrowError(/invalid adServerUri/);
    });

    it("throws when the ad-break endpoint URL is not http(s)", () => {
      expect(() => {
        new ChannelEngine(testAssetManager, {
          channelManager: testChannelManager,
          adBreak: { enabled: true, adServerUri: "ftp://ads.example.com/vast" }
        });
      }).toThrowError(/http\(s\)/);
    });
  });

  xit("is updated when new channels are added", async () => {
    const testAssetManager = new TestAssetManager();
    const testChannelManager = new TestChannelManager();

    const engine = new ChannelEngine(testAssetManager, { channelManager: testChannelManager});
    engine.start();
    testChannelManager._increment();
    jasmine.clock().tick((60 * 1000) + 1);
    jasmine.clock().tick(5001);
    
    expect(engine.getSessionCount()).toEqual(1);

    const status = await engine.getStatusForSessionAsync("1")
    expect(status.playhead.state).toEqual("idle");
    testChannelManager._increment();
    jasmine.clock().tick((60 * 1000) + 1);
    expect(engine.getSessionCount()).toEqual(2);
  });

  xit("is updated when channels are removed", async () => {
    const testAssetManager = new TestAssetManager();
    const testChannelManager = new TestChannelManager();

    const engine = new ChannelEngine(testAssetManager, { channelManager: testChannelManager});
    engine.start();

    testChannelManager._increment();
    jasmine.clock().tick((60 * 1000) + 1);
    jasmine.clock().tick(5001);    
    console.log(engine.getSessionCount());
    expect(engine.getSessionCount()).toEqual(1);

    const status = await engine.getStatusForSessionAsync("1");
    expect(status.playhead.state).toEqual("idle");

    testChannelManager._increment();
    jasmine.clock().tick((60 * 1000) + 1);
    console.log(engine.getSessionCount());
    expect(engine.getSessionCount()).toEqual(2);

    testChannelManager._increment();
    jasmine.clock().tick((2 * 60 * 1000) + 1);
    console.log(engine.getSessionCount());
    expect(engine.getSessionCount()).toEqual(1);
  });
});