const ChannelEngine = require('../../index.js');

class TestAssetManager {
  constructor(opts, assets) {
    if (opts && opts.errorHandler) {
      this.errorHandlerFn = opts.errorHandler;
    }
  }

  getNextVod(vodRequest) {
    return new Promise((resolve, reject) => {
      resolve({ id: 1, title: "Tears of Steel", uri: "https://maitv-vod.lab.eyevinn.technology/404/master.m3u8" });
    });
  }

  handleError(err, vodResponse) {
    if (this.errorHandlerFn) {
      this.errorHandlerFn(err, vodResponse);
    }
  }
}

class TestChannelManager {
  getChannels() {
    return [ { id: '1', profile: this._getProfile() } ];
  }

  _getProfile() {
    return [
      { bw: 6134000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 1024, 458 ] },
      { bw: 2323000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 640, 286 ] },
      { bw: 1313000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 480, 214 ] }
    ];
  }
};

describe("Asset Manager Interface", () => {
  beforeEach(() => {
    jasmine.clock().install();
  });

  afterEach(() => {
    jasmine.clock().uninstall();
  });

  it("receives an error event when VOD fails to load", async (done) => {
    const errorHandler = (err, data) => {
      expect(err.message).toEqual("Failed to init first VOD");
      expect(data.id).toEqual(1);
      done();
    };

    const testAssetManager = new TestAssetManager({ errorHandler: errorHandler });
    const testChannelManager = new TestChannelManager();

    const engine = new ChannelEngine(testAssetManager, { channelManager: testChannelManager });
    await engine.start();
    jasmine.clock().tick((10 * 1000) + 1);

  });
});