const Session = require('../../engine/session.js');

class TestAssetManager {
  constructor() {
    this.assets = [
      { id: 1, title: "Tears of Steel", uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/master.m3u8" },
      { id: 2, title: "VINN", uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8" }
    ];
    this.pos = 0;
  }
  getNextVod(vodRequest) {
    return new Promise((resolve, reject) => {
      const vod = this.assets[this.pos++];
      if (this.pos > this.assets.length - 1) {
        this.pos = 0;
      }
      resolve(vod);
    });

  }
}

describe("Playhead consumer", () => {
  it("continously increases media sequence over two VOD switches", async (done) => {
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: '1' });
    const loop = async (increments) => {
      let remain = increments;
      let promiseFns = [];
      while (remain > 0) {
        promiseFns.push(() => session.increment());
        remain--;
      }
      let expectedMseq = 1;
      for (let promiseFn of promiseFns) {
        manifest = await promiseFn();
        expect(manifest.match('#EXT-X-MEDIA-SEQUENCE:' + expectedMseq++)).not.toBeNull();
        fail(manifest);
      }
    };
    await loop(100);
    done();
  });
});