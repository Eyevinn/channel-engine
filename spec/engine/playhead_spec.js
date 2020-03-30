const Session = require('../../engine/session.js');
const m3u8 = require('@eyevinn/m3u8');
const Readable = require('stream').Readable;

class TestAssetManager {
  constructor(assets) {
    this.assets = [
      { id: 1, title: "Tears of Steel", uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/master.m3u8" },
      { id: 2, title: "VINN", uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8" }
    ];
    if (assets) {
      this.assets = assets;
    }
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

const verificationLoop = async (session, increments) => {
  let remain = increments;
  let promiseFns = [];
  while (remain > 0) {
    promiseFns.push(() => session.increment());
    remain--;
  }
  let lastUri = null;
  let lastManifest = null;
  for (let promiseFn of promiseFns) {
    manifest = await promiseFn();
    const parser = m3u8.createStream();
    await new Promise((resolve, reject) => {
      let manifestStream = new Readable();
      manifestStream.push(manifest);
      manifestStream.push(null);

      manifestStream.pipe(parser);
      parser.on('m3u', m3u => {
        const firstItem = m3u.items.PlaylistItem[0];
        if (lastUri && firstItem.get('uri') === lastUri) {
          console.log(lastManifest);
          console.log(manifest);
          fail(`${m3u.get('mediaSequence')}:${firstItem.get('uri')} was included in last media sequence (${lastUri})`);
        }
        lastUri = firstItem.get('uri');
        resolve();
      });
    });
    lastManifest = manifest;
  }
};

const parseMasterManifest = async (manifest) => {
  const parser = m3u8.createStream();
  const streams = await new Promise((resolve, reject) => {
    let manifestStream = new Readable();
    manifestStream.push(manifest);
    manifestStream.push(null);
    manifestStream.pipe(parser);
    parser.on('m3u', m3u => {
      let profile = [];
      m3u.items.StreamItem.map(streamItem => {
        profile.push({ bw: streamItem.get('bandwidth'), resolution: streamItem.get('resolution') });
      });
      resolve(profile);
    });
  });
  return streams;
};

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
        if (!manifest.match('#EXT-X-MEDIA-SEQUENCE:' + expectedMseq++)) {
          fail(manifest);
        }
        currentMediaManifest = await session.getCurrentMediaManifest(180000);
        expect(currentMediaManifest).toEqual(manifest);
      }
    };
    await loop(100);
    done();
  });

  it("never get the same top segment after media sequence is increased", async (done) => {
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: '1' });
    await verificationLoop(session, 10);
    done();
  });

  it("can handle three short VODs in a row", async (done) => {
    const assetMgr = new TestAssetManager([{ id: 1, title: "Short", uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8" }]);
    const session = new Session(assetMgr, { sessionId: '1' });
    await verificationLoop(session, 10);
    done();
  });

  it("provides all available bitrates for all media sequences", async (done) => {
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: '1' });
    const masterManifest = await session.getMasterManifest();
    const profile = await parseMasterManifest(masterManifest);
    const loop = async (increments) => {
      let remain = increments;
      let verificationFns = [];
      while (remain > 0) {
        const verificationFn = () => {
          return new Promise((resolve, reject) => {
            const bwMap = {
              '6134000': '2000',
              '2323000': '1000',
              '1313000': '600'
            };
            session.increment()
            .then(manifest => {
              profile.map(p => {
                session.getCurrentMediaManifest(p.bw)
                .then(mediaManifest => {
                  expect(mediaManifest.match(`${bwMap[p.bw]}/${bwMap[p.bw]}-.*\.ts$`));
                });
              });
              resolve();
            });
          });
        };
        verificationFns.push(verificationFn);
        remain--;
      }

      for (let verificationFn of verificationFns) {
        await verificationFn();
      }
    };
    await loop(100);
    done();
  });
});
