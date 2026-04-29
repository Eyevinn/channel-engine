const Session = require('../../engine/session.js');
const m3u8 = require('@eyevinn/m3u8');
const Readable = require('stream').Readable;

const { SessionStateStore } = require('../../engine/session_state.js');
const { PlayheadStateStore } = require('../../engine/playhead_state.js');

class TestAssetManager {
  constructor(opts, assets) {
    this.assets = [
      { id: 1, title: "Tears of Steel", uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/master.m3u8" },
      { id: 2, title: "VINN", uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8" }
    ];
    if (assets) {
      this.assets = assets;
    }
    this.pos = 0;
    this.doFail = false;
    if (opts && opts.fail) {
      this.doFail = true;
    }
  }
  getNextVod(vodRequest) {
    return new Promise((resolve, reject) => {
      if (this.doFail) {
        reject("should fail");
      } else {
        const vod = this.assets[this.pos++];
        if (this.pos > this.assets.length - 1) {
          this.pos = 0;
        }
        resolve(vod);
      }
    });
  }
}

const parseMediaManifest = async (manifest) => {
  const parser = m3u8.createStream();
  const m3u = await new Promise((resolve, reject) => {
    let manifestStream = new Readable();
    manifestStream.push(manifest);
    manifestStream.push(null);
    manifestStream.pipe(parser);
    parser.on('m3u', m3u => {
      resolve(m3u);
    });
  });
  return m3u;
};

describe("High Availability", () => {
  describe("Leader-Follower with shared state", () => {
    let sessionStateStore;
    let playheadStateStore;

    beforeEach(() => {
      sessionStateStore = new SessionStateStore();
      playheadStateStore = new PlayheadStateStore();
    });

    it("leader writes state that follower can read", async () => {
      const assetMgr = new TestAssetManager();
      const leaderStore = {
        sessionStateStore,
        playheadStateStore,
        instanceId: "leader-instance",
      };
      const followerStore = {
        sessionStateStore,
        playheadStateStore,
        instanceId: "follower-instance",
      };

      // Leader session inits first — becomes the leader
      const leaderSession = new Session(assetMgr, { sessionId: '1' }, leaderStore);
      await leaderSession.initAsync();

      // Follower session shares same sessionId and stores
      const followerSession = new Session(assetMgr, { sessionId: '1' }, followerStore);
      await followerSession.initAsync();

      // Leader increments — should write state
      await leaderSession.incrementAsync();
      const leaderManifest = await leaderSession.getCurrentMediaManifestAsync(180000);
      expect(leaderManifest).not.toBeNull();

      // Follower increments — should read leader's state, not write
      await followerSession.incrementAsync();
      const followerManifest = await followerSession.getCurrentMediaManifestAsync(180000);
      expect(followerManifest).not.toBeNull();

      // Both should produce valid manifests
      const leaderM3u = await parseMediaManifest(leaderManifest);
      const followerM3u = await parseMediaManifest(followerManifest);
      expect(leaderM3u.get('mediaSequence')).toBeDefined();
      expect(followerM3u.get('mediaSequence')).toBeDefined();
    });

    it("follower does not overwrite leader's vodMediaSeqVideo", async () => {
      const assetMgr = new TestAssetManager();
      const leaderStore = {
        sessionStateStore,
        playheadStateStore,
        instanceId: "leader-instance",
      };
      const followerStore = {
        sessionStateStore,
        playheadStateStore,
        instanceId: "follower-instance",
      };

      const leaderSession = new Session(assetMgr, { sessionId: '1' }, leaderStore);
      await leaderSession.initAsync();

      const followerSession = new Session(assetMgr, { sessionId: '1' }, followerStore);
      await followerSession.initAsync();

      // Leader advances several times
      for (let i = 0; i < 5; i++) {
        await leaderSession.incrementAsync();
      }

      // Read the shared state directly — leader incremented 5 times
      const stateAfterLeader = await sessionStateStore.get('1', 'vodMediaSeqVideo');
      expect(stateAfterLeader).toEqual(5);

      // Follower increments — should NOT change vodMediaSeqVideo in store
      // (follower reads but doesn't write via setValues since isLeader=false)
      await followerSession.incrementAsync();
      const stateAfterFollower = await sessionStateStore.get('1', 'vodMediaSeqVideo');
      expect(stateAfterFollower).toEqual(5); // Follower must not overwrite leader's value
    });

    it("leader and follower media sequences stay monotonically increasing", async () => {
      const assetMgr = new TestAssetManager();
      const leaderStore = {
        sessionStateStore,
        playheadStateStore,
        instanceId: "leader-instance",
      };
      const followerStore = {
        sessionStateStore,
        playheadStateStore,
        instanceId: "follower-instance",
      };

      const leaderSession = new Session(assetMgr, { sessionId: '1' }, leaderStore);
      await leaderSession.initAsync();
      const followerSession = new Session(assetMgr, { sessionId: '1' }, followerStore);
      await followerSession.initAsync();

      let lastLeaderMseq = -1;
      let lastFollowerMseq = -1;

      for (let i = 0; i < 20; i++) {
        const leaderManifest = await leaderSession.incrementAsync();
        if (leaderManifest) {
          const lm = leaderManifest.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/);
          if (lm) {
            const mseq = Number(lm[1]);
            expect(mseq).toBeGreaterThanOrEqual(lastLeaderMseq);
            lastLeaderMseq = mseq;
          }
        }

        const followerManifest = await followerSession.incrementAsync();
        if (followerManifest) {
          const fm = followerManifest.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/);
          if (fm) {
            const mseq = Number(fm[1]);
            expect(mseq).toBeGreaterThanOrEqual(lastFollowerMseq);
            lastFollowerMseq = mseq;
          }
        }
      }

      // Both should have progressed
      expect(lastLeaderMseq).toBeGreaterThan(0);
      expect(lastFollowerMseq).toBeGreaterThan(0);
    });
  });

  describe("_lastTickState consistency", () => {
    let sessionStore;

    beforeEach(() => {
      sessionStore = {
        sessionStateStore: new SessionStateStore(),
        playheadStateStore: new PlayheadStateStore(),
        instanceId: "test-instance",
      };
    });

    it("_lastTickState is populated after incrementAsync", async () => {
      const assetMgr = new TestAssetManager();
      const session = new Session(assetMgr, { sessionId: '1' }, sessionStore);
      await session.initAsync();
      await session.incrementAsync();

      expect(session._lastTickState).toBeDefined();
      expect(session._lastTickState.sessionState).toBeDefined();
      expect(session._lastTickState.isLeader).toBe(true);
      expect(session._lastTickState.currentVod).toBeDefined();
    });

    it("_lastTickState reflects correct state after VOD transition", async () => {
      const assetMgr = new TestAssetManager(null, [
        { id: 1, title: "Short", uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8" }
      ]);
      const session = new Session(assetMgr, { sessionId: '1' }, sessionStore);
      await session.initAsync();

      let lastState = null;
      // Run enough increments to cross a VOD boundary
      for (let i = 0; i < 20; i++) {
        await session.incrementAsync();
        lastState = session._lastTickState;
      }

      expect(lastState.sessionState).toBeDefined();
      expect(lastState.currentVod).not.toBeNull();
      // vodMediaSeqVideo should be a valid non-negative number
      expect(lastState.sessionState.vodMediaSeqVideo).toBeGreaterThanOrEqual(0);
    });

    it("_lastTickState has correct state after slate insertion", async () => {
      const assetMgr = new TestAssetManager({ fail: true });
      const session = new Session(assetMgr, {
        sessionId: '1',
        slateUri: 'http://testcontent.eyevinn.technology/slates/ottera/playlist.m3u8'
      }, sessionStore);
      await session.initAsync();

      await session.incrementAsync();

      // After slate insertion + incrementAsync processing, _lastTickState should have valid state
      expect(session._lastTickState).toBeDefined();
      expect(session._lastTickState.sessionState).toBeDefined();
      expect(session._lastTickState.currentVod).not.toBeNull();
      // State is VOD_PLAYING (2) because incrementAsync detects VOD_NEXT_INITIATING
      // from _tickAsync and transitions it to VOD_PLAYING
      expect(session._lastTickState.sessionState.state).toEqual(2); // VOD_PLAYING
    });
  });
});

describe("Concurrent ticks", () => {
  let sessionStateStore;
  let playheadStateStore;

  beforeEach(() => {
    sessionStateStore = new SessionStateStore();
    playheadStateStore = new PlayheadStateStore();
  });

  it("multiple channels ticking concurrently do not corrupt each other's state", async () => {
    const channels = [];
    const numChannels = 4;

    for (let i = 0; i < numChannels; i++) {
      const assetMgr = new TestAssetManager();
      const store = {
        sessionStateStore,
        playheadStateStore,
        instanceId: "instance-1",
      };
      const session = new Session(assetMgr, { sessionId: `ch-${i}` }, store);
      await session.initAsync();
      channels.push(session);
    }

    // Tick all channels concurrently for several rounds
    for (let round = 0; round < 10; round++) {
      await Promise.all(channels.map(ch => ch.incrementAsync()));
    }

    // Each channel should have valid, independent state
    for (let i = 0; i < numChannels; i++) {
      const manifest = await channels[i].getCurrentMediaManifestAsync(180000);
      expect(manifest).not.toBeNull();

      const m = manifest.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/);
      expect(m).not.toBeNull();
      const mseq = Number(m[1]);
      // Each channel started at 0 and ticked 10 times
      expect(mseq).toBeGreaterThanOrEqual(10);
    }
  });

  it("concurrent ticks produce monotonically increasing media sequences per channel", async () => {
    const numChannels = 3;
    const channels = [];
    const lastMseqs = new Array(numChannels).fill(-1);

    for (let i = 0; i < numChannels; i++) {
      const assetMgr = new TestAssetManager();
      const store = {
        sessionStateStore,
        playheadStateStore,
        instanceId: "instance-1",
      };
      const session = new Session(assetMgr, { sessionId: `concurrent-${i}` }, store);
      await session.initAsync();
      channels.push(session);
    }

    for (let round = 0; round < 15; round++) {
      const results = await Promise.all(channels.map(ch => ch.incrementAsync()));

      for (let i = 0; i < numChannels; i++) {
        if (results[i]) {
          const m = results[i].match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/);
          if (m) {
            const mseq = Number(m[1]);
            if (lastMseqs[i] >= 0) {
              expect(mseq).toBeGreaterThanOrEqual(lastMseqs[i]);
            }
            lastMseqs[i] = mseq;
          }
        }
      }
    }

    // All channels should have progressed
    for (let i = 0; i < numChannels; i++) {
      expect(lastMseqs[i]).toBeGreaterThan(0);
    }
  });

  it("concurrent ticks across a VOD switch produce valid manifests", async () => {
    const numChannels = 3;
    const channels = [];

    for (let i = 0; i < numChannels; i++) {
      // Short VOD to force VOD switches
      const assetMgr = new TestAssetManager(null, [
        { id: 1, title: "Short", uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8" }
      ]);
      const store = {
        sessionStateStore,
        playheadStateStore,
        instanceId: "instance-1",
      };
      const session = new Session(assetMgr, { sessionId: `vodswitch-${i}` }, store);
      await session.initAsync();
      channels.push(session);
    }

    let errorCount = 0;
    for (let round = 0; round < 20; round++) {
      const results = await Promise.all(channels.map(ch => ch.incrementAsync()));
      for (const manifest of results) {
        if (manifest) {
          // Verify it's parseable HLS
          const hasHeader = manifest.includes('#EXTM3U');
          if (!hasHeader) errorCount++;
        }
      }
    }

    expect(errorCount).toBe(0);
  });
});
