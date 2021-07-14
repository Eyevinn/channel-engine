const SessionLive = require('../../engine/session_live.js');
const Session = require('../../engine/session.js');
const StreamSwitcher = require('../../engine/stream_switcher.js');
const { v4: uuidv4 } = require('uuid');
const { SessionStateStore } = require('../../engine/session_state.js');
const { PlayheadStateStore } = require('../../engine/playhead_state.js');
const StreamType = Object.freeze({
  LIVE: 1,
  VOD: 2,
});
const tsNow = Date.now();

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
      if (opts && opts.failOnIndex) {
        this.failOnIndex = 1;
      }
    }

    getNextVod(vodRequest) {
      return new Promise((resolve, reject) => {
        if (this.doFail || this.pos === this.failOnIndex) {
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
let allListSchedules = [
  [ // list 0
    {
      eventId: "abc-100",
      type: StreamType.LIVE,
      assetId: 1,
      start_time: tsNow + 20 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8",
    },
  ],
  [ // list 1
    {
      eventId: "abc-100",
      type: StreamType.VOD,
      assetId: 1,
      start_time: tsNow + 20 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8",
      duration: 60*1000,
    }
  ],
  [ // list 2
    {
      eventId: "abc-100",
      type: StreamType.LIVE,
      assetId: 1,
      start_time: tsNow + 20 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8",
    },
    {
      eventId: "abc-101",
      type: StreamType.VOD,
      assetId: 2,
      start_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000 + (30 * 1000) + 60*1000,
      uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8",
      duration: 60*1000,
    }
  ]
];

class TestSwitchManager {
  constructor(listNum) {
    this.schedule = [];
    this.eventId = 0;
    this.listIndex = listNum;
  }

  getSchedule() {
    return allListSchedules[this.listIndex];
  }
}

describe("The Stream Switcher", () => {
  let sessionStore = undefined;

  beforeEach(() => {
    jasmine.clock().install();
    sessionStore = {
      sessionStateStore: new SessionStateStore(),
      playheadStateStore: new PlayheadStateStore(),
      instanceId: uuidv4(),
    };
  });
  afterEach(() => {
    jasmine.clock().uninstall();
  });

  it("will switch from session to sessionLive (type: live) according to schedule", async (done) => {
    const switchMgr = new TestSwitchManager(0);
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, {sessionId: "1"}, sessionStore);
    const sessionLive = new SessionLive({sessionId: "1"});
    await session.initAsync();
    await session.incrementAsync();

    let TestStreamSwitcher = new StreamSwitcher({streamSwitchManager: switchMgr});
    expect(await TestStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
    jasmine.clock().mockDate(tsNow);
    jasmine.clock().tick((25 * 1000));
    expect(await TestStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
    done();
  });

  it("will switch from sessionLive (type: live) to session according to schedule", async (done) => {
    const switchMgr = new TestSwitchManager(0);
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, {sessionId: "1"}, sessionStore);
    const sessionLive = new SessionLive({sessionId: "1"});
    await session.initAsync();
    await session.incrementAsync();

    let TestStreamSwitcher = new StreamSwitcher({streamSwitchManager: switchMgr});
    jasmine.clock().mockDate(tsNow);
    jasmine.clock().tick((25 * 1000));
    expect(await TestStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
    jasmine.clock().tick((1 + (60 * 1000 + 25 * 1000)));
    await sessionLive.getCurrentMediaManifestAsync(180000);
    expect(await TestStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
    done();
  });

  xit("will switch from session to sessionLive (type=vod) according to schedule", async (done) => {
    const switchMgr = new TestSwitchManager(1);
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, {sessionId: "1"}, sessionStore);
    const sessionLive = new SessionLive({sessionId: "1"});
    await session.initAsync();
    await session.incrementAsync();

    let TestStreamSwitcher = new StreamSwitcher({streamSwitchManager: switchMgr});
    expect(await TestStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
    jasmine.clock().mockDate(tsNow);
    jasmine.clock().tick((25 * 1000));
    expect(await TestStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
    done();
  });

  xit("will switch from sessionLive (type=vod) to session according to schedule", async (done) => {
    const switchMgr = new TestSwitchManager(1);
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, {sessionId: "1"}, sessionStore);
    const sessionLive = new SessionLive({sessionId: "1"});
    await session.initAsync();
    await session.incrementAsync();

    let TestStreamSwitcher = new StreamSwitcher({streamSwitchManager: switchMgr});
    jasmine.clock().mockDate(tsNow);
    jasmine.clock().tick((25 * 1000));
    expect(await TestStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
    jasmine.clock().tick((1 + (60 * 1000 + 25 * 1000)));
    await sessionLive.getCurrentMediaManifestAsync(180000);
    expect(await TestStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
    done();
  });
});