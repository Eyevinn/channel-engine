const SessionLive = require('../../engine/session_live.js');
const Session = require('../../engine/session.js');
const StreamSwitcher = require('../../engine/stream_switcher.js');
const { v4: uuidv4 } = require('uuid');
const { SessionStateStore } = require('../../engine/session_state.js');
const { PlayheadStateStore } = require('../../engine/playhead_state.js');
const { SessionLiveStateStore } = require('../../engine/session_live_state.js');
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

const allListSchedules = [
  [
    {
      eventId: "live-0",
      type: StreamType.LIVE,
      assetId: 1,
      start_time: tsNow + 20 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8",
    },
  ],
  [
    {
      eventId: "vod-0",
      type: StreamType.VOD,
      assetId: 1,
      start_time: tsNow + 20 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8",
      duration: 60 * 1000,
    }
  ],
  [
    {
      eventId: "live-1",
      type: StreamType.LIVE,
      assetId: 1,
      start_time: tsNow + 20 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8",
    },
    {
      eventId: "vod-1",
      type: StreamType.VOD,
      assetId: 2,
      start_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000 + 60 * 1000,
      uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8",
      duration: 60 * 1000,
    }
  ],
  [
    {
      eventId: "live-2",
      type: StreamType.LIVE,
      assetId: 1,
      start_time: tsNow + 20 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8",
    },
    {
      eventId: "live-3",
      type: StreamType.LIVE,
      assetId: 2,
      start_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000 + 60 * 1000,
      uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8",
    }
  ],
  [
    {
      eventId: "vod-2",
      type: StreamType.VOD,
      assetId: 2,
      start_time: tsNow + 20 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8",
      duration: 60 * 1000,
    },
    {
      eventId: "live-4",
      type: StreamType.LIVE,
      assetId: 1,
      start_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000 + 60 * 1000,
      uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8",
    }
  ],
  [
    {
      eventId: "vod-3",
      type: StreamType.VOD,
      assetId: 2,
      start_time: tsNow + 20 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8",
      duration: 60 * 1000,
    },
    {
      eventId: "vod-4",
      type: StreamType.VOD,
      assetId: 2,
      start_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000 + 60 * 1000,
      uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8",
      duration: 60 * 1000,
    },
  ],
  [
    {
      eventId: "vod-0",
      type: StreamType.VOD,
      assetId: 1,
      start_time: tsNow + 20 * 1000,
      end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
      uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8",
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

const mockLiveSegments = {
  "180000": [{ duration: 7, uri: "http://mock.mock.com/180000/seg09.ts" },
  { duration: 7, uri: "http://mock.mock.com/180000/seg10.ts" },
  { duration: 7, uri: "http://mock.mock.com/180000/seg11.ts" },
  { duration: 7, uri: "http://mock.mock.com/180000/seg12.ts" },
  { duration: 7, uri: "http://mock.mock.com/180000/seg13.ts" },
  { duration: 7, uri: "http://mock.mock.com/180000/seg14.ts" },
  { duration: 7, uri: "http://mock.mock.com/180000/seg15.ts" },
  { duration: 7, uri: "http://mock.mock.com/180000/seg16.ts" },
  { discontinuity: true }],
  "1258000": [{ duration: 7, uri: "http://mock.mock.com/1258000/seg09.ts" },
  { duration: 7, uri: "http://mock.mock.com/1258000/seg10.ts" },
  { duration: 7, uri: "http://mock.mock.com/1258000/seg11.ts" },
  { duration: 7, uri: "http://mock.mock.com/1258000/seg12.ts" },
  { duration: 7, uri: "http://mock.mock.com/1258000/seg13.ts" },
  { duration: 7, uri: "http://mock.mock.com/1258000/seg14.ts" },
  { duration: 7, uri: "http://mock.mock.com/1258000/seg15.ts" },
  { duration: 7, uri: "http://mock.mock.com/1258000/seg16.ts" },
  { discontinuity: true }],
  "2488000": [{ duration: 7, uri: "http://mock.mock.com/2488000/seg09.ts" },
  { duration: 7, uri: "http://mock.mock.com/2488000/seg10.ts" },
  { duration: 7, uri: "http://mock.mock.com/2488000/seg11.ts" },
  { duration: 7, uri: "http://mock.mock.com/2488000/seg12.ts" },
  { duration: 7, uri: "http://mock.mock.com/2488000/seg13.ts" },
  { duration: 7, uri: "http://mock.mock.com/2488000/seg14.ts" },
  { duration: 7, uri: "http://mock.mock.com/2488000/seg15.ts" },
  { duration: 7, uri: "http://mock.mock.com/2488000/seg16.ts" },
  { discontinuity: true }]
};

describe("The Stream Switcher", () => {
  let sessionStore = undefined;
  let sessionLiveStore = undefined;

  beforeEach(() => {
    jasmine.clock().install();
    sessionStore = {
      sessionStateStore: new SessionStateStore(),
      playheadStateStore: new PlayheadStateStore(),
      instanceId: uuidv4(),
    };
    sessionLiveStore = {
      sessionLiveStateStore: new SessionLiveStateStore(),
      instanceId: uuidv4(),
    };
  });
  afterEach(() => {
    jasmine.clock().uninstall();
  });

  it("should return false if no StreamSwitchManager was given.", async () => {
    const assetMgr = new TestAssetManager();
    const testStreamSwitcher = new StreamSwitcher();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);

    await session.initAsync();
    await session.incrementAsync();
    await sessionLive.initAsync();

    expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
    expect(testStreamSwitcher.getEventId()).toBe(null);
  });

  it("should validate uri and switch back to linear-vod (session) from event-livestream (sessionLive) if uri is unreachable", async () => {
    const switchMgr = new TestSwitchManager(0);
    const testStreamSwitcher = new StreamSwitcher({ streamSwitchManager: switchMgr });
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);
    spyOn(sessionLive, "resetLiveStoreAsync").and.callFake(() => true);
    spyOn(sessionLive, "resetSession").and.callFake(() => true);
    spyOn(sessionLive, "getCurrentMediaSequenceSegments").and.returnValue({ currMseqSegs: mockLiveSegments, segCount: 8 });
    spyOn(session, "setCurrentMediaSequenceSegments").and.returnValue(true);
    spyOn(session, "setCurrentMediaAndDiscSequenceCount").and.returnValue(true);

    await session.initAsync();
    await session.incrementAsync();
    await sessionLive.initAsync();
    sessionLive.startPlayheadAsync();


    expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
    expect(testStreamSwitcher.getEventId()).toBe(null);
    jasmine.clock().mockDate(tsNow);
    jasmine.clock().tick((25 * 1000));
    expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
    expect(testStreamSwitcher.getEventId()).toBe("live-0");

    jasmine.clock().tick((25 * 1000) + (10 * 1000));
    spyOn(switchMgr, "getSchedule").and.returnValue([{
      eventId: "live-0",
      type: StreamType.LIVE,
      assetId: 1,
      start_time: tsNow,
      end_time: tsNow + 1 * 60 * 1000,
      uri: "https://www.google.com/nothere",
    }]);
    expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
    expect(testStreamSwitcher.getEventId()).toEqual(null);
  });

  it("should switch from linear-vod (session) to event-livestream (sessionLive) according to schedule", async () => {
    const switchMgr = new TestSwitchManager(0);
    const testStreamSwitcher = new StreamSwitcher({ streamSwitchManager: switchMgr });
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);
    spyOn(sessionLive, "resetLiveStoreAsync").and.callFake(() => true);
    spyOn(sessionLive, "resetSession").and.callFake(() => true);
    spyOn(sessionLive, "getCurrentMediaSequenceSegments").and.returnValue({ currMseqSegs: mockLiveSegments, segCount: 8 });

    await session.initAsync();
    await session.incrementAsync();
    await sessionLive.initAsync();
    sessionLive.startPlayheadAsync();

    expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
    expect(testStreamSwitcher.getEventId()).toBe(null);
    jasmine.clock().mockDate(tsNow);
    jasmine.clock().tick((25 * 1000));
    expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
    expect(testStreamSwitcher.getEventId()).toBe("live-0");
  });

  it("should switch from event-livestream (sessionLive) to linear-vod (session) according to schedule", async () => {
    const switchMgr = new TestSwitchManager(0);
    const testStreamSwitcher = new StreamSwitcher({ streamSwitchManager: switchMgr });
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);
    spyOn(sessionLive, "resetLiveStoreAsync").and.callFake(() => true);
    spyOn(sessionLive, "resetSession").and.callFake(() => true);
    spyOn(sessionLive, "getCurrentMediaSequenceSegments").and.returnValue({ currMseqSegs: mockLiveSegments, segCount: 8 });
    spyOn(session, "setCurrentMediaSequenceSegments").and.returnValue(true);
    spyOn(session, "setCurrentMediaAndDiscSequenceCount").and.returnValue(true);

    await session.initAsync();
    await session.incrementAsync();
    await sessionLive.initAsync();
    sessionLive.startPlayheadAsync();

    jasmine.clock().mockDate(tsNow);
    jasmine.clock().tick((25 * 1000));
    expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
    expect(testStreamSwitcher.getEventId()).toBe("live-0");
    jasmine.clock().tick((60 * 1000 + 25 * 1000));
    expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
    expect(testStreamSwitcher.getEventId()).toBe(null);
  });

  it("should switch from linear-vod (session) to event-vod (session) according to schedule", async () => {
    const switchMgr = new TestSwitchManager(1);
    const testStreamSwitcher = new StreamSwitcher({ streamSwitchManager: switchMgr });
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    spyOn(session, "setCurrentMediaSequenceSegments").and.returnValue(true);
    spyOn(session, "setCurrentMediaAndDiscSequenceCount").and.returnValue(true);

    await session.initAsync();
    await session.incrementAsync();


    expect(await testStreamSwitcher.streamSwitcher(session, null)).toBe(false);
    expect(testStreamSwitcher.getEventId()).toBe(null);
    jasmine.clock().mockDate(tsNow);
    jasmine.clock().tick((25 * 1000));
    expect(await testStreamSwitcher.streamSwitcher(session, null)).toBe(false);
    expect(testStreamSwitcher.getEventId()).toBe("vod-0");
  });

  it("should switch from event-vod (session) to linear-vod (session) according to schedule", async () => {
    const switchMgr = new TestSwitchManager(1);
    const testStreamSwitcher = new StreamSwitcher({ streamSwitchManager: switchMgr });
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    spyOn(session, "setCurrentMediaSequenceSegments").and.returnValue(true);
    spyOn(session, "setCurrentMediaAndDiscSequenceCount").and.returnValue(true);

    await session.initAsync();
    await session.incrementAsync();

    jasmine.clock().mockDate(tsNow);
    jasmine.clock().tick((25 * 1000));
    expect(await testStreamSwitcher.streamSwitcher(session, null)).toBe(false);
    expect(testStreamSwitcher.getEventId()).toBe("vod-0");
    jasmine.clock().tick((1 + (60 * 1000 + 25 * 1000)));
    expect(await testStreamSwitcher.streamSwitcher(session, null)).toBe(false);
    expect(testStreamSwitcher.getEventId()).toBe(null);
  });

  it("should not switch from linear-vod (session) to event-vod (session) if duration is not set in schedule", async () => {
    const switchMgr = new TestSwitchManager(6);
    const testStreamSwitcher = new StreamSwitcher({ streamSwitchManager: switchMgr });
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);

    await session.initAsync();
    await session.incrementAsync();

    jasmine.clock().mockDate(tsNow);
    jasmine.clock().tick((25 * 1000));
    expect(await testStreamSwitcher.streamSwitcher(session, null)).toBe(false);
    expect(testStreamSwitcher.getEventId()).toBe(null);
  });

  xit("should switch from linear-vod (session) to event-livestream (sessionLive) according to schedule. " +
    "\nThen directly to event-vod (session) and finally switch back to linear-vod (session)", async () => {
      const switchMgr = new TestSwitchManager(2);
      const testStreamSwitcher = new StreamSwitcher({ streamSwitchManager: switchMgr });
      const assetMgr = new TestAssetManager();
      const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
      const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);
      spyOn(sessionLive, "_loadAllPlaylistManifests").and.returnValue(mockLiveSegments);

      await session.initAsync();
      await session.incrementAsync();
      sessionLive.initAsync();

      jasmine.clock().mockDate(tsNow);
      jasmine.clock().tick((25 * 1000));
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
      expect(testStreamSwitcher.getEventId()).toBe("live-1");
      jasmine.clock().tick((1 + (60 * 1000 + 20 * 1000)));
      await sessionLive.getCurrentMediaManifestAsync(180000);
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
      expect(testStreamSwitcher.getEventId()).toBe("vod-1");
      jasmine.clock().tick((1 + (60 * 1000 + 20 * 1000)) + (60 * 1000));
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
      expect(testStreamSwitcher.getEventId()).toBe(null);
    });

  xit("should switch from linear-vod (session) to event-livestream (sessionLive) according to schedule. " +
    "\nThen directly to 2nd event-livestream (sessionLive) and finally switch back to linear-vod (session)", async () => {
      const switchMgr = new TestSwitchManager(3);
      const testStreamSwitcher = new StreamSwitcher({ streamSwitchManager: switchMgr });
      const assetMgr = new TestAssetManager();
      const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
      const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);
      spyOn(sessionLive, "_loadAllPlaylistManifests").and.returnValue(mockLiveSegments);

      await session.initAsync();
      await session.incrementAsync();
      await sessionLive.initAsync();

      jasmine.clock().mockDate(tsNow);
      jasmine.clock().tick((25 * 1000));
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
      expect(testStreamSwitcher.getEventId()).toBe("live-2");
      jasmine.clock().tick((1 + (60 * 1000 + 20 * 1000)));
      await sessionLive.getCurrentMediaManifestAsync(180000);
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
      expect(testStreamSwitcher.getEventId()).toBe("live-3");
      jasmine.clock().tick((1 + (60 * 1000 + 20 * 1000)) + (60 * 1000));
      await sessionLive.getCurrentMediaManifestAsync(180000);
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
      expect(testStreamSwitcher.getEventId()).toBe(null);
    });

  it("should switch from linear-vod (session) to event-vod (session) according to schedule. " +
    "\nThen directly to event-livestream (sessionLive) and finally switch back to linear-vod (session)", async () => {
      const switchMgr = new TestSwitchManager(4);
      const testStreamSwitcher = new StreamSwitcher({ streamSwitchManager: switchMgr });
      const assetMgr = new TestAssetManager();
      const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
      const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);
      spyOn(sessionLive, "resetLiveStoreAsync").and.callFake(() => true);
      spyOn(sessionLive, "resetSession").and.callFake(() => true);
      spyOn(sessionLive, "getCurrentMediaSequenceSegments").and.returnValue({ currMseqSegs: mockLiveSegments, segCount: 8 });
      spyOn(session, "setCurrentMediaSequenceSegments").and.returnValue(true);
      spyOn(session, "setCurrentMediaAndDiscSequenceCount").and.returnValue(true);

      await session.initAsync();
      await session.incrementAsync();
      await sessionLive.initAsync();

      sessionLive.startPlayheadAsync();

      jasmine.clock().mockDate(tsNow);
      jasmine.clock().tick((25 * 1000));
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
      expect(testStreamSwitcher.getEventId()).toBe("vod-2");
      jasmine.clock().tick((1 + (60 * 1000 + 20 * 1000)));
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(true);
      expect(testStreamSwitcher.getEventId()).toBe("live-4");
      jasmine.clock().tick((1 + (60 * 1000 + 20 * 1000)) + (60 * 1000));
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
      expect(testStreamSwitcher.getEventId()).toBe(null);
    });

  it("should switch from linear-vod (session) to event-vod (session) according to schedule. " +
    "\nThen directly to 2nd event-vod (session) and finally switch back to linear-vod (session)", async () => {
      const switchMgr = new TestSwitchManager(5);
      const testStreamSwitcher = new StreamSwitcher({ streamSwitchManager: switchMgr });
      const assetMgr = new TestAssetManager();
      const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
      const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);
      spyOn(sessionLive, "_loadAllPlaylistManifests").and.returnValue(mockLiveSegments);
      spyOn(session, "setCurrentMediaSequenceSegments").and.returnValue(true);
      spyOn(session, "setCurrentMediaAndDiscSequenceCount").and.returnValue(true);

      await session.initAsync();
      await session.incrementAsync();
      await sessionLive.initAsync();

      jasmine.clock().mockDate(tsNow);
      jasmine.clock().tick((25 * 1000));
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
      expect(testStreamSwitcher.getEventId()).toBe("vod-3");
      jasmine.clock().tick((1 + (60 * 1000 + 20 * 1000)));
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
      expect(testStreamSwitcher.getEventId()).toBe("vod-4");
      jasmine.clock().tick((1 + (60 * 1000 + 20 * 1000)) + (60 * 1000));
      expect(await testStreamSwitcher.streamSwitcher(session, sessionLive)).toBe(false);
      expect(testStreamSwitcher.getEventId()).toBe(null);
    });

  it("should merge audio segments correctly", async () => {
    const switchMgr = new TestSwitchManager(5);
    const sessionLive = new StreamSwitcher({ streamSwitchManager: switchMgr });
    const fromSegments = {
      aac: {
        en: [
          {
            id: 1,
            uri: "en1.m3u8"
          },
          {
            id: 2,
            uri: "en2.m3u8"
          },
          {
            id: 3,
            uri: "en3.m3u8"
          },
        ],
        es: [
          {
            id: 1,
            uri: "es1.m3u8"
          },
          {
            id: 2,
            uri: "es2.m3u8"
          },
          {
            id: 3,
            uri: "es3.m3u8"
          },
        ]
      }

    };
    const toSegments = {
      aac: {
        en: [
          {
            id: 4,
            uri: "en4.m3u8"
          },
          {
            id: 5,
            uri: "en5.m3u8"
          },
          {
            id: 6,
            uri: "en6.m3u8"
          },
        ]
      }
    };
    let newList = sessionLive._mergeAudioSegments(toSegments, fromSegments, true);
    let result = {
      aac: {
        en: [
          { discontinuity: true },
          { id: 4, uri: 'en4.m3u8' },
          { id: 5, uri: 'en5.m3u8' },
          { id: 6, uri: 'en6.m3u8' },
          { id: 1, uri: 'en1.m3u8' },
          { id: 2, uri: 'en2.m3u8' },
          { id: 3, uri: 'en3.m3u8' }
        ],
        es: [
          { discontinuity: true },
          { id: 4, uri: 'en4.m3u8' },
          { id: 5, uri: 'en5.m3u8' },
          { id: 6, uri: 'en6.m3u8' },
          { id: 1, uri: 'es1.m3u8' },
          { id: 2, uri: 'es2.m3u8' },
          { id: 3, uri: 'es3.m3u8' }
        ]
      }
    }
    
    expect(newList).toEqual(result);
  });
});

describe("The Stream Switcher adaptive poll cadence (#366)", () => {
  const tsNow2 = Date.now();
  // Mirror of Server.computeStreamSwitchIntervalMs(): pick the MINIMUM interval any
  // channel requires from each switcher's getEventPollingState(). Kept here so the
  // aggregation rule is exercised against real StreamSwitcher instances without
  // standing up a full Server/TypeScript build in the jasmine specs.
  const computeInterval = (switchers, cfg) => {
    let desiredMs = cfg.base;
    for (const s of switchers) {
      const state = s.getEventPollingState();
      let channelMs;
      if (state.nearEnd) {
        channelMs = cfg.nearEnd;
      } else if (state.midEvent) {
        channelMs = cfg.ongoing;
      } else {
        channelMs = cfg.base;
      }
      if (channelMs < desiredMs) {
        desiredMs = channelMs;
      }
    }
    return desiredMs;
  };
  const CFG = { base: 3000, ongoing: 3000, nearEnd: 1000 };

  beforeEach(() => {
    jasmine.clock().install();
    jasmine.clock().mockDate(new Date(tsNow2));
  });
  afterEach(() => {
    jasmine.clock().uninstall();
  });

  const midEventSwitcher = () => {
    const s = new StreamSwitcher({ streamSwitchManager: new TestSwitchManager(0) });
    s.streamTypeLive = true;
    s.timeDiff = { start_time: tsNow2 - 30 * 1000, end_time: tsNow2 + 60 * 1000 };
    return s;
  };
  const nearEndSwitcher = () => {
    const s = new StreamSwitcher({ streamSwitchManager: new TestSwitchManager(0) });
    s.streamTypeLive = true;
    s.timeDiff = { start_time: tsNow2 - 60 * 1000, end_time: tsNow2 + 5 * 1000 };
    return s;
  };

  it("reports no ongoing event when there is no schedule / not live => base interval", () => {
    const s = new StreamSwitcher({ streamSwitchManager: new TestSwitchManager(0) });
    // Not live, no scheduleObj tracked yet.
    const state = s.getEventPollingState();
    expect(state.midEvent).toBe(false);
    expect(state.nearEnd).toBe(false);
    expect(state.msUntilEnd).toBe(null);
    expect(computeInterval([s], CFG)).toBe(CFG.base);
  });

  it("backs off to the ongoing interval while mid-event and not near end", () => {
    const s = midEventSwitcher();
    const state = s.getEventPollingState();
    expect(state.midEvent).toBe(true);
    expect(state.nearEnd).toBe(false);
    expect(state.msUntilEnd).toBe(60 * 1000);
    expect(computeInterval([s], CFG)).toBe(CFG.ongoing);
  });

  it("speeds up to the near-end interval as end_time comes within the 10000ms guard", () => {
    const s = midEventSwitcher();
    expect(s.getEventPollingState().nearEnd).toBe(false);
    // Advance until only 5s remain of the event => within the 10000ms near-end guard.
    jasmine.clock().tick(55 * 1000);
    const state = s.getEventPollingState();
    expect(state.midEvent).toBe(true);
    expect(state.nearEnd).toBe(true);
    expect(state.msUntilEnd).toBe(5 * 1000);
    expect(computeInterval([s], CFG)).toBe(CFG.nearEnd);
  });

  it("takes the minimum interval across channels (one near-end pulls the loop fast)", () => {
    const ongoing = midEventSwitcher();
    const nearEnd = nearEndSwitcher();
    // All-ongoing => back off.
    expect(computeInterval([ongoing, midEventSwitcher()], CFG)).toBe(CFG.ongoing);
    // Any near-end => the whole loop drops to the fast near-end interval.
    expect(computeInterval([ongoing, nearEnd], CFG)).toBe(CFG.nearEnd);
    // Any channel with no ongoing event keeps the base/fast interval too.
    const idle = new StreamSwitcher({ streamSwitchManager: new TestSwitchManager(0) });
    expect(computeInterval([ongoing, idle], CFG)).toBe(CFG.base);
  });
});