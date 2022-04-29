const SessionLive = require("../../engine/session_live.js");
const Session = require("../../engine/session.js");
const nock = require("nock");
const { v4: uuidv4 } = require("uuid");
const { SessionStateStore, SessionState } = require("../../engine/session_state.js");
const { PlayheadStateStore } = require("../../engine/playhead_state.js");
const { SessionLiveStateStore } = require("../../engine/session_live_state.js");

const mockLiveUri = "https://mock.mock.com/live/master.m3u8";
const mockBaseUri = "https://mock.mock.com/";

const mockMasterM3U8 = `#EXTM3U
#EXT-X-VERSION:3

#EXT-X-STREAM-INF:BANDWIDTH=550001,RESOLUTION=480x240
level_0.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=1650001,RESOLUTION=640x266
level_1.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=2749001,RESOLUTION=1280x534
level_2.m3u8
`;

const mockMediaM3U8_0 = [
  `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:10
#EXTINF:6.000000,
segment_0_0.ts
#EXTINF:6.000000,
segment_0_1.ts
#EXTINF:6.000000,
segment_0_2.ts
#EXTINF:6.000000,
segment_0_3.ts
#EXTINF:6.000000,
segment_0_4.ts
#EXTINF:6.000000,
segment_0_5.ts
#EXTINF:6.000000,
segment_0_6.ts
`,
  `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:11
#EXTINF:6.000000,
segment_0_1.ts
#EXTINF:6.000000,
segment_0_2.ts
#EXTINF:6.000000,
segment_0_3.ts
#EXTINF:6.000000,
segment_0_4.ts
#EXTINF:6.000000,
segment_0_5.ts
#EXTINF:6.000000,
segment_0_6.ts
#EXTINF:6.000000,
segment_0_7.ts
`,
  `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:12
#EXTINF:6.000000,
segment_0_2.ts
#EXTINF:6.000000,
segment_0_3.ts
#EXTINF:6.000000,
segment_0_4.ts
#EXTINF:6.000000,
segment_0_5.ts
#EXTINF:6.000000,
segment_0_6.ts
#EXTINF:6.000000,
segment_0_7.ts
#EXTINF:6.000000,
segment_0_8.ts
`,
];
const mockMediaM3U8_1 = [
  `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:10
#EXTINF:6.000000,
segment_1_0.ts
#EXTINF:6.000000,
segment_1_1.ts
#EXTINF:6.000000,
segment_1_2.ts
#EXTINF:6.000000,
segment_1_3.ts
#EXTINF:6.000000,
segment_1_4.ts
#EXTINF:6.000000,
segment_1_5.ts
#EXTINF:6.000000,
segment_1_6.ts
`,
  `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:11
#EXTINF:6.000000,
segment_1_1.ts
#EXTINF:6.000000,
segment_1_2.ts
#EXTINF:6.000000,
segment_1_3.ts
#EXTINF:6.000000,
segment_1_4.ts
#EXTINF:6.000000,
segment_1_5.ts
#EXTINF:6.000000,
segment_1_6.ts
#EXTINF:6.000000,
segment_1_7.ts
`,
  `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:12
#EXTINF:6.000000,
segment_1_2.ts
#EXTINF:6.000000,
segment_1_3.ts
#EXTINF:6.000000,
segment_1_4.ts
#EXTINF:6.000000,
segment_1_5.ts
#EXTINF:6.000000,
segment_1_6.ts
#EXTINF:6.000000,
segment_1_7.ts
#EXTINF:6.000000,
segment_1_8.ts
`,
];
const mockMediaM3U8_2 = [
  `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:10
#EXTINF:6.000000,
segment_2_0.ts
#EXTINF:6.000000,
segment_2_1.ts
#EXTINF:6.000000,
segment_2_2.ts
#EXTINF:6.000000,
segment_2_3.ts
#EXTINF:6.000000,
segment_2_4.ts
#EXTINF:6.000000,
segment_2_5.ts
#EXTINF:6.000000,
segment_2_6.ts
`,
  `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:11
#EXTINF:6.000000,
segment_2_1.ts
#EXTINF:6.000000,
segment_2_2.ts
#EXTINF:6.000000,
segment_2_3.ts
#EXTINF:6.000000,
segment_2_4.ts
#EXTINF:6.000000,
segment_2_5.ts
#EXTINF:6.000000,
segment_2_6.ts
#EXTINF:6.000000,
segment_2_7.ts
`,
  `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:6
#EXT-X-MEDIA-SEQUENCE:12
#EXTINF:6.000000,
segment_0_2.ts
#EXTINF:6.000000,
segment_0_3.ts
#EXTINF:6.000000,
segment_0_4.ts
#EXTINF:6.000000,
segment_0_5.ts
#EXTINF:6.000000,
segment_0_6.ts
#EXTINF:6.000000,
segment_0_7.ts
#EXTINF:6.000000,
segment_0_8.ts
`,
];

const mockLiveSegments = {
  180000: [
    { duration: 7, uri: "http://mock.mock.com/180000/seg09.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg10.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg11.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg12.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg13.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg14.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg15.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg16.ts" },
    { discontinuity: true },
  ],
  1258000: [
    { duration: 7, uri: "http://mock.mock.com/180000/seg09.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg10.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg11.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg12.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg13.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg14.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg15.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg16.ts" },
    { discontinuity: true },
  ],
  2488000: [
    { duration: 7, uri: "http://mock.mock.com/180000/seg09.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg10.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg11.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg12.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg13.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg14.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg15.ts" },
    { duration: 7, uri: "http://mock.mock.com/180000/seg16.ts" },
    { discontinuity: true },
  ],
};

class TestAssetManager {
  constructor(opts, assets) {
    this.assets = [
      {
        id: 1,
        title: "Tears of Steel",
        uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/master.m3u8",
      },
      {
        id: 2,
        title: "VINN",
        uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8",
      },
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

describe("The initialize switching", () => {
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

  it("should give correct segments and sequence counts from session to sessionLive (case: V2L->LIVE)", async () => {
    nock(mockBaseUri)
      .persist()
      .get("/live/master.m3u8")
      .reply(200, mockMasterM3U8)
      .get("/live/level_0.m3u8")
      .reply(200, mockMediaM3U8_0[0])
      .get("/live/level_1.m3u8")
      .reply(200, mockMediaM3U8_1[0])
      .get("/live/level_2.m3u8")
      .reply(200, mockMediaM3U8_2[0]);
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);

    await session.initAsync();
    for (let i = 0; i < 2; i++) {
      await session.incrementAsync();
    }
    await sessionLive.initAsync();
    sessionLive.startPlayheadAsync();

    currVodSegments = await session.getCurrentMediaSequenceSegments();
    currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
    expect(currVodCounts).toEqual({
      mediaSeq: 2,
      discSeq: 0,
      vodMediaSeqVideo: 2
    });
    expect(currVodSegments[1313000][0]).toEqual({
      duration: 7.5,
      uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/600/600-00002.ts",
      timelinePosition: null,
      cue: null,
    });
    expect(currVodSegments[1313000][currVodSegments[1313000].length - 1]).toEqual({
      duration: 7.5,
      uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/600/600-00008.ts",
      timelinePosition: null,
      cue: null,
    });

    await sessionLive.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
    await sessionLive.setCurrentMediaSequenceSegments(currVodSegments);
    await sessionLive.setLiveUri(mockLiveUri);
    let counts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
    let tSegments = await sessionLive.getTransitionalSegments();
    expect(counts).toEqual({
      mediaSeq: 2,
      discSeq: 0,
    });
    expect(Object.keys(tSegments)).toEqual(Object.keys(currVodSegments));
    currVodSegments[1313000].push({ discontinuity: true, cue: { in: true } });
    expect(tSegments[1313000]).toEqual(currVodSegments[1313000]);
    nock.cleanAll();
  }, 10000);

  it("should give correct segments and sequence counts from sessionLive to session (case: LIVE->V2L)", async () => {
    nock(mockBaseUri)
      .persist()
      .get("/live/master.m3u8")
      .reply(200, mockMasterM3U8)
      .get("/live/level_0.m3u8")
      .reply(200, mockMediaM3U8_0[0])
      .get("/live/level_1.m3u8")
      .reply(200, mockMediaM3U8_1[0])
      .get("/live/level_2.m3u8")
      .reply(200, mockMediaM3U8_2[0]);
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);

    await session.initAsync();
    for (let i = 0; i < 6; i++) {
      await session.incrementAsync();
    }
    await sessionLive.initAsync();

    spyOn(sessionLive, "resetLiveStoreAsync").and.callFake(() => true);
    spyOn(sessionLive, "resetSession").and.callFake(() => true);
    spyOn(sessionLive, "getCurrentMediaSequenceSegments").and.returnValue({ currMseqSegs: mockLiveSegments, segCount: 8 });

    await sessionLive.setCurrentMediaAndDiscSequenceCount(13, 1);
    await sessionLive.setCurrentMediaSequenceSegments(mockLiveSegments);
    await sessionLive.setLiveUri(mockLiveUri);

    sessionLive.mediaSeqCount += 7;

    const currCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
    const currSegments = await sessionLive.getCurrentMediaSequenceSegments();

    session.setCurrentMediaAndDiscSequenceCount(currCounts.mediaSeq, currCounts.discSeq);
    session.setCurrentMediaSequenceSegments(currSegments.currMseqSegs, currSegments.segCount);

    await session.incrementAsync(); // sets state to VOD_RELOAD_INIT
    await session.incrementAsync(); // performs VOD_RELOAD_INIT
    await session.incrementAsync(); // sets state to VOD_PLAYING and increments with 1
    await session.incrementAsync(); // +1
    await session.incrementAsync(); // +1

    const sessionCounts = await session.getCurrentMediaAndDiscSequenceCount();
    const sessionCurrentSegs = await session.getCurrentMediaSequenceSegments();
    const size = sessionCurrentSegs["1313000"].length;

    const expectedCounts = {
      mediaSeq: 23,
      discSeq: 1,
      vodMediaSeqVideo: 3
    };

    expect(expectedCounts).toEqual(sessionCounts);
    expect(sessionCurrentSegs["1313000"][0]).toEqual({
      duration: 7,
      uri: "http://mock.mock.com/180000/seg12.ts",
    });
    expect(sessionCurrentSegs["1313000"][size - 1 - 2]).toEqual({
      duration: 7.5,
      timelinePosition: null,
      uri: 'https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/600/600-00008.ts',
      cue: null
    });
    expect(sessionCurrentSegs["1313000"][size - 1]).toEqual({
      duration: 7.5,
      uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/600/600-00010.ts",
      timelinePosition: null,
      cue: null,
    });
    nock.cleanAll();
  });

  it("should give correct segments and sequence counts from session back to session (case: V2L->VOD)", async () => {
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const vodUri = "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8";
    const vodEventDurationMs = 60 * 1000;

    await session.initAsync();
    for (let i = 0; i < 6; i++) {
      await session.incrementAsync();
    }
    const currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
    const eventSegments = await session.getTruncatedVodSegments(vodUri, vodEventDurationMs / 1000);

    session.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
    session.setCurrentMediaSequenceSegments(eventSegments, 0, true);

    await session.incrementAsync(); // sets state to VOD_RELOAD_INIT
    await session.incrementAsync(); // performs VOD_RELOAD_INIT
    await session.incrementAsync(); // sets state to VOD_PLAYING and increments with 1
    await session.incrementAsync(); // +1
    const sessionCounts = await session.getCurrentMediaAndDiscSequenceCount();

    const expectedCounts = {
      mediaSeq: 8,
      discSeq: 0,
      vodMediaSeqVideo: 2
    };

    expect(expectedCounts).toEqual(sessionCounts);

    await session.incrementAsync(); // +1
    await session.incrementAsync(); // +1

    const sessionCurrentSegs = await session.getCurrentMediaSequenceSegments();
    let manifest = await session.getCurrentMediaManifestAsync(180000);
    const m = manifest.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/);
    let mseqNo;
    if (m) {
      mseqNo = Number(m[1]);
    }
    expect(mseqNo).toBe(10);
    const size = sessionCurrentSegs["1313000"].length;
    expect(sessionCurrentSegs["1313000"][0]).toEqual({
      duration: 7.5,
      uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/600/600-00012.ts",
      timelinePosition: null,
      cue: null,
    });
    expect(sessionCurrentSegs["1313000"][size - 1 - 4]).toEqual({
      discontinuity: true,
      cue: { in: true },
    });
    const borderSegment = sessionCurrentSegs["1313000"][size - 1 - 3];
    expect(borderSegment.duration).toBe(10.880);
    expect(borderSegment.cue).toBe(null);
    expect(borderSegment.timelinePosition).toBe(null);
    expect(borderSegment.uri).toBe("https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/1000/1000-00000.ts");
    expect(borderSegment.daterange["id"]).not.toBe(null);
    expect(borderSegment.daterange["start-date"]).not.toBe(null);
    expect(borderSegment.daterange["planned-duration"]).toBe(vodEventDurationMs/1000);
    
  });

  xit("should give correct segments and sequence counts from session to sessionLive (case: VOD->LIVE)", async () => {
    let manifestNumber = 0;
    nock(mockBaseUri)
      .persist()
      .get("/live/master.m3u8")
      .reply(200, mockMasterM3U8)
      .get("/live/level_0.m3u8")
      .reply(200, () => {
        switch (manifestNumber) {
          case 0:
            return mockMediaM3U8_0[0];
          case 1:
            return mockMediaM3U8_0[1];
          default:
            return mockMediaM3U8_0[0];
        }
      })
      .get("/live/level_1.m3u8")
      .reply(200, () => {
        switch (manifestNumber) {
          case 0:
            return mockMediaM3U8_1[0];
          case 1:
            return mockMediaM3U8_1[1];
          default:
            return mockMediaM3U8_1[0];
        }
      })
      .get("/live/level_2.m3u8")
      .reply(200, () => {
        switch (manifestNumber) {
          case 0:
            return mockMediaM3U8_2[0];
          case 1:
            return mockMediaM3U8_2[1];
          default:
            return mockMediaM3U8_2[0];
        }
      });
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);
    const vodUri = "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8";
    const vodEventDurationMs = 70 * 1000;

    await session.initAsync();
    for (let i = 0; i < 3; i++) {
      await session.incrementAsync();
    }
    spyOn(sessionLive, "resetLiveStoreAsync").and.callFake(() => true);
    spyOn(sessionLive, "resetSession").and.callFake(() => true);
    spyOn(sessionLive, "getCurrentMediaSequenceSegments").and.returnValue({ currMseqSegs: mockLiveSegments, segCount: 8 });
    await sessionLive.initAsync();
    sessionLive.startPlayheadAsync();
    const currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
    const eventSegments = await session.getTruncatedVodSegments(vodUri, vodEventDurationMs / 1000);
    await session.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
    await session.setCurrentMediaSequenceSegments(eventSegments, 0, true);
    for (let i = 0; i < 8; i++) {
      await session.incrementAsync();
    }
    // Get from session
    const newVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
    const newVodSegments = await session.getCurrentMediaSequenceSegments();
    // Set to sessionLive
    await sessionLive.setCurrentMediaAndDiscSequenceCount(newVodCounts.mediaSeq, newVodCounts.discSeq);
    await sessionLive.setCurrentMediaSequenceSegments(newVodSegments);
    await sessionLive.setLiveUri(mockLiveUri);
    // Get and inspect counts from sessionLive
    const manifest = ""; //await sessionLive.getCurrentMediaManifestAsync(180000);
    const m = manifest.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/);
    let mseqNo = 0;
    if (m) {
      mseqNo = Number(m[1]);
    }
    expect(mseqNo).toBe(11);
    manifestNumber++;
    const liveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
    expect(liveCounts).toEqual({
      mediaSeq: 11,
      discSeq: 1,
    });
    // Get and inspect segments from sessionLive
    const liveSegments = await sessionLive.getCurrentMediaSequenceSegments();
    await sessionLive.resetSession();
    const size = liveSegments.currMseqSegs["1258000"].length;
    expect(liveSegments.currMseqSegs["1258000"][size - 1]).toEqual({ discontinuity: true });
    expect(liveSegments.currMseqSegs["1258000"][size - 1 - 1]).toEqual({
      duration: 6,
      uri: "http://mock.mock.com/180000/seg16.ts",
    });
    expect(liveSegments.currMseqSegs["1258000"][size - 1 - 1 - 1]).toEqual({
      discontinuity: true,
    });
    expect(liveSegments.currMseqSegs["1258000"][size - 1 - 1 - 1 - 1]).toEqual({
      duration: 7.2,
      uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/1000/1000-00007.ts",
      timelinePosition: null,
      cue: null,
    });
    nock.cleanAll();
  });

  xit("should give correct segments and sequence counts from sessionLive to session (case: LIVE->VOD)", async () => {
    let manifestNumber = 0;
    nock(mockBaseUri)
      .persist()
      .get("/live/master.m3u8")
      .reply(200, mockMasterM3U8)
      .get("/live/level_0.m3u8")
      .reply(200, () => {
        switch (manifestNumber) {
          case 0:
            return mockMediaM3U8_0[0];
          case 1:
            return mockMediaM3U8_0[1];
          case 2:
            return mockMediaM3U8_0[2];
          default:
            return mockMediaM3U8_0[0];
        }
      })
      .get("/live/level_1.m3u8")
      .reply(200, () => {
        switch (manifestNumber) {
          case 0:
            return mockMediaM3U8_1[0];
          case 1:
            return mockMediaM3U8_1[1];
          case 2:
            return mockMediaM3U8_1[2];
          default:
            return mockMediaM3U8_1[0];
        }
      })
      .get("/live/level_2.m3u8")
      .reply(200, () => {
        switch (manifestNumber) {
          case 0:
            return mockMediaM3U8_2[0];
          case 1:
            return mockMediaM3U8_2[1];
          case 2:
            return mockMediaM3U8_2[2];
          default:
            return mockMediaM3U8_2[0];
        }
      });
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);

    const vodUri = "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8";
    const vodEventDurationMs = 70 * 1000;

    await session.initAsync();
    for (let i = 0; i < 6; i++) {
      await session.incrementAsync();
    }
    await sessionLive.initAsync();

    await sessionLive.setCurrentMediaAndDiscSequenceCount(13, 1);
    await sessionLive.setCurrentMediaSequenceSegments(mockLiveSegments);
    await sessionLive.setLiveUri(mockLiveUri);
    await sessionLive.getCurrentMediaManifestAsync(180000);

    manifestNumber = 2;

    const currCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
    const currSegments = await sessionLive.getCurrentMediaSequenceSegments();
    await sessionLive.resetSessionAsync();

    await session.setCurrentMediaAndDiscSequenceCount(currCounts.mediaSeq, currCounts.discSeq);
    await session.setCurrentMediaSequenceSegments(currSegments);

    const eventSegments = await session.getTruncatedVodSegments(vodUri, vodEventDurationMs / 1000);
    await session.setCurrentMediaSequenceSegments(eventSegments, true);

    await session.incrementAsync();

    const manifest = await session.getCurrentMediaManifestAsync(180000);
    const newSegments = await session.getCurrentMediaSequenceSegments();
    const size = newSegments["1313000"].length;
    const m = manifest.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/);
    const d = manifest.match(/#EXT-X-DISCONTINUITY-SEQUENCE:(\d+)/);
    let mseqNo = 0;
    let dseqNo = 0;
    if (m && d) {
      mseqNo = Number(m[1]);
      dseqNo = Number(d[1]);
    }
    expect(mseqNo).toBe(14);
    expect(dseqNo).toBe(1);
    expect(newSegments["1313000"][size - 1].daterange["planned-duration"]).toEqual(vodEventDurationMs / 1000);
    expect("start-date" in newSegments["1313000"][size - 1].daterange).toBe(true);
    expect("id" in newSegments["1313000"][size - 1].daterange).toBe(true);
    expect(newSegments["1313000"][size - 1].duration).toEqual(10.88);
    expect(newSegments["1313000"][size - 1].timelinePosition).toEqual(null);
    expect(newSegments["1313000"][size - 1].cue).toEqual(null);
    expect(newSegments["1313000"][size - 1].uri).toEqual("https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/1000/1000-00000.ts");
    expect(newSegments["1313000"][size - 1 - 1]).toEqual({
      discontinuity: true,
    });
    expect(newSegments["1313000"][size - 1 - 1 - 1]).toEqual({
      duration: 6,
      uri: "https://mock.mock.com/live/segment_0_8.ts",
    });
    expect(newSegments["1313000"][size - 1 - 1 - 1 - 1 - 1]).toEqual({
      discontinuity: true,
    });

    nock.cleanAll();
  });

  it("should give correct segments and sequence counts from session to session (case: VOD->VOD)", async () => {
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const vodUri = "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8";
    const vodUri2 = "https://maitv-vod.lab.eyevinn.technology/BECKY_Trailer_2020.mp4/master.m3u8";
    const vodEventDurationMs = 70 * 1000;
    const vodEventDurationMs2 = 70 * 1000;

    await session.initAsync();
    for (let i = 0; i < 3; i++) {
      await session.incrementAsync();
    }
    const currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
    const eventSegments = await session.getTruncatedVodSegments(vodUri, vodEventDurationMs / 1000);
    session.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
    session.setCurrentMediaSequenceSegments(eventSegments, 0, true);

    await session.incrementAsync(); // sets state to VOD_RELOAD_INIT
    await session.incrementAsync(); // performs VOD_RELOAD_INIT
    await session.incrementAsync(); // sets state to VOD_PLAYING and increments with 1

    for (let i = 0; i < 10; i++) {
      await session.incrementAsync();
    }
    const newVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
    let newVodSegments = await session.getCurrentMediaSequenceSegments();
    const eventSegments2 = await session.getTruncatedVodSegments(vodUri2, vodEventDurationMs2 / 1000);

    session.setCurrentMediaAndDiscSequenceCount(newVodCounts.mediaSeq, newVodCounts.discSeq);
    session.setCurrentMediaSequenceSegments(eventSegments2, 0, true);

    await session.incrementAsync(); // sets state to VOD_RELOAD_INIT
    await session.incrementAsync(); // performs VOD_RELOAD_INIT
    await session.incrementAsync(); // sets state to VOD_PLAYING and increments with 1

    for (let i = 0; i < 2; i++) {
      await session.incrementAsync();
    }

    newVodSegments = await session.getCurrentMediaSequenceSegments();
    const borderSegment = newVodSegments["1313000"][6];

    const expectedLastVODSegItem = {
      discontinuity: true,
      daterange: null
    }
    const expectedFirstV2LSegItem = {
      duration: 7.2,
      timelinePosition: null,
      cue: null,
      uri: 'https://maitv-vod.lab.eyevinn.technology/VINN.mp4/600/600-00001.ts'
    }
    const expectedSecondVODSegItem = {
      duration: 7.5075,
      uri: "https://maitv-vod.lab.eyevinn.technology/BECKY_Trailer_2020.mp4/1000/1000-00001.ts",
      timelinePosition: null,
      cue: null,
    }

    expect(newVodSegments["1313000"][0]).toEqual(expectedLastVODSegItem);
    expect(newVodSegments["1313000"][1]).toEqual({
      duration: 10.846444,
      timelinePosition: null,
      cue: null,
      uri: 'https://maitv-vod.lab.eyevinn.technology/VINN.mp4/600/600-00000.ts'
    });
    expect(newVodSegments["1313000"][2]).toEqual(expectedFirstV2LSegItem);
    // expect(newVodSegments["1313000"][5]).toEqual({
    //   discontinuity: true,
    //   cue: { in: true },
    // });
    expect(borderSegment.daterange["planned-duration"]).toEqual(vodEventDurationMs2 / 1000);
    expect("start-date" in borderSegment.daterange).toBe(true);
    expect("id" in borderSegment.daterange).toBe(true);
    expect(borderSegment.duration).toEqual(11.3447);
    expect(borderSegment.timelinePosition).toEqual(null);
    expect(borderSegment.cue).toEqual(null);
    expect(borderSegment.uri).toEqual("https://maitv-vod.lab.eyevinn.technology/BECKY_Trailer_2020.mp4/1000/1000-00000.ts");
    expect(newVodSegments["1313000"][7]).toEqual(expectedSecondVODSegItem);
  });

  xit("should give correct segments and sequence counts from sessionLive to sessionLive (case: LIVE->LIVE)", async () => {
    let manifestNumber = 0;
    nock(mockBaseUri)
      .persist()
      .get("/live/master.m3u8")
      .reply(200, mockMasterM3U8)
      .get("/live/level_0.m3u8")
      .reply(200, () => {
        switch (manifestNumber) {
          case 0:
            return mockMediaM3U8_0[0];
          case 1:
            return mockMediaM3U8_0[1];
          case 2:
            return mockMediaM3U8_0[2];
          default:
            return mockMediaM3U8_0[0];
        }
      })
      .get("/live/level_1.m3u8")
      .reply(200, () => {
        switch (manifestNumber) {
          case 0:
            return mockMediaM3U8_1[0];
          case 1:
            return mockMediaM3U8_1[1];
          case 2:
            return mockMediaM3U8_1[2];
          default:
            return mockMediaM3U8_1[0];
        }
      })
      .get("/live/level_2.m3u8")
      .reply(200, () => {
        switch (manifestNumber) {
          case 0:
            return mockMediaM3U8_2[0];
          case 1:
            return mockMediaM3U8_2[1];
          case 2:
            return mockMediaM3U8_2[2];
          default:
            return mockMediaM3U8_2[0];
        }
      });
    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, { sessionId: "1" }, sessionStore);
    const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);

    await session.initAsync();
    for (let i = 0; i < 2; i++) {
      await session.incrementAsync();
    }
    await sessionLive.initAsync();

    currVodSegments = await session.getCurrentMediaSequenceSegments();
    currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();

    await sessionLive.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
    await sessionLive.setCurrentMediaSequenceSegments(currVodSegments);
    await sessionLive.setLiveUri(mockLiveUri);

    let manifest = await sessionLive.getCurrentMediaManifestAsync(180000);

    manifestNumber = 2;

    let liveSegs = await sessionLive.getCurrentMediaSequenceSegments();
    let liveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
    await sessionLive.resetSessionAsync();

    await sessionLive.setCurrentMediaAndDiscSequenceCount(liveCounts.mediaSeq, liveCounts.discSeq);
    await sessionLive.setCurrentMediaSequenceSegments(liveSegs);
    await sessionLive.setLiveUri(mockLiveUri);

    manifestNumber = 1;
    manifest = await sessionLive.getCurrentMediaManifestAsync(180000);

    manifestNumber = 2;
    manifest = await sessionLive.getCurrentMediaManifestAsync(1313000);

    liveSegs = await sessionLive.getCurrentMediaSequenceSegments();
    liveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();

    const size = liveSegs["550001"].length;
    const m = manifest.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/);
    const d = manifest.match(/#EXT-X-DISCONTINUITY-SEQUENCE:(\d+)/);
    let mseqNo = 0;
    let dseqNo = 0;
    if (m && d) {
      mseqNo = Number(m[1]);
      dseqNo = Number(d[1]);
    }
    expect(mseqNo).toBe(5);
    expect(dseqNo).toBe(0);
    expect(liveSegs["550001"][size - 1 - 1 - 1 - 1 - 1 - 1]).toEqual({
      discontinuity: true,
    });
    expect(liveSegs["550001"][size - 1 - 1 - 1 - 1 - 1]).toEqual({
      duration: 6,
      uri: "https://mock.mock.com/live/segment_0_7.ts",
    });
    expect(liveSegs["550001"][size - 1 - 1 - 1]).toEqual({
      discontinuity: true,
    });
    expect(liveSegs["550001"][size - 1 - 1]).toEqual({
      duration: 6,
      uri: "https://mock.mock.com/live/segment_0_8.ts",
    });
    expect(liveSegs["550001"][size - 1]).toEqual({
      discontinuity: true,
    });
    nock.cleanAll();
  });
});
