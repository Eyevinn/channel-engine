const SessionLive = require('../../engine/session_live.js');
const Session = require('../../engine/session.js');
const nock = require('nock')
const { v4: uuidv4 } = require('uuid');
const { SessionStateStore } = require('../../engine/session_state.js');
const { PlayheadStateStore } = require('../../engine/playhead_state.js');

const mockLiveUri = "https://mock.mock.com/live/master.m3u8";
const mockBaseUri = "https://mock.mock.com/";

const mockMasterM3U8 = `#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=550172,RESOLUTION=256x106
level_0.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=1650064,RESOLUTION=640x266
level_1.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=2749539,RESOLUTION=1280x534
level_2.m3u8`;
const mockMediaM3U8_0 = `#EXTM3U
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
`;
const mockMediaM3U8_1 = `#EXTM3U
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
`;
const mockMediaM3U8_2 = `#EXTM3U
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
`;

const mockLiveSegments = {
  "180000": [{duration: 7,uri: "http://mock.mock.com/180000/seg09.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg10.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg11.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg12.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg13.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg14.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg15.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg16.ts"},
  {discontinuity: true }],
  "1258000": [{duration: 7,uri: "http://mock.mock.com/180000/seg09.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg10.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg11.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg12.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg13.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg14.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg15.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg16.ts"},
  {discontinuity: true }],
  "2488000": [{duration: 7,uri: "http://mock.mock.com/180000/seg09.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg10.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg11.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg12.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg13.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg14.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg15.ts"},
  {duration: 7,uri: "http://mock.mock.com/180000/seg16.ts"},
  {discontinuity: true }]
};

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

describe("The initialize switching", () => {
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

  it("should give correct segments and sequence counts from session to sessionLive (case: V2L->LIVE)", async () => {
    nock(mockBaseUri).get('/live/master.m3u8').times(1).reply(200, mockMasterM3U8);
    nock(mockBaseUri).get('/live/level_0.m3u8').times(1).reply(200, mockMediaM3U8_0);
    nock(mockBaseUri).get('/live/level_1.m3u8').times(1).reply(200, mockMediaM3U8_1);
    nock(mockBaseUri).get('/live/level_2.m3u8').times(1).reply(200, mockMediaM3U8_2);

    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, {sessionId: "1"}, sessionStore);
    const sessionLive = new SessionLive({sessionId: "1"});

    await session.initAsync();
    for (let i = 0; i < 2; i++) {
      await session.incrementAsync();
    }

    currVodSegments = await session.getCurrentMediaSequenceSegments();
    currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
    expect(currVodCounts).toEqual({
      mediaSeq: 2,
      discSeq: 0
    });
    expect(currVodSegments[1313000][0]).toEqual({
      duration: 7.5,
      uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/600/600-00002.ts",
      timelinePosition: null,
      cue: null
    });
    expect(currVodSegments[1313000][currVodSegments[1313000].length - 1]).toEqual({
      duration: 7.5,
      uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/600/600-00008.ts",
      timelinePosition: null,
      cue: null
    });

    await sessionLive.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
    await sessionLive.setCurrentMediaSequenceSegments(currVodSegments);
    await sessionLive.setLiveUri(mockLiveUri);
    let counts =  await sessionLive.getCurrentMediaAndDiscSequenceCount();
    let tSegments = await sessionLive.getTransitionalSegments();
    expect(counts).toEqual({
      mediaSeq: 1,
      discSeq: 0
    });
    expect(Object.keys(tSegments)).toEqual(Object.keys(currVodSegments));
    currVodSegments[1313000].push({discontinuity: true});
    expect(tSegments[1313000]).toEqual( currVodSegments[1313000]);
    nock.cleanAll();
  });

  it("should give correct segments and sequence counts from sessionLive to session (case: LIVE->V2L)", async () => {
    nock(mockBaseUri).get('/live/master.m3u8').times(1).reply(200, mockMasterM3U8);
    nock(mockBaseUri).get('/live/level_0.m3u8').times(1).reply(200, mockMediaM3U8_0);
    nock(mockBaseUri).get('/live/level_1.m3u8').times(1).reply(200, mockMediaM3U8_1);
    nock(mockBaseUri).get('/live/level_2.m3u8').times(1).reply(200, mockMediaM3U8_2);

    const assetMgr = new TestAssetManager();
    const session = new Session(assetMgr, {sessionId: "1"}, sessionStore);
    const sessionLive = new SessionLive({sessionId: "1"});

    spyOn(sessionLive, "_loadAllMediaManifests").and.returnValue(mockLiveSegments);
    await session.initAsync();
    for (let i = 0; i < 6; i++) {
      await session.incrementAsync();
    }

    await sessionLive.setCurrentMediaAndDiscSequenceCount(13, 1);
    await sessionLive.setCurrentMediaSequenceSegments(mockLiveSegments);
    await sessionLive.setLiveUri(mockLiveUri);

    await sessionLive.getCurrentMediaManifestAsync(180000);

    const currCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
    const currSegments = await sessionLive.getCurrentMediaSequenceSegments();

    await session.setCurrentMediaAndDiscSequenceCount(currCounts.mediaSeq, currCounts.discSeq);
    await session.setCurrentMediaSequenceSegments(currSegments);
    currCounts.mediaSeq = currCounts.mediaSeq + 2;
    await session.incrementAsync();
    // TODO: get rid off lonely reload bandwidths
    const sessionCounts = await session.getCurrentMediaAndDiscSequenceCount();
    const sessionCurrentSegs = await session.getCurrentMediaSequenceSegments();
    expect(currCounts).toEqual(sessionCounts);
    expect(currSegments).toEqual(sessionCurrentSegs);
    nock.cleanAll();
  });
});