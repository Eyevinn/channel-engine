const SessionLive = require('../../engine/session_live.js');
const m3u8 = require('@eyevinn/m3u8');
const nock = require("nock");
const Readable = require('stream').Readable;
const { v4: uuidv4 } = require('uuid');
const { SessionLiveStateStore } = require('../../engine/session_live_state.js');
const { ExpectationFailedError } = require('restify-errors');

const mockBaseUri = "https://mock.mock.com/";
const mockLiveUri = "https://mock.mock.com/live/master.m3u8";

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

const verificationLoop = async (session, increments) => {
  let remain = increments;
  let promiseFns = [];
  while (remain > 0) {
    promiseFns.push(() => session.incrementAsync());
    remain--;
  }
  let lastUri = null;
  let lastManifest = null;
  let lastMediaSeq = null;
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
        if (lastUri && firstItem.get('uri') === lastUri && lastMediaSeq && lastMediaSeq != m3u.get('mediaSequence')) {
          console.log(lastManifest);
          console.log(manifest);
          fail(`${lastMediaSeq}:${m3u.get('mediaSequence')}:${firstItem.get('uri')} was included in last media sequence (${lastUri})`);
        }
        lastUri = firstItem.get('uri');
        lastMediaSeq = m3u.get('mediaSequence');
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

describe("SessionLive-Playhead consumer", () => {
  let sessionLiveStore = undefined;
  beforeEach(() => {
    sessionLiveStore = {
      sessionLiveStateStore: new SessionLiveStateStore(),
      instanceId: uuidv4(),
    };
  });

  it("continuously fetches new segments and stores them while also increasing media sequence", async () => {
    nock(mockBaseUri)
    .get("/live/master.m3u8")
    .reply(200, mockMasterM3U8)
    .get("/live/level_0.m3u8")
    .reply(200, mockMediaM3U8_0[0])
    .get("/live/level_1.m3u8")
    .reply(200, mockMediaM3U8_1[0])
    .get("/live/level_2.m3u8")
    .reply(200, mockMediaM3U8_2[0])
    .get("/live/master.m3u8")
    .reply(200, mockMasterM3U8)
    .get("/live/level_0.m3u8")
    .reply(200, mockMediaM3U8_0[1])
    .get("/live/level_1.m3u8")
    .reply(200, mockMediaM3U8_1[1])
    .get("/live/level_2.m3u8")
    .reply(200, mockMediaM3U8_2[1])
    .get("/live/master.m3u8")
    .reply(200, mockMasterM3U8)
    .get("/live/level_0.m3u8")
    .reply(200, mockMediaM3U8_0[2])
    .get("/live/level_1.m3u8")
    .reply(200, mockMediaM3U8_1[2])
    .get("/live/level_2.m3u8")
    .reply(200, mockMediaM3U8_2[2]);

    let TEST_MSEQ = 10;

    const sessionLive = new SessionLive({ sessionId: "1" }, sessionLiveStore);

    await sessionLive.initAsync();
    await sessionLive.setCurrentMediaAndDiscSequenceCount(TEST_MSEQ, 1);
    await sessionLive.setCurrentMediaSequenceSegments(mockLiveSegments);
    await sessionLive.setLiveUri(mockLiveUri);

    const loop = async (increments) => {
      let remain = increments;
      let lastMseqNo = 0;
      while (remain > 0) {
        await sessionLive._loadAllMediaManifests();
        let manifest = await sessionLive.getCurrentMediaManifestAsync(180000);

        const m = manifest.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/);
        let mseqNo;
        if (m) {
          mseqNo = Number(m[1]);
        }
        if (mseqNo < lastMseqNo) {
          fail(`expected ${mseqNo} to be greater than ${lastMseqNo}:\n${manifest}`);
        }
        lastMseqNo = mseqNo;
        expect(mseqNo).toBe(++TEST_MSEQ);
        remain--;
      }
    };
    await loop(3);
  });
});