/*
 * Reference implementation of Channel Engine library
 */

const ChannelEngine = require("./index.js");
const { v4: uuidv4 } = require('uuid');

const STITCH_ENDPOINT = process.env.STITCH_ENDPOINT || "http://lambda.eyevinn.technology/stitch/master.m3u8";
class RefAssetManager {
  constructor(opts) {
    if (process.env.TEST_CHANNELS) {
      this.assets = {};
      this.pos = {};

      const testChannelsCount = parseInt(process.env.TEST_CHANNELS, 10);
      for (let i = 0; i < testChannelsCount; i++) {
        const channelId = `${i + 1}`;
        this.assets[channelId] = [
          { id: 1, title: "Tears of Steel", uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/master.m3u8" },
          { id: 2, title: "Unhinged Trailer", uri: "https://maitv-vod.lab.eyevinn.technology/UNHINGED_Trailer_2020.mp4/master.m3u8" },
          { id: 3, title: "Morbius Trailer", uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8" },
          { id: 4, title: "TV Plus Joachim", uri: "https://maitv-vod.lab.eyevinn.technology/tvplus-ad-joachim.mov/master.m3u8" },
          { id: 5, title: "The Outpost Trailer", uri: "https://maitv-vod.lab.eyevinn.technology/THE_OUTPOST_Trailer_2020.mp4/master.m3u8" },
          { id: 6, title: "TV Plus Megha", uri: "https://maitv-vod.lab.eyevinn.technology/tvplus-ad-megha.mov/master.m3u8" },
        ];
        this.pos[channelId] = 2;
      }
    } else {
      this.assets = {
        '1': [
          { id: 1, title: "Tears of Steel", uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/master.m3u8" },
          { id: 2, title: "Morbius Trailer", uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8" },
          { id: 3, title: "The Outpost Trailer", uri: "https://maitv-vod.lab.eyevinn.technology/THE_OUTPOST_Trailer_2020.mp4/master.m3u8" },
          { id: 4, title: "Unhinged Trailer", uri: "https://maitv-vod.lab.eyevinn.technology/UNHINGED_Trailer_2020.mp4/master.m3u8" },
          { id: 5, title: "TV Plus Megha", uri: "https://maitv-vod.lab.eyevinn.technology/tvplus-ad-megha.mov/master.m3u8" },
          { id: 6, title: "TV Plus Joachim", uri: "https://maitv-vod.lab.eyevinn.technology/tvplus-ad-joachim.mov/master.m3u8" },
        ]
      };
      this.pos = {
        '1': 1
      };
    }
  }

  /* @param {Object} vodRequest
   *   {
   *      sessionId,
   *      category,
   *      playlistId
   *   }
   */
  getNextVod(vodRequest) {
    return new Promise((resolve, reject) => {
      const channelId = vodRequest.playlistId;
      if (this.assets[channelId]) {
        let vod = this.assets[channelId][this.pos[channelId]++];
        if (this.pos[channelId] > this.assets[channelId].length - 1) {
          this.pos[channelId] = 0;
        }
        const payload = {
          uri: vod.uri,
          breaks: [
            {
              pos: 0,
              duration: 15 * 1000,
              url: "https://maitv-vod.lab.eyevinn.technology/ads/6cd7d768_e214_4ebc_9f14_7ed89710115e_mp4/master.m3u8"
            },
            {
              pos: 100,
              duration: 15 * 1000,
              url: "https://maitv-vod.lab.eyevinn.technology/ads/6cd7d768_e214_4ebc_9f14_7ed89710115e_mp4/master.m3u8"
            }
          ]
        };
        const buff = Buffer.from(JSON.stringify(payload));
        const encodedPayload = buff.toString("base64");
        const vodResponse = {
          id: vod.id,
          title: vod.title,
          uri: STITCH_ENDPOINT + "?payload=" + encodedPayload
        };
        resolve(vodResponse);
      } else {
        reject("Invalid channelId provided");
      }
    });
  }

  handleError(err, vodResponse) {
    console.error(err.message);
  }
}

class RefChannelManager {
  constructor(opts) {
    this.channels = [];
    if (process.env.TEST_CHANNELS) {
      const testChannelsCount = parseInt(process.env.TEST_CHANNELS, 10);
      for (let i = 0; i < testChannelsCount; i++) {
        this.channels.push({ id: `${i + 1}`, profile: this._getProfile() });
      }
    } else {
      this.channels = [{ id: "1", profile: this._getProfile() }];
    }
  }

  getChannels() {
    return this.channels;
  }

  _getProfile() {
    return [
      { bw: 8242000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [1024, 458] },
      { bw: 1274000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [640, 286] },
      { bw: 742000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [480, 214] },
    ]
  }
}
const StreamType = Object.freeze({
  LIVE: 1,
  VOD: 2,
});

class StreamSwitchManager {
  constructor() {
    this.schedule = [];
  }

  generateID() {
    return uuidv4();
  }

  getSchedule() {
    const tsNow = Date.now();
    const streamDuration = 60 * 1000;
    const startOffset = tsNow + streamDuration;
    const endTime = startOffset + streamDuration;
    // Break in with live and scheduled VOD content after 60 seconds of VOD2Live the first time Channel Engine starts
    // Required: "assetId", "start_time", "end_time", "uri", "duration"
    // "duration" is only required for StreamType.VOD
    this.schedule = this.schedule.filter((obj) => obj.end_time >= tsNow);
    if (this.schedule.length === 0) {
      this.schedule.push({
        eventId: this.generateID(),
        assetId: this.generateID(),
        title: "Live stream test",
        type: StreamType.LIVE,
        start_time: startOffset/2,
        end_time: 60*endTime,
        uri: "https://engine.cdn.consuo.tv/live/master.m3u8?channel=eyevinn",
        //uri: "https://csm-e-telenorse-eb.tls1.yospace.com/csm/extlive/telenorseprd01,vc-clear.m3u8?yo.ac=true&yo.d.cp=true&forceAds=true&tv4.chId=61",
      },
      {
        eventId: this.generateID(),
        assetId: this.generateID(),
        title: "Scheduled VOD test",
        type: StreamType.VOD,
        start_time: (endTime + 100*1000),
        end_time: (endTime + 100*1000) + streamDuration,
        uri: "https://maitv-vod.lab.eyevinn.technology/COME_TO_DADDY_Trailer_2020.mp4/master.m3u8",
        duration: streamDuration,
      });
    }
    return this.schedule;
  }
}

const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();
const refStreamSwitchManager = new StreamSwitchManager();

const engineOptions = {
  heartbeat: "/",
  averageSegmentDuration: 2000,
  channelManager: refChannelManager,
  streamSwitchManager: refStreamSwitchManager,
  defaultSlateUri: "https://maitv-vod.lab.eyevinn.technology/slate-consuo.mp4/master.m3u8",
  slateRepetitions: 10,
  redisUrl: process.env.REDIS_URL,
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.PORT || 8000);

/*{
  "items": {
    "PlaylistItem": [
      {"attributes": {
        "attributes":{}
      },
      "properties": {
        "byteRange":null,
        "daiPlacementOpportunity": null,
        "date":null,
        "discontinuity":null,
        "duration":9,
        "title":"",
        "uri":"https://maitv-vod.lab.eyevinn.technology/tvplus-ad-megha.mov/2000/2000-00004.ts"
      }
    },
    {
      "attributes":
    {
      "attributes":{}
    },
    "properties":{
      "byteRange":null,
      "daiPlacementOpportunity":null,
      "date":null,"discontinuity":null,
      "duration":6,
      "title":"",
      "uri":"https://maitv-vod.lab.eyevinn.technology/tvplus-ad-megha.mov/2000/2000-00005.ts"
    }},
    {"attributes":{
      "attributes":{}
    },"properties":{
      "byteRange":null,
      "daiPlacementOpportunity":null,
      "date":null,
      "discontinuity":null,
      "duration":9,
      "title":""
      ,"uri":"https://maitv-vod.lab.eyevinn.technology/tvplus-ad-megha.mov/2000/2000-00006.ts"
    }
  },
  {"attributes":{
    "attributes":{}
  },
  "properties":{
    "byteRange":null,
    "daiPlacementOpportunity":null,
    "date":null,
    "discontinuity":null,
    "duration":1.867,
    "title":"",
    "uri":"https://maitv-vod.lab.eyevinn.technology/tvplus-ad-megha.mov/2000/2000-00007.ts"
  }
},
{
  "attributes":{
    "attributes":{
      "cueout":15
    }
  },
  "properties":{
    "byteRange":null,
    "daiPlacementOpportunity":null,
    "date":null,
    "discontinuity":true,
    "duration":10.88,
    "title":"","uri":"https://maitv-vod.lab.eyevinn.technology/ads/6cd7d768-e214-4ebc-9f14-7ed89710115e.mp4/2000/2000-00000.ts"
  }
},
{
  "attributes":{
    "attributes":{
      "cuein":true
    }
  },
  "properties":{
    "byteRange":null,
    "daiPlacementOpportunity":null,
    "date":null,
    "discontinuity":null,
    "duration":4.24,
    "title":"",
    "uri":"https://maitv-vod.lab.eyevinn.technology/ads/6cd7d768-e214-4ebc-9f14-7ed89710115e.mp4/2000/2000-00001.ts"
  }
}
  }
}*/