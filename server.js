/*
 * Reference implementation of Channel Engine library
 */

const ChannelEngine = require("./index.js");
const { v4: uuidv4 } = require('uuid');
const fetch = require("node-fetch");
const { AbortController } = require("abort-controller");

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
          breaks: []
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
class RefStreamSwitchManager {
  constructor() {
    this.schedule = [];
    this.SCHEDULE_ENDPOINT = process.env.SWITCH_MGR_URL || "http://localhost:8001/api/v1/schedule";
  }

  generateID() {
    return uuidv4();
  }

  async getSchedule(channelId) {
    
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      debug(`[${this.sessionId}]: Request Timeout! Aborting Request to ${this.SCHEDULE_ENDPOINT}`);
      controller.abort();
    }, 3000);

    try {
      const res = await fetch(`${this.SCHEDULE_ENDPOINT}/${channelId}`, { signal: controller.signal });
      if(res.ok){
        const data = await res.json();
        this.schedule = data;
      } else {
        this.schedule = [];
      }
    } catch (e) {
      console.log(e);
    } finally {
      clearTimeout(timeout);
    }
    return this.schedule;
  }
}

const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();
const refStreamSwitchManager = new RefStreamSwitchManager();

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
