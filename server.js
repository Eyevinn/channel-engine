/*
 * Reference implementation of Channel Engine library
 */

const ChannelEngine = require("./index.js");

const STITCH_ENDPOINT =
  process.env.STITCH_ENDPOINT ||
  "http://lambda.eyevinn.technology/stitch/master.m3u8";
class RefAssetManager {
  constructor(opts) {
    this.assets = {
      1: [
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
      ],
    };
    this.pos = {
      1: 1,
    };
  }

  /**
   *
   * @param {Object} vodRequest
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
              pos: 100,
              duration: 15 * 1000,
              url: "https://maitv-vod.lab.eyevinn.technology/ads/6cd7d768_e214_4ebc_9f14_7ed89710115e_mp4/master.m3u8",
            },
          ],
        };
        const buff = Buffer.from(JSON.stringify(payload));
        const encodedPayload = buff.toString("base64");
        const vodResponse = {
          id: vod.id,
          title: vod.title,
          uri: STITCH_ENDPOINT + "?payload=" + encodedPayload,
        };
        resolve(vodResponse);
      } else {
        reject("Invalid channelId provided");
      }
    });
  }
}

class RefChannelManager {
  getChannels() {
    //return [ { id: '1', profile: this._getProfile() }, { id: 'faulty', profile: this._getProfile() } ];
    return [{ id: "1", profile: this._getProfile() }];
  }

  _getProfile() {
    return [
      { bw: 6134000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [1024, 458] },
      { bw: 2323000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [640, 286] },
      { bw: 1313000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [480, 214] },
    ];
  }
}

const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();

const engineOptions = {
  heartbeat: "/",
  averageSegmentDuration: 2000,
  channelManager: refChannelManager,
  defaultSlateUri:
    "https://maitv-vod.lab.eyevinn.technology/slate-consuo.mp4/master.m3u8",
  slateRepetitions: 10,
  redisUrl: process.env.REDIS_URL,
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.port || 8000);
