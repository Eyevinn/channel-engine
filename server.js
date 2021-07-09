/*
 * Reference implementation of Channel Engine library
 */

const ChannelEngine = require("./index.js");

class RefAssetManager {
  constructor(opts) {
    this.assets = {
      1: [
        {
          id: 1,
          title: "VINN",
          uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8",
        },
        {
          id: 2,
          title: "MORBIUS",
          uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8"
        },
      ],
    };
    this.pos = {
      '1': 1,
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
        resolve(vod);
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
      { bw: 6134000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 1024, 458 ] },
      { bw: 2323000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 640, 286 ] },
      { bw: 1313000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 480, 214 ] },
    ];
  }
}

class StreamSwitchManager {
  constructor() {
    this.schedule = [];
    this.scheduleID = 0;
  }
  getSchedule() {
    const tsNow = Date.now();
    const liveStreamDuration = 1 * 60 * 1000;
    const startOffset = tsNow + liveStreamDuration;
    const endTime = startOffset + liveStreamDuration;
    // Break in with live content after 1 minute of VOD2Live the first time Channel Engine starts
    // and let it play for 1 minute then break in with live content after 1 minute of VOD2Live and let it run for 1 minute
    console.log(JSON.stringify(this.schedule));
    this.schedule = this.schedule.filter((obj) => obj.estEnd >= tsNow);
    if (this.schedule.length === 0) {
      this.schedule.push({
        id: this.scheduleID++,
        start: startOffset,
        estEnd: endTime,
        uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8",
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
engine.listen(process.env.port || 8000);
