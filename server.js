/*
 * Reference implementation of Channel Engine library
 */

const ChannelEngine = require("./index.js");
const { v4: uuidv4 } = require('uuid');

class RefAssetManager {
  constructor(opts) {
    this.assets = {
      1: [
        {
          id: 1,
          title: "VINN",
          //uri: "https://bitmovin-a.akamaihd.net/content/playhouse-vr/m3u8s/105560.m3u8",
          //uri: "https://maitv-vod.lab.eyevinn.technology/THE_OUTPOST_Trailer_2020.mp4/master.m3u8",
          //type: "gap",
          //desiredDuration: 60*1000,
          uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8",
        },
        {
          id: 2,
          title: "VOD_2",
          //uri: "https://bitmovin-a.akamaihd.net/content/playhouse-vr/m3u8s/105560.m3u8",
          //uri: "https://nfrederiksen.github.io/testing-streams-hls/test-audio-enNfr/master_demux.m3u8"
          //uri: "https://maitv-vod.lab.eyevinn.technology/THE_OUTPOST_Trailer_2020.mp4/master.m3u8"
          uri: "https://maitv-vod.lab.eyevinn.technology/BECKY_Trailer_2020.mp4/master.m3u8",
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

const tsNow = Date.now();
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

const StreamType = Object.freeze({
  LIVE: 1,
  VOD: 2,
});

class StreamSwitchManager {
  constructor() {
    this.schedule = [];
    this.eventId = 0;
  }
  // TODO: If we do not have a predefined ID generate one
  generateEventID() {
    return uuidv4();
  }

  getSchedule() {
    /*
    const tsNow = Date.now();
    const liveStreamDuration = 1 * 60 * 1000;
    const startOffset = tsNow + liveStreamDuration;
    const endTime = startOffset + liveStreamDuration;
    // Break in with live content after 1 minute of VOD2Live the first time Channel Engine starts
    // and let it play for 1 minute then break in with live content after 1 minute of VOD2Live and let it run for 1 minute
    //console.log(JSON.stringify(this.schedule));
    //this.schedule = this.schedule.filter((obj) => obj.end_time >= tsNow);
    if (this.schedule.length === 0) {
      this.schedule.push({
        eventId: this.eventId++,
        assetId: "asset-"+ this.eventId++,
        title: "Live stream test",
        type: "live",
        start_time: startOffset,
        end_time: endTime,
        duration: endTime - startOffset,
        uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8",
      });
    }*/

    this.schedule = [
      {
        eventId: "abc-100",
        type: StreamType.VOD,
        assetId: 1,
        start_time: tsNow + 20 * 1000,
        end_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
        uri: "https://maitv-vod.lab.eyevinn.technology/THE_OUTPOST_Trailer_2020.mp4/master.m3u8",
        //uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8",
        duration: 60*1000,
      },
      {
        eventId: "abc-101",
        type: StreamType.VOD,
        assetId: 2,
        start_time: tsNow + 20 * 1000 + 1 * 60 * 1000,
        end_time: tsNow + 20 * 1000 + 1 * 60 * 1000 + (30 * 1000) + 60*1000,
        uri: "https://maitv-vod.lab.eyevinn.technology/MORBIUS_Trailer_2020.mp4/master.m3u8",
        duration: 60*1000,
      },
    ];

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
