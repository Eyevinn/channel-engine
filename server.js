/*
 * Reference implementation of Channel Engine library
 */

const ChannelEngine = require('./index.js');

class RefAssetManager {
  constructor(opts) {
    this.assets = {
      '1': [
        { id: 1, title: "BBB", uri: "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8" },
        { id: 2, title: "BBB2", uri: "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8" },
        //{ id: 2, title: "VINN", uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8" },
        //{ id: 1, title: "SHORT SLATE", uri: "https://nfrederiksen.github.io/testing-streams-hls/test-audio-enNfr/master_demux.m3u8" }
      ]
    };
    this.pos = {
      '1': 1
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
};

class RefChannelManager {
  getChannels() {
    //return [ { id: '1', profile: this._getProfile() }, { id: 'faulty', profile: this._getProfile() } ];
    return [ { id: '1', profile: this._getProfile() } ];
  }
  _getProfile() {
    return [
      // { bw: 6134000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 1024, 458 ] },
      // { bw: 2323000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 640, 286 ] },
      // { bw: 1313000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 480, 214 ] }
      { bw: 3606000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 1024, 458 ] },
      { bw: 2588000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 640, 286 ] },
      { bw: 881000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 480, 214 ] }
    ];
  }
};

// TODO: Create a Ref Stream Switch Manager
let tsNow = Date.now();
class StreamSwitchManager {
  getSchedule() {
    let schedule = [
      { start: tsNow + (20*1000), estEnd: (tsNow + (20*1000)) + (60*1000*2), type: "live", uri: "https://engine.cdn.consuo.tv/live/master.m3u8?channel=eyevinn"},
      //{ start: tsNow + (20*1000) + (33*1000), estEnd: (tsNow + (20*1000)) + (45*1000), type: "live", uri: "https://engine.cdn.consuo.tv/live/master.m3u8?channel=eyevinn"},
      //{ start: tsNow + (20*1000) + (75*1000), estEnd: (tsNow + (20*1000)) + (105*1000), type: "live", uri: "https://engine.cdn.consuo.tv/live/master.m3u8?channel=eyevinn"},
    ];
    return schedule;
  }
};

const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();
const refStreamSwitchManager = new StreamSwitchManager();

const engineOptions = {
  heartbeat: '/',
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
