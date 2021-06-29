/*
 * Reference implementation of Channel Engine library
 */

const ChannelEngine = require('./index.js');

class RefAssetManager {
  constructor(opts) {
    this.assets = {
      '1': [
        //{ id: 1, title: "BBB", uri: "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8" },
        //{ id: 2, title: "10 sec segs", uri: "https://test-streams.mux.dev/test_001/stream.m3u8" },
        //{ id: 2, title: "2 Second Segments", uri: "https://bitdash-a.akamaihd.net/content/sintel/hls/playlist.m3u8"}, 
        { id: 0, title: "unhinged trailer", uri: "https://maitv-vod.lab.eyevinn.technology/UNHINGED_Trailer_2020.mp4/master.m3u8" },
        { id: 1, title: "4 sec esegs", uri: "https://bitmovin-a.akamaihd.net/content/playhouse-vr/m3u8s/105560.m3u8"},
        //"https://test-streams.mux.dev/test_001/stream.m3u8"
        // https://playertest.longtailvideo.com/adaptive/elephants_dream_v4/redundant.m3u8
        //{ id: 1, title: "VINN", uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8" },
        //{ id: 1, title: "6 sec segs", uri: "https://nfrederiksen.github.io/testing-streams-hls/hls-test-short-no-sound/playlist.m3u8" },
        //{ id: 2, title: "SHORT SLATE", uri: "https://nfrederiksen.github.io/testing-streams-hls/test-audio-enNfr/master_demux.m3u8" },
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
      { bw: 4947980, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 1024, 458 ] },
      { bw: 2749539, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 640, 286 ] },
      { bw: 550172, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [ 480, 214 ] }
    ];
  }
};

// TODO: Create a Ref Stream Switch Manager
let tsNow = Date.now();
class StreamSwitchManager {
  getSchedule() {
    let schedule = [
      { id: "test123", title: "My Live Show", start: tsNow + (20*1000), estEnd: (tsNow + (20*1000)) + (100*60*1000), uri: "https://cph-p2p-msl.akamaized.net/hls/live/2000341/test/master.m3u8" },
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
