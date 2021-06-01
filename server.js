/*
 * Reference implementation of Channel Engine library
 */

const ChannelEngine = require('./index.js');


class RefAssetManager {
  constructor(opts) {
    this.assets = {
      1: [
        {
          id: 1,
          title: "OTTera test VOD 1",
          uri: "https://cdnapisec.kaltura.com/p/513551/sp/51355100/playManifest/entryId/1_f59nwght/format/applehttp/protocol/https/flavorIds/1_1zxcyu45,1_g08twnrf,1_eo2rvctu,1_gzu2b4l0,1_jszuw76k,1_gi68n741/preferredBitrate/1800/maxBitrate/2800/defaultAudioLang/en/a.m3u8",
          //id: 1, title: "Tears of Steel", uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/master.m3u8"
        },
        {
          id: 2,
          title: "OTTera test VOD 2",
          uri: "https://cdnapisec.kaltura.com/p/513551/sp/51355100/playManifest/entryId/1_fpsqsbf4/format/applehttp/protocol/https/flavorIds/1_7lbzn50a,1_9bvnxo63,1_j873iic0,1_j9w5phf3,1_nwwuphp4,1_09sf9wgp/preferredBitrate/1800/maxBitrate/2800/defaultAudioLang/en/a.m3u8",
          //id: 2, title: "VINN", uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8"
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
        vod.timedMetadata = {
          'start-date': (new Date()).toISOString()
        };
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
    return [
      {
        id: "1",
        profile: this._getProfile(),

        //# Uncomment if using demuxed audio.
        audioTracks: this._getAudioTracks(),
      },
    ];
  }

  _getProfile() {
    return [
      { bw: 6134000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [1024, 458] },
      { bw: 2323000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [640, 286] },
      { bw: 1313000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [480, 214] },
    ];
  }

  //# Uncomment if using demuxed audio.
  _getAudioTracks() {
    return [
      { language: "en", name: "English", default: true },
      { language: "es", name: "Spanish" },
      { language: "de", name: "German" },
    ];
  }
}


const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();

const engineOptions = {
  heartbeat: '/',
  averageSegmentDuration: 2000,
  channelManager: refChannelManager,
  defaultSlateUri: "https://maitv-vod.lab.eyevinn.technology/slate-consuo.mp4/master.m3u8",
  slateRepetitions: 10,
  redisUrl: process.env.REDIS_URL,
  //# Uncomment if using demuxed audio.
  useDemuxedAudio: true,
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.port || 8000);

