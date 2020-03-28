/*
 * Reference implementation of Channel Engine library
 */

const ChannelEngine = require('./index.js');

class RefAssetManager {
  constructor(opts) {
    this.assets = {
      '1': [
        { id: 1, title: "Tears of Steel", uri: "https://maitv-vod.lab.eyevinn.technology/tearsofsteel_4k.mov/master.m3u8" },
        { id: 2, title: "VINN", uri: "https://maitv-vod.lab.eyevinn.technology/VINN.mp4/master.m3u8" }
      ]
    };
    this.pos = {
      '1': 0
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
      let vod = this.assets[channelId][this.pos[channelId]++];
      if (this.pos[channelId] > this.assets[channelId].length - 1) {
        this.pos[channelId] = 0;
      }
      resolve(vod);
    });
  }
};

class RefChannelManager {
  getChannels() {
    return [ { id: '1' } ];
  }
};

const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();

const engineOptions = {
  heartbeat: '/',
  averageSegmentDuration: 2000,
  channelManager: refChannelManager
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.port || 8000);

