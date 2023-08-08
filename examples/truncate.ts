/*
 * Reference implementation of Channel Engine library
 */

import { ChannelEngine, ChannelEngineOpts, 
  IAssetManager, IChannelManager, 
  VodRequest, VodResponse, Channel, ChannelProfile
} from "../index";

const DEFAULT_ASSETS = [
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
];

class RefAssetManager implements IAssetManager {
  private assets;
  private pos;

  constructor(opts?) {
    this.assets = {};
    this.assets['1'] = DEFAULT_ASSETS;
    this.pos = {
      '1': 0,
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
  getNextVod(vodRequest: VodRequest): Promise<VodResponse> {
    return new Promise((resolve, reject) => {
      const channelId = vodRequest.playlistId;
      if (this.pos[channelId] === undefined) {
        this.pos[channelId] = 0;
        this.assets[channelId] = DEFAULT_ASSETS;
      }
      if (this.assets[channelId]) {
        let vod = this.assets[channelId][this.pos[channelId]++];
        if (this.pos[channelId] > this.assets[channelId].length - 1) {
          this.pos[channelId] = 0;
        }
        const vodResponse = {
          id: vod.id,
          title: vod.title,
          uri: vod.uri,
          desiredDuration: 50000,
          startOffset: 10000,
        };
        resolve(vodResponse);
      } else {
        reject("Invalid channelId provided");
      }
    });
  }
}

class RefChannelManager implements IChannelManager {
  private channels: Channel[] = [];

  constructor() {
    this.channels.push({ id: "1", profile: this._getProfile() });
  }

  getChannels(): Channel[] {
    return this.channels;
  }

  _getProfile(): ChannelProfile[] {
    return [
      { bw: 6134000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [1024, 458] },
      { bw: 2323000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [640, 286] },
      { bw: 1313000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [480, 214] },
    ];
  }
}

const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();

const engineOptions: ChannelEngineOpts = {
  heartbeat: "/",
  averageSegmentDuration: 2000,
  channelManager: refChannelManager,
  defaultSlateUri:
    "https://maitv-vod.lab.eyevinn.technology/slate-consuo.mp4/master.m3u8",
  slateRepetitions: 10,
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.PORT || 8000);
