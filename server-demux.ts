/*
 * Reference implementation of Channel Engine library using demuxed VOD assets.
 */

import {
  ChannelEngine,
  ChannelEngineOpts,
  IAssetManager,
  IChannelManager,
  VodRequest,
  VodResponse,
  Channel,
  ChannelProfile,
  AudioTracks,
} from "./index";

class RefAssetManager implements IAssetManager {
  private assets;
  private pos;
  constructor(opts?) {
    this.assets = {
      1: [
        {
          id: 1,
          title: "Sintel",
          uri: "https://cdn.bitmovin.com/content/assets/sintel/hls/playlist.m3u8",
        },
        {
          id: 2,
          title: "Test HLS Bird noises (1m10s)",
          uri: "https://mtoczko.github.io/hls-test-streams/test-audio-pdt/playlist.m3u8",
        },
      ],
    };
    this.pos = {
      1: 0,
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
      if (this.assets[channelId]) {
        let vod: VodResponse = this.assets[channelId][this.pos[channelId]++];
        if (this.pos[channelId] > this.assets[channelId].length - 1) {
          this.pos[channelId] = 0;
        }
        vod.timedMetadata = {
          'start-date': new Date().toISOString(),
          'class': 'se.eyevinn.demo'
        };
        resolve(vod);
      } else {
        reject("Invalid channelId provided");
      }
    });
  }
}

class RefChannelManager implements IChannelManager {
  getChannels(): Channel[] {
    return [{ id: "1", profile: this._getProfile(), audioTracks: this._getAudioTracks() }];
  }

  _getProfile(): ChannelProfile[] {
    return [
      {
        bw: 7934000,
        codecs: "avc1.4d001f,mp4a.40.2",
        resolution: [2880, 1440],
      },
      {
        bw: 7514000,
        codecs: "avc1.4d001f,mp4a.40.2",
        resolution: [1920, 1080],
      },
      { bw: 7134000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [1280, 720] },
      { bw: 6134000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [1024, 458] },
      { bw: 2323000, codecs: "avc1.4d001f,mp4a.40.2", resolution: [640, 286] },
      { bw: 495894, codecs: "avc1.4d001f,mp4a.40.2", resolution: [480, 214] },
    ];
  }
  _getAudioTracks(): AudioTracks[] {
    return [
      { language: "en", name: "English", default: true },
      { language: "sp", name: "Spanish", default: false }
    ];
  }
}

const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();

const engineOptions: ChannelEngineOpts = {
  heartbeat: "/",
  averageSegmentDuration: 2000,
  channelManager: refChannelManager,
  defaultSlateUri: "https://maitv-vod.lab.eyevinn.technology/slate-consuo.mp4/master.m3u8",
  slateRepetitions: 10,
  redisUrl: process.env.REDIS_URL,
  useDemuxedAudio: true,
  alwaysNewSegments: true,
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.PORT || 8000);