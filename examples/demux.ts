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
  SubtitleTracks,
} from "../index";

class RefAssetManager implements IAssetManager {
  private assets;
  private pos;
  constructor(opts?) {
    this.assets = {
      1: [
        {
          id: 1,
          title: "Elephants dream",
          uri: "https://mtoczko.github.io/hls-test-streams/test-audio-pdt/playlist.m3u8",
        },
        
      ],
      2: [
        {
          id: 2,
          title: "DEV DEMUX ASSET ts but perfect match langs",
          uri: "https://trailer-admin-cdn.a2d.tv/virtualchannels/dev_asset_001/demux/demux2.2.m3u8",
        },
      ],
      3: [
        {
          id: 3,
          title: "DEV DEMUX ASSET ts but has 3 langs not 2",
          uri: "https://trailer-admin-cdn.a2d-dev.tv/demux/asset_001/master_720360enspde.m3u8",
        },
      ],
    };
    this.pos = {
      1: 0,
      2: 0,
      3: 0,
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
        resolve(vod);
      } else {
        reject("Invalid channelId provided");
      }
    });
  }
}

class RefChannelManager implements IChannelManager {
  getChannels(): Channel[] {
    return [
    //{ id: "1", profile: this._getProfile(), audioTracks: this._getAudioTracks(), subtitleTracks: this._getSubtitleTracks() },
    //{ id: "2", profile: this._getProfile(), audioTracks: this._getAudioTracks(), subtitleTracks: this._getSubtitleTracks() },
    { id: "3", profile: this._getProfile(), audioTracks: [
      { language: "en", name: "English", default: true },
      { language: "sp", name: "Spanish", default: false },
      { language: "de", name: "German", default: false },
    ], subtitleTracks: this._getSubtitleTracks() }];
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
      { language: "sp", name: "Spanish", default: false },
    ];
  }
  _getSubtitleTracks(): SubtitleTracks[] {
    return [
      // { language: "zh", name: "chinese", default: true },
      // { language: "fr", name: "french", default: false }
    ];
  }
}

const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();

const engineOptions: ChannelEngineOpts = {
  heartbeat: "/",
  averageSegmentDuration: 2000,
  channelManager: refChannelManager,
  defaultSlateUri: "https://mtoczko.github.io/hls-test-streams/test-audio-pdt/playlist.m3u8",
  slateRepetitions: 10,
  redisUrl: process.env.REDIS_URL,
  useDemuxedAudio: true,
  alwaysNewSegments: false,
  useVTTSubtitles: true,
  vttBasePath: '/subtitles'
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.PORT || 5000);
