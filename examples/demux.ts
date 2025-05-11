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
          title: "Elephants dream (TS) - 3 audio tracks",
          uri: "https://cdn.theoplayer.com/video/elephants-dream/playlist.m3u8",
        },
        
      ],
      2: [
        {
          id: 2,
          title: "DEV DEMUX ASSET (TS) - 2 audio tracks",
          uri: "https://testcontent.eyevinn.technology/ce_test_content/DEMUX_DEMO_VOD_TS_010/master_720360ensp.m3u8",
        },
      ],
      3: [
        {
          id: 3,
          title: "DEV DEMUX ASSET (TS) - 3 audio tracks",
          uri: "https://testcontent.eyevinn.technology/ce_test_content/DEMUX_DEMO_VOD_TS_011/master_720360enspde.m3u8",
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
      // { language: "zho", name: "chinese", default: true },
      // { language: "fra", name: "french", default: false }
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
