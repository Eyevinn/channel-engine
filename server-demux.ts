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

const STITCH_ENDPOINT = "http://localhost:8000/stitch/master.m3u8";

class RefAssetManager implements IAssetManager {
  private assets;
  private pos;
  constructor(opts?) {
    this.assets = {
      1: [
        /* #TS+DEMUX HLS VODS */
        {
          id: 1,
          title: "Elephants Dream",
          uri: "https://playertest.longtailvideo.com/adaptive/elephants_dream_v4/index.m3u8",
        },
        {
          id: 2,
          title: "Test HLS Bird noises (1m10s)",
          uri: "https://mtoczko.github.io/hls-test-streams/test-audio-pdt/playlist.m3u8",
        },
        /* #CMAF+DEMUX HLS VODS */
        // {
        //   id: 1,
        //   title: "Idol Final",
        //   uri: "https://vod.streaming.a2d.tv/a07ff4eb-6770-4805-a0ad-a4d1b127880d/4fef8b00-6d0b-11ed-89b6-2b1a288899a0_20356478.ism/.m3u8"
        // },
        // {
        //   id: 2,
        //   title: "Benjamin Sjunger",
        //   uri: "https://vod.streaming.a2d.tv/3f389c48-03e3-48a2-8e98-a02c55185a68/4c792a30-89ad-11ed-95d9-1b374c4e2f9f_20411056.ism/.m3u8"
        // },
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
  getNextVod(vodRequest: VodRequest): Promise<VodResponse> {
    return new Promise((resolve, reject) => {
      const channelId = vodRequest.playlistId;
      if (this.assets[channelId]) {
        let vod: VodResponse = this.assets[channelId][this.pos[channelId]++];
        if (this.pos[channelId] > this.assets[channelId].length - 1) {
          this.pos[channelId] = 0;
        }
        vod.timedMetadata = {
          "start-date": new Date().toISOString(),
          class: "se.eyevinn.demo",
        };
        const payload = {
          uri: vod.uri,
          breaks: [
            /* #TS+DEMUX HLS BREAK VOD */
            {
              pos: 0,
              duration: 60 * 1000,
              url: "https://lab-live.cdn.eyevinn.technology/DEMUX_002/master_demux_aac-en-fr.m3u8",
            },
            /* #CMAF+DEMUX HLS BREAK VOD */
            // {
            //   pos: 0,
            //   duration: 20 * 1000,
            //   url: "https://ovpuspvod.a2d-stage.tv/trailers/63ef9c36e3ffa90028603374/output.ism/.m3u8",
            // },
          ],
        };
        const buff = Buffer.from(JSON.stringify(payload));
        const encodedPayload = buff.toString("base64");
        vod = {
          id: vod.id,
          title: vod.title,
          uri: STITCH_ENDPOINT + "?payload=" + encodedPayload,
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
    return [{ language: "Swedish", name: "Swedish" }];
  }
}

const refAssetManager = new RefAssetManager();
const refChannelManager = new RefChannelManager();

const engineOptions: ChannelEngineOpts = {
  heartbeat: "/",
  averageSegmentDuration: 2000,
  channelManager: refChannelManager,
  /* #TS+DEMUX SLATE VOD */
  defaultSlateUri: "https://ovpuspvod.a2d-stage.tv/trailers/bumpers/tv4_spring/output.ism/.m3u8",
  /* #CMAF+DEMUX SLATE VOD */
  //defaultSlateUri: "https://ovpuspvod.a2d-stage.tv/trailers/bumpers/tv4_spring/output.ism/.m3u8",
  slateRepetitions: 10,
  redisUrl: process.env.REDIS_URL,
  useDemuxedAudio: true,
  alwaysNewSegments: true,
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.PORT || 8000);
