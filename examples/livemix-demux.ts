/*
 * Reference implementation of Channel Engine library
 */

import { ChannelEngine, ChannelEngineOpts, 
    IAssetManager, IChannelManager, IStreamSwitchManager,
    VodRequest, VodResponse, Channel, ChannelProfile,
    Schedule, AudioTracks
  } from "../index";
  const { v4: uuidv4 } = require('uuid');
  
  const DEMUX_CONTENT = {
    ts: {
      slate: "https://trailer-admin-cdn.a2d.tv/virtualchannels/bumper/demux/demux.m3u8",
      vod: "https://playertest.longtailvideo.com/adaptive/elephants_dream_v4/index.m3u8",
      trailer: "https://trailer-admin-cdn.a2d.tv/virtualchannels/trailers/demux002/demux.m3u8",
      bumper: "https://trailer-admin-cdn.a2d.tv/virtualchannels/bumper/demux/demux.m3u8",
      live: "http://localhost:5000/channels/3/master.m3u8"
    },
    cmaf: {
      slate: "https://vod.streaming.a2d.tv/trailers/fillers/6409d46c07b49f0029c1b170/output_v2.ism/.m3u8",
      vod: "https://vod.streaming.a2d.tv/13ec7661-66d7-44d6-b818-7743fe916a87/b747af60-ef3f-11ed-bd7e-9125837ccca3_20343615.ism/.m3u8", // 66 min
      trailer: "https://vod.streaming.a2d.tv/trailers/650c397b298d58002a812ca0/output_v2.ism/.m3u8",
      bumper: "https://vod.streaming.a2d.tv/trailers/bumpers/tv4_summer/start/output_v2.ism/.m3u8",
      live: "https://vc-engine-alb.a2d.tv/channels/a7b2c62f-99b7-4fd9-bde5-56201c59b0a2/master.m3u8"//"https://vc-engine-alb.a2d-dev.tv/channels/1d1847f1-2de7-4f87-b06c-c971107d0ca3/master.m3u8"
    }
  };
  
  const HLS_CONTENT = DEMUX_CONTENT.cmaf;

  const STITCH_ENDPOINT = process.env.STITCH_ENDPOINT || "http://lambda.eyevinn.technology/stitch/master.m3u8";
  
  class RefAssetManager implements IAssetManager {
    private assets;
    private pos;
    constructor(opts?) {
        this.assets = {
          '1': [
            { id: 1, title: "Tears of Steel", uri: HLS_CONTENT.vod },
          ]
        };
        this.pos = {
          '1': 0
        };
    }
  
    /* @param {Object} vodRequest
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
          let vod = this.assets[channelId][this.pos[channelId]++];
          if (this.pos[channelId] > this.assets[channelId].length - 1) {
            this.pos[channelId] = 0;
          }
          const payload = {
            uri: vod.uri,
            breaks: [
              {
                pos: 0,
                duration: 15 * 1000,
                url: "https://playertest.longtailvideo.com/adaptive/elephants_dream_v4/index.m3u8"
                
              }
            ]
          };
          const buff = Buffer.from(JSON.stringify(payload));
          const encodedPayload = buff.toString("base64");
          const vodResponse = {
            id: vod.id,
            title: vod.title,
            uri: vod.uri
          };
          resolve(vodResponse);
        } else {
          reject("Invalid channelId provided");
        }
      });
    }
  
    handleError(err, vodResponse) {
      console.error(err.message);
    }
  }
  
  class RefChannelManager implements IChannelManager {
    private channels;
    constructor(opts?) {
      this.channels = [];
      if (process.env.TEST_CHANNELS) {
        const testChannelsCount = parseInt(process.env.TEST_CHANNELS, 10);
        for (let i = 0; i < testChannelsCount; i++) {
          this.channels.push({ id: `${i + 1}`, profile: this._getProfile(), audioTracks: this._getAudioTracks() });
        }
      } else {
        this.channels = [{ id: "1", profile: this._getProfile(), audioTracks: this._getAudioTracks() }];
      }
    }
  
    getChannels(): Channel[] {
      return this.channels;
    }
  
    _getProfile(): ChannelProfile[] {
      return [
        { bw: 8242000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [1024, 458] },
        { bw: 1274000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [640, 286] },
        { bw: 742000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [480, 214] },
      ]
    }
  
    _getAudioTracks(): AudioTracks[] {
      return [
        { language: "en", name: "English", default: true },
      ];
    }
  }
  const StreamType = Object.freeze({
    LIVE: 1,
    VOD: 2,
  });
  
  class StreamSwitchManager implements IStreamSwitchManager {
    private schedule;
    constructor() {
      this.schedule = {};
      if (process.env.TEST_CHANNELS) {
        const testChannelsCount = parseInt(process.env.TEST_CHANNELS, 10);
        for (let i = 0; i < testChannelsCount; i++) {
          const channelId = `${i + 1}`;
          this.schedule[channelId] = [];
        }
      } else {
        this.schedule = {
          '1': []
        };
      }
    }
  
    generateID(): string {
      return uuidv4();
    }
  
    getPrerollUri(channelId): Promise<string> {
      const defaultPrerollSlateUri = HLS_CONTENT.slate
      return new Promise((resolve, reject) => { resolve(defaultPrerollSlateUri); });
    }
  
    getSchedule(channelId): Promise<Schedule[]> {
      return new Promise((resolve, reject) => {
        if (this.schedule[channelId]) {
          const tsNow = Date.now();
          const streamDuration = 60 * 1000;
          const startOffset = tsNow + streamDuration;
          const endTime = startOffset + streamDuration;
          // Break in with live and scheduled VOD content after 60 seconds of VOD2Live the first time Channel Engine starts
          // Required: "assetId", "start_time", "end_time", "uri", "duration"
          // "duration" is only required for StreamType.VOD
          this.schedule[channelId] = this.schedule[channelId].filter((obj) => obj.end_time >= tsNow);
          if (this.schedule[channelId].length === 0) {
            this.schedule[channelId].push({
              eventId: this.generateID(),
              assetId: this.generateID(),
              title: "Live stream test",
              type: StreamType.LIVE,
              start_time: startOffset,
              end_time: endTime,
              uri: HLS_CONTENT.live,
            }/*,
            {
              eventId: this.generateID(),
              assetId: this.generateID(),
              title: "Scheduled VOD test",
              type: StreamType.VOD,
              start_time: (endTime + 100*1000),
              end_time: (endTime + 100*1000) + streamDuration,
              uri: "https://maitv-vod.lab.eyevinn.technology/COME_TO_DADDY_Trailer_2020.mp4/master.m3u8",
              duration: streamDuration,
            }*/);
          }
          resolve(this.schedule[channelId]);
        } else {
          reject("Invalid channelId provided");
        }
      });
    }
  }
  
  const refAssetManager = new RefAssetManager();
  const refChannelManager = new RefChannelManager();
  const refStreamSwitchManager = new StreamSwitchManager();
  
  const engineOptions: ChannelEngineOpts = {
    heartbeat: "/",
    averageSegmentDuration: 2000,
    channelManager: refChannelManager,
    streamSwitchManager: refStreamSwitchManager,
    defaultSlateUri: HLS_CONTENT.slate,
    slateRepetitions: 10, 
    redisUrl: process.env.REDIS_URL,
    useDemuxedAudio: true,
  };
  
  const engine = new ChannelEngine(refAssetManager, engineOptions);
  engine.start();
  engine.listen(process.env.PORT || 8000);
  