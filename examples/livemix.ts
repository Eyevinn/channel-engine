/*
 * Reference implementation of Channel Engine library
 */

import { ChannelEngine, ChannelEngineOpts, 
  IAssetManager, IChannelManager, IStreamSwitchManager,
  VodRequest, VodResponse, Channel, ChannelProfile,
  Schedule, AudioTracks
} from "../index";
const { v4: uuidv4 } = require('uuid');

const STITCH_ENDPOINT = process.env.STITCH_ENDPOINT || "http://lambda.eyevinn.technology/stitch/master.m3u8";
class RefAssetManager implements IAssetManager {
  private assets;
  private pos;
  constructor(opts?) {
      this.assets = {
        '1': [
          { id: 1, title: "Tears of Steel", uri: "https://playertest.longtailvideo.com/adaptive/elephants_dream_v4/index.m3u8" },
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
    const defaultPrerollSlateUri = "http://localhost:8002/playlist.m3u8"
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
            uri: "http://localhost:8001/channels/1/master.m3u8",
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
  defaultSlateUri: "https://maitv-vod.lab.eyevinn.technology/slate-consuo.mp4/master.m3u8",
  slateRepetitions: 10, 
  redisUrl: process.env.REDIS_URL,
  useDemuxedAudio: true,
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.PORT || 8000);
