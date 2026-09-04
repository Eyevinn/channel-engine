/*
 * Reference implementation showing a channel configured for
 * HLS-interstitial (SGAI) ad breaks.
 *
 * When `adBreak.enabled` is true and an `adServerUri` is configured, the
 * engine annotates the served manifest with an EXT-X-DATERANGE HLS
 * interstitial (class `com.apple.hls.interstitial`) at the VOD boundary and
 * resolves the ad asset from the ad-serving endpoint. If the endpoint cannot
 * be reached in time (2s), the break falls back to the configured slate.
 * See `docs/reference.md` for the full field reference.
 */

import { ChannelEngine, ChannelEngineOpts,
  IAssetManager, IChannelManager,
  VodRequest, VodResponse, Channel, ChannelProfile
} from "../index";

class RefAssetManager implements IAssetManager {
  private assets;
  private pos;
  constructor(opts?) {
    this.assets = {
      "1": [
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
      ],
    };
    this.pos = { "1": 0 };
  }

  getNextVod(vodRequest: VodRequest): Promise<VodResponse> {
    return new Promise((resolve, reject) => {
      const channelId = vodRequest.playlistId;
      if (this.assets[channelId]) {
        const vod = this.assets[channelId][this.pos[channelId]++];
        if (this.pos[channelId] > this.assets[channelId].length - 1) {
          this.pos[channelId] = 0;
        }
        resolve({ id: vod.id, title: vod.title, uri: vod.uri });
      } else {
        reject("Invalid channelId provided");
      }
    });
  }
}

class RefChannelManager implements IChannelManager {
  private channels: Channel[];

  constructor() {
    this.channels = [
      {
        id: "1",
        profile: this._getProfile(),
        // Per-channel ad-break configuration. Overrides the engine-level
        // `adBreak` default when present.
        adBreak: {
          // Master enable flag. When false (the default) no interstitial
          // metadata is emitted and the manifest is unchanged.
          enabled: true,
          // Ad-serving endpoint queried to fill the break. Must be an
          // absolute http(s) URL when enabled. It should answer with JSON
          // shaped as { assetUri } | { assetList } | { assets: [...] }.
          adServerUri: "https://ad-endpoint.example.com/vast",
          // Slate shown while the break is being resolved, and used as the
          // fallback if the ad-serving endpoint times out or errors.
          slate: {
            uri: "https://maitv-vod.lab.eyevinn.technology/slate-consuo.mp4/master.m3u8",
            repetitions: 10,
            duration: 4000,
          },
        },
      },
    ];
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
  // Engine-level default applied to channels that do not carry their own
  // `adBreak`. Left disabled here so only channel "1" opens ad breaks.
  adBreak: { enabled: false },
};

const engine = new ChannelEngine(refAssetManager, engineOptions);
engine.start();
engine.listen(process.env.PORT || 8000);
