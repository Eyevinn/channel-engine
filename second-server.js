"use strict";

const ChannelEngine = require("./index.js");

const STITCH_ENDPOINT = process.env.STITCH_ENDPOINT || "http://lambda.eyevinn.technology/stitch/master.m3u8";
class MyAssetManager {
  constructor(opts) {

  }

  getNextVod(vodRequest) {
    return new Promise((resolve, reject) => {
      const payload = {
        uri: "https://lab.cdn.eyevinn.technology/NO_TIME_TO_DIE_short_Trailer_2021.mp4/manifest.m3u8",
        breaks: [
          {
            pos: 0,
            duration: 24 * 1000,
            url: "https://lab.cdn.eyevinn.technology/stswe-ad-30sec.mp4/manifest.m3u8"
          }
        ]
      };
      const buff = Buffer.from(JSON.stringify(payload));
      const encodedPayload = buff.toString("base64");
      const vod = {
        id: 1,
        title: "VINN",
        uri: STITCH_ENDPOINT + "?payload=" + encodedPayload
      }
      resolve(vod);
    });
  }
}

class MyChannelManager {
  getChannels() {
    return [{ id: "1", profile: this._getProfile() }];
  }

  _getProfile() {
    return [
      { bw: 6134000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [1024, 458] },
      { bw: 2323000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [640, 286] },
      { bw: 1313000, codecs: 'avc1.4d001f,mp4a.40.2', resolution: [480, 214] }
    ];
  }
}

// MAKE IT LISTEN
const run = async () => {
  try {
    const myAssetManager = new MyAssetManager();
    const myChannelManager = new MyChannelManager();

    const engine = new ChannelEngine(myAssetManager, {
      heartbeat: "/",
      defaultSlateUri: "https://maitv-vod.lab.eyevinn.technology/slate-consuo.mp4/master.m3u8",
      averageSegmentDuration: 7200,
      channelManager: myChannelManager,
    });

    const port = process.env.VC_PORT || 8089

    engine.start();
    console.log("...VC listening on port " + port + `\n...Playback on: http://localhost:${port}/live/master.m3u8?channel=1 \n`);
    engine.listen(port);
  } catch (err) {
    fastifyServer.log.error(err);
    process.exit(1);
  }
};

run();
