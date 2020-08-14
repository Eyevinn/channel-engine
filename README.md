The Eyevinn Channel Engine is an NPM library that provides the functionality to generate "fake" live HLS stream by stitching HLS VOD's together. The library is provided as open source and this repository includes a basic reference implementation as a guide on how the library can be used.

## Usage

To use this library in your NodeJS project download and install the library in your project by running the following in your project folder.

```
$ npm install --save eyevinn-channel-engine
```

And to run the basic reference implementation included in this repository you run:

```
$ npm start
```

Then point your HLS video player to `http://localhost:8000/live/master.m3u8?channel=1` to start playing the linear live stream.

## API

Initiate and start the engine as below.

```
const ChannelEngine = require('eyevinn-channel-engine');

const engine = new ChannelEngine(myAssetManager, { channelManager: myChannelManager });
engine.start();
engine.listen(process.env.port || 8000);
```

where `myAssetManager` and `myChannelManager` are classes implementing the interfaces below.

```
class MyAssetManager {
  getNextVod({ sessionId, category, playlistId }) -> { id, title, uri, offset }
}

class MyChannelManager {
  getChannels() -> [ { id, name, profile? } ]
}
```

Find a simplistic reference implementation for guidance in `./server.js`.

### Options

Available options when constructing the Channel Engine object are:

- `defaultSlateUri`: URI to an HLS VOD that can be inserted when a VOD for some reason cannot be loaded.
- `slateRepetitions`: Number of times the slate should be repeated.
- `redisUrl`: A Redis DB URL for storing states that can be shared between nodes.
- `heartbeat`: Path for heartbeat requests
- `channelManager`: A reference to a channel manager object.

## Commercial Alternative

Eyevinn offers a commercial alternative based on this module called `Consuo`. `Consuo` is a software component (Docker container) that can be plugged into your online video platform to provide you with unlimited thematic, regional or personal linear TV channels based on an existing VOD library.

Contact sales@eyevinn.se if you want to know more.

## About Eyevinn Technology

Eyevinn Technology is an independent consultant firm specialized in video and streaming. Independent in a way that we are not commercially tied to any platform or technology vendor.

At Eyevinn, every software developer consultant has a dedicated budget reserved for open source development and contribution to the open source community. This give us room for innovation, team building and personal competence development. And also gives us as a company a way to contribute back to the open source community. 

Want to know more about Eyevinn and how it is to work here. Contact us at work@eyevinn.se!
