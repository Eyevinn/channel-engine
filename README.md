The Eyevinn Channel Engine is a microservice that offers the functionality to generate personalized live streams from available VOD content.

![system description](https://github.com/Eyevinn/channel-engine/blob/master/docs/system-description.png)

A live demonstration of the Channel Engine is available at https://tv.eyevinn.technology/

## Running

To install and run an instance of the Eyevinn Channel Engine we have a Docker image available that can be used. The Channel Engine requests from an Asset Manager API what content to play next. This API is not included in this package and needs to be provided seperately. The Asset Manager API needs to provide the Channel Engine with an endpoint `/nextVod/PLAYLIST` that returns an JSON object in the following format:

```
{
  "id": ASSETID,
  "uri": URI-TO-VOD-HLS,
  "title": TITLE
}
```

This will be the next content to be stitched into the live stream by the engine. To start the Channel Engine run the Docker container and specify with an environment variable the address to the Asset Manager API.

```
$ docker run -e ASSETMGR_URI=https://assetmgr.example.com -p 8000:8000 eyevinntechnology/channelengine:v1.0.2
```

The point an HLS video player to playback the URL `http://localhost:8000/live/master.m3u8`

## Node Module

```
$ npm install --save eyevinn-channel-engine
```

To use the Channel Engine in your NodeJS code you initiate the engine like this, and where you also
have the possibility to provide a custom asset manager that you have built:

```
  const ChannelEngine = require('eyevinn-channel-engine');
  const MyAssetManager = require('./my_asset_manager.js');

  /**
   * Implements the interface:
   *
   * getNextVod(sessionId, category) -> { id, title, uri }
   * getNextVodById(sessionId, id) -> { id, title, uri }
   *
   * Example in ./assetmanagers/default.js
   */
  const assetManager = new MyAssetManager();
  const engine = new ChannelEngine(assetManager);
  engine.listen(process.env.PORT || 8000);
```
