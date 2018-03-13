# Eyevinn Channel Engine Reference Documentation

## Environment Variables

Environment Variable | Description
-------------------- | -----------
ASSETMGR_URI | The URI to the Asset Manager API
PORT | The port the Channel Engine is listening to (default 8000)

## API Interfaces

### Asset Manager API Interface

The Channel Engine expects that the Asset Manager API implements the following interface

Resource | Method | Request Payload | Response Payload | Description
-------- | ------ | --------------- | ---------------- | -----------
/nextVod/random | GET | n/a | ASSET JSON | Return next VOD chosen by random to stitch
/nextVod/PLAYLIST[?position=CURRENTPOSITION] | GET | n/a | ASSET JSON | Return next VOD in the playlist PLAYLIST after position CURRENTPOSITION
/vod/ID | GET | n/a | ASSET JSON | Return asset metadata for a specific asset ID

### ASSET JSON

Key | Value | Type | Description
--- | ----- | ---- | -----------
id  | ASSETID | string | The unique ID of the asset
uri | HLSURI | string | URI to HLS master manifest for the VOD
title (optional) | TITLE | The title of the asset
playlistPosition (optional) | POSITION | id | Current position of the VOD in the playlist

