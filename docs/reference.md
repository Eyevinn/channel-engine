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
/nextVod/PLAYLIST[?position=CURRENT] | GET | n/a | ASSET JSON | Return next VOD in the playlist PLAYLIST after position CURRENT (integer)
/vod/ID | GET | n/a | ASSET JSON | Return asset metadata for a specific asset ID

### ASSET JSON

Key | Value | Type | Description
--- | ----- | ---- | -----------
id  | ASSETID | string | The unique ID of the asset
uri | HLSURI | string | URI to HLS master manifest for the VOD
title (optional) | TITLE | string |  The title of the asset
playlistPosition (optional) | POSITION | id | Current position of the VOD in the playlist

## Ad-break configuration (HLS interstitials)

The engine can annotate a channel's manifest with SGAI HLS-interstitial
[EXT-X-DATERANGE](https://datatracker.ietf.org/doc/html/rfc8216) tags
(`CLASS="com.apple.hls.interstitial"`) so that an HLS player performs
client-side ad replacement at the VOD boundary. Ad breaks are configured
through the `adBreak` object, which can be set either as an engine-level
default (on `ChannelEngineOpts`) or per channel (on a `Channel`). A
per-channel `adBreak` overrides the engine-level default. Ad breaks default
to **disabled** when neither is set.

### `adBreak` fields

Field | Type | Default | Description
----- | ---- | ------- | -----------
enabled | boolean | `false` | Master enable flag. When `false`, no interstitial metadata is emitted and the served manifest is byte-identical to the non-ad-break output.
adServerUri | string | (none) | URL of the ad-serving endpoint queried to fill a break. **Required** when `enabled` is `true`, and validated to be an absolute `http`/`https` URL — an enabled break with a missing or invalid `adServerUri` throws at engine construction.
slate (optional) | object | (none) | Slate reference shown while the break is being resolved and used as the fallback asset if the ad-serving endpoint cannot be resolved. Omit to fall back to the channel/engine slate configuration.

### `adBreak.slate` fields

Field | Type | Default | Description
----- | ---- | ------- | -----------
uri | string | (none) | URI to the slate HLS VOD.
repetitions (optional) | number | `10` | Number of times the slate VOD is repeated to fill the break window.
duration (optional) | number | `4000` | Duration of a single slate repetition, in milliseconds. Together with `repetitions` this drives the interstitial `PLANNED-DURATION`.

### Ad replacement flow

When a break is opened on an enabled channel:

1. The engine queries the configured `adServerUri` for an ad asset (with an
   `Accept: application/json` request and a 2-second timeout).
2. The endpoint answers with a JSON body in one of these shapes:

   Response body | Interstitial attribute
   ------------- | ----------------------
   `{ "assetUri": "<uri>" }` | `X-ASSET-URI` (single asset)
   `{ "assetList": "<uri>" }` | `X-ASSET-LIST` (an asset-list endpoint)
   `{ "assets": ["<uri>", ...] }` | `X-ASSET-URI` from the first entry

3. The resolved asset is written into an EXT-X-DATERANGE HLS interstitial on
   the served manifest, and the HLS player fetches and plays the ad in place
   of (client-side ad replacement), leaving the underlying VOD2Live stream
   untouched.
4. If the endpoint times out, returns a non-2xx status, or returns a
   malformed/empty body, the engine logs and falls back to the configured
   slate (or the channel/engine slate) so the break still renders — the
   session tick is never interrupted.

See `examples/adbreak.ts` for a runnable channel wired with an ad break.

