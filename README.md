# Eyevinn Channel Engine
> *Open Source FAST Channel Engine library based on VOD2Live technology*

[![Slack](http://slack.streamingtech.se/badge.svg)](http://slack.streamingtech.se)

Eyevinn Channel Engine is a core component library for creating FAST channels based on VOD2Live technology. Combine this vod2live technology component with your business and scheduling logic to build your very own and unique FAST channel engine. 

Please see the [Documentation](https://vod2live.docs.eyevinn.technology) for introductory tutorials and how to get started building your own FAST channel engine.

![Screenshot of demo site](docs/demosite.png)

A live demo and example is available here: [https://vod2live.eyevinn.technology](https://vod2live.eyevinn.technology)

## Features

- Produce 24/7 HLS live channels based on already transcoded HLS VODs
- Produce a personalized 24/7 HLS live channel unique for each viewer
- Mix VOD2Live channel with a "real" live HLS stream
- Develop adapters to plugin with custom scheduling endpoints
- And much more!

## System Requirements

Supported Node.js Versions

| Version | Supported | 
| ------- | --------- |
| 14.x    | Yes       |
| 16.x    | Yes       |
| 18.x    | No        |

## Supported Source Formats

| HLS Format | Muxed | Demuxed | Mix w. Live | Subtitles | DRM |
| ---------- | ----- | ------- | ----------- | --------- | --- |
| HLS + TS   | Yes   | Yes     | Yes*        | TBD       | No  |
| HLS + CMAF | No    | Yes     | TBD         | TBD       | TBD |

*\* not supported with demuxed sources*

## Usage

Follow [this tutorial](https://vod2live.docs.eyevinn.technology/getting-started.html) to get started building your own FAST channel engine.

## Migration

### Upgrading from 3.4.x to >= 4.0.0

Support for HLS-CMAF and handling audio and video segments of different durations was added to v4
and this means that it is not possible to mix muxed TS with demuxed CMAF.

### Upgrading from 3.3.x to >= 3.4.x

A breaking change was introduced in v3.4.0 when Typescript types were introduced. The library no longer exports a default. This means that you need to change `const ChannelEngine = require('eyevinn-channel-engine')` to `const { ChannelEngine } = require('eyevinn-channel-engine')`;

## Support

Join our [community on Slack](http://slack.streamingtech.se) where you can post any questions regarding any of our open source projects. Eyevinn's consulting business can also offer you:

- Further development of this component
- Customization and integration of this component into your platform
- Support and maintenance agreement

## About Eyevinn Technology

[Eyevinn Technology](https://www.eyevinntechnology.se) is an independent consultant firm specialized in video and streaming. Independent in a way that we are not commercially tied to any platform or technology vendor. As our way to innovate and push the industry forward we develop proof-of-concepts and tools. The things we learn and the code we write we share with the industry in [blogs](https://dev.to/video) and by open sourcing the code we have written.

Want to know more about Eyevinn and how it is to work here. Contact us at work@eyevinn.se!

