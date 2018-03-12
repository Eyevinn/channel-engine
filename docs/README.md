The Eyevinn Channel Engine is a "video server" for OTT-only TV channels.

## Background
We asked ourselves the question: If we would startup a TV channel for distribution only over the Internet (OTT) today, how would a technical solution for this look like?

A TV channel is an editorial based packaging of content designed for the laid-back viewer. Though the on-demand watching is becoming a big portion of TV consumption today there are still room for a service directed to a viewer that doesn’t know exactly what to watch, which is why we believe consumption of TV channels still has a place in this landscape. But instead of having a number of fixed and pre-programmed TV channels each viewer will have their own personalized TV channel. However, that doesn’t mean that we would have one encoder per viewer and instead we found a more efficient and scalable way of achieving this.

The general concept that we used is that we take a content repository with already encoded video on demand (VOD) packages and dynamically “stitch” these VOD packages together into a “live” stream. For the player and viewer it looks and feels like a “live” TV channel but it is actually created on-the-fly by concatenating the already prepared VOD packages.

The component that dynamically stitch these VOD packages together is what we call the Eyevinn Channel Engine.

![system description](system-description.png)

## Demo

A live demonstration of this concept is available at https://tv.eyevinn.technology

![screenshot](screenshot.png)

## References

* [Server-less OTT-Only Playout](https://medium.com/@eyevinntechnology/server-less-ott-only-playout-bc5a7f2e6d04)
* [Enabling Video Ads in our Server-less OTT-only TV Channel Playout](https://medium.com/@eyevinntechnology/enabling-video-ads-in-our-server-less-ott-only-tv-channel-playout-81a5e0458f17)