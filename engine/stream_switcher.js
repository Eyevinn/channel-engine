const debug = require("debug")("engine-stream-switcher");
const crypto = require("crypto");
const fetch = require("node-fetch");
const { AbortController } = require("abort-controller");
const { SessionState } = require("./session_state");
const { timer, findNearestValue, isValidUrl, fetchWithRetry } = require("./util");
const m3u8 = require("@eyevinn/m3u8");

const SwitcherState = Object.freeze({
  V2L_TO_LIVE: 1,
  V2L_TO_VOD: 2,
  LIVE_TO_V2L: 3,
  LIVE_TO_LIVE: 4,
  LIVE_TO_VOD: 5,
});
const StreamType = Object.freeze({
  LIVE: 1,
  VOD: 2,
});

const FAIL_TIMEOUT = 3000;
const MAX_FAILS = 3;

class StreamSwitcher {
  constructor(config) {
    this.sessionId = crypto.randomBytes(20).toString("hex");
    this.useDemuxedAudio = false;
    this.cloudWatchLogging = false;
    this.streamTypeLive = false;
    this.streamSwitchManager = null;
    this.eventId = null;
    this.working = false;
    this.timeDiff = null;
    this.abortTimeStamp = null;
    if (config) {
      if (config.sessionId) {
        this.sessionId = config.sessionId;
      }
      if (config.useDemuxedAudio) {
        this.useDemuxedAudio = true;
      }
      if (config.cloudWatchMetrics) {
        this.cloudWatchLogging = true;
      }
      if (config.streamSwitchManager) {
        this.streamSwitchManager = config.streamSwitchManager;
      }
    }
    this.prerollsCache = {};
  }

  getEventId() {
    return this.eventId;
  }

  async abortLiveFeed(session, sessionLive, message) {
    if (this.streamTypeLive) {
      let status = null;
      debug(`[${this.sessionId}]: Abort Live Stream! Reason: ${message}`);
      try {
        this.abortTimeStamp = Date.now();
        status = await this._initSwitching(SwitcherState.LIVE_TO_V2L, session, sessionLive, null);
        return status;
      } catch (err) {
        debug(`Failed to force a switch off live feed: ${err}`);
        throw new Error(err);
      }
    }
  }

  /**
   *
   * @param {Session} session The VOD2Live Session object.
   * @param {SessionLive} sessionLive The Live Session object.
   * @returns A bool, true if streamSwitchManager contains current Live event to be played else false.
   */
  async streamSwitcher(session, sessionLive) {
    let status = null;
    if (!this.streamSwitchManager) {
      debug(`[${this.sessionId}]: No streamSwitchManager available`);
      return false;
    }
    if (this.working) {
      debug(`[${this.sessionId}]: streamSwitcher is currently busy`);
      return null;
    }

    try {
      // Handle Complete Storage Reset
      let sessionState = await session.getSessionState();
      if (sessionState === SessionState.VOD_INIT || !sessionState) {
        this.working = true;
        sessionLive.waitForPlayhead = false;
        sessionLive.allowedToSet = false; // only have effect on leader
        await sessionLive.resetSession();
        await sessionLive.resetLiveStoreAsync(0);
        this.working = false;
        this.abortTimeStamp = Date.now() + 30 * 1000; // 30 second V2L->LIVE timeout
        if (this.streamTypeLive) {
          debug(`[${this.sessionId}]: [ Ending LIVE Abruptly, Going to -> V2L ]`);
          this.streamTypeLive = false;
        }
        debug(`[${this.sessionId}]: StreamSwitcher reacting to Full Store Reset`);
        return false; // Go to V2L feed
      }

      // Filter out schedule objects from the past
      const tsNow = Date.now();
      const strmSchedule = await this.streamSwitchManager.getSchedule(this.sessionId);
      const schedule = strmSchedule.filter((obj) => obj.end_time > tsNow);

      // Load Preroll, if any, once per channel every (maxAge) seconds
      if (!this.prerollsCache[this.sessionId] || this.prerollsCache[this.sessionId].maxAge < tsNow) {
        if (this.streamSwitchManager.getPrerollUri) {
          const prerollUri = this.streamSwitchManager.getPrerollUri(this.sessionId);
          if (isValidUrl(prerollUri)) {
            try {
              const segments = await this._loadPreroll(prerollUri);
              const prerollItem = {
                segments: segments,
                maxAge: tsNow + 30 * 60 * 1000,
              };
              this.prerollsCache[this.sessionId] = prerollItem;
            } catch (err) {
              debug(`[${this.sessionId}]: Failed loading preroll vod for channel=${this.sessionId}`);
              console.error(err);
            }
          } else {
            debug(`[${this.sessionId}]: Preroll uri:'${prerollUri}' is not a valid URL. Using No preroll.`);
          }
        }
      }

      if (schedule.length === 0 && this.streamTypeLive) {
        status = await this._initSwitching(SwitcherState.LIVE_TO_V2L, session, sessionLive, null);
        return status;
      }
      if (schedule.length === 0) {
        this.eventId = null;
        return false;
      }
      const scheduleObj = schedule[0];
      this.timeDiff = scheduleObj;
      if (tsNow < scheduleObj.start_time) {
        if (this.streamTypeLive) {
          status = await this._initSwitching(SwitcherState.LIVE_TO_V2L, session, sessionLive, null);
          return status;
        }
        this.eventId = null;
        return false;
      }

      let tries = 0;
      let validURI = false;
      while (!validURI && tries < MAX_FAILS) {
        debug(`[${this.sessionId}]: Switcher is validating Master URI... (tries left=${MAX_FAILS - tries})`);
        validURI = await this._validURI(scheduleObj.uri);
        tries++;
        if (!validURI) {
          const delayMs = tries * 500;
          debug(`[${this.sessionId}]: Going to try validating Master URI again in ${delayMs}ms`);
          await timer(delayMs);
        }
      }
      if (!validURI) {
        debug(`[${this.sessionId}]: Unreachable URI: [${scheduleObj.uri}]`);
        if (this.streamTypeLive) {
          await this.abortLiveFeed(session, sessionLive, "Switching back to VOD2Live due to unreachable URI");
        }
        return false;
      }
      debug(`[${this.sessionId}]: ....Master URI -> VALID`);

      if (this.abortTimeStamp && tsNow - this.abortTimeStamp <= 10000) {
        // If we have a valid URI and no more than 10 seconds have passed since switching from Live->V2L.
        // Stay on V2L to give live sessionLive some time to prepare before switching back to live.
        debug(`[${this.sessionId}]: Waiting [${10000 - (tsNow - this.abortTimeStamp)}ms] before switching back to Live due to unreachable URI`);
        return false;
      }
      this.abortTimeStamp = null;

      if (this.streamTypeLive) {
        if (tsNow >= scheduleObj.start_time && this.eventId !== scheduleObj.eventId) {
          if (scheduleObj.type === StreamType.LIVE) {
            status = await this._initSwitching(SwitcherState.LIVE_TO_LIVE, session, sessionLive, scheduleObj);
            return status;
          }
          status = await this._initSwitching(SwitcherState.LIVE_TO_VOD, session, sessionLive, scheduleObj);
          return status;
        }
      }
      if (tsNow >= scheduleObj.start_time && tsNow < scheduleObj.end_time && scheduleObj.end_time - tsNow > 10000) {
        if (scheduleObj.type === StreamType.LIVE) {
          if (!this.streamTypeLive) {
            status = await this._initSwitching(SwitcherState.V2L_TO_LIVE, session, sessionLive, scheduleObj);
            return status;
          }
          return true;
        }
        if (!this.streamTypeLive) {
          if (!scheduleObj.duration) {
            debug(`[${this.sessionId}]: Cannot switch VOD no duration specified for schedule item: [${scheduleObj.assetId}]`);
            return false;
          }
          if (this.eventId !== scheduleObj.eventId) {
            status = await this._initSwitching(SwitcherState.V2L_TO_VOD, session, sessionLive, scheduleObj);
            return status;
          }
          return false;
        }
      }
    } catch (err) {
      debug(`[${this.sessionId}]: Unexpected failure in Stream Switcher...`);
      console.error(err);
      throw new Error(err);
    }
  }

  async _initSwitching(state, session, sessionLive, scheduleObj) {
    this.working = true;
    const RESET_DELAY = 5000;
    let liveCounts = 0;
    let liveSegments = null;
    let currVodCounts = 0;
    let currLiveCounts = 0;
    let currVodSegments = null;
    let eventSegments = null;
    let liveUri = null;

    switch (state) {
      case SwitcherState.V2L_TO_LIVE:
        try {
          debug(`[${this.sessionId}]: [ INIT Switching from V2L->LIVE ]`);
          this.eventId = scheduleObj.eventId;
          currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
          currVodSegments = await session.getCurrentMediaSequenceSegments();

          // Insert preroll if available for current channel
          if (this.prerollsCache[this.sessionId]) {
            const prerollSegments = this.prerollsCache[this.sessionId].segments;
            currVodSegments = this._mergeSegments(prerollSegments, currVodSegments, false);
          }
          // In risk that the SL-playhead might have updated some data after
          // we reset last time... we should Reset SessionLive before sending new data.
          await sessionLive.resetLiveStoreAsync(0);
          await sessionLive.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
          await sessionLive.setCurrentMediaSequenceSegments(currVodSegments);
          liveUri = await sessionLive.setLiveUri(scheduleObj.uri);

          if (!liveUri) {
            debug(`[${this.sessionId}]: [ ERROR Switching from V2L->LIVE ]`);
            this.working = false;
            this.eventId = null;
            return false;
          }

          this.working = false;
          this.streamTypeLive = true;
          debug(`[${this.sessionId}]: [ Switched from V2L->LIVE ]`);
          return true;
        } catch (err) {
          this.streamTypeLive = false;
          this.working = false;
          this.eventId = null;
          debug(`[${this.sessionId}]: [ ERROR Switching from V2L->LIVE ]`);
          console.error(err);
          throw new Error(err);
        }

      case SwitcherState.V2L_TO_VOD:
        try {
          debug(`[${this.sessionId}]: [ INIT Switching from V2L->VOD ]`);
          this.eventId = scheduleObj.eventId;
          currVodCounts = await session.getCurrentMediaAndDiscSequenceCount();
          eventSegments = await session.getTruncatedVodSegments(scheduleObj.uri, scheduleObj.duration / 1000);

          if (!eventSegments) {
            debug(`[${this.sessionId}]: [ ERROR Switching from V2L->VOD ]`);
            this.working = false;
            this.eventId = null;
            return false;
          }

          // Insert preroll if available for current channel
          if (this.prerollsCache[this.sessionId]) {
            const prerollSegments = this.prerollsCache[this.sessionId].segments;
            eventSegments = this._mergeSegments(prerollSegments, eventSegments, true);
          }

          await session.setCurrentMediaAndDiscSequenceCount(currVodCounts.mediaSeq, currVodCounts.discSeq);
          await session.setCurrentMediaSequenceSegments(eventSegments, 0, true);

          this.working = false;
          debug(`[${this.sessionId}]: [ Switched from V2L->VOD ]`);
          return false;
        } catch (err) {
          this.streamTypeLive = false;
          this.working = false;
          this.eventId = null;
          debug(`[${this.sessionId}]: [ ERROR Switching from V2L->VOD ]`);
          throw new Error(err);
        }
      case SwitcherState.LIVE_TO_V2L:
        try {
          debug(`[${this.sessionId}]: [ INIT Switching from LIVE->V2L ]`);
          this.eventId = null;
          liveSegments = await sessionLive.getCurrentMediaSequenceSegments();
          liveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();

          await sessionLive.resetSession();
          sessionLive.resetLiveStoreAsync(RESET_DELAY); // In parallel

          if (scheduleObj && !scheduleObj.duration) {
            debug(`[${this.sessionId}]: Cannot switch VOD. No duration specified for schedule item: [${scheduleObj.assetId}]`);
          }

          if (this._isEmpty(liveSegments.currMseqSegs)) {
            this.working = false;
            this.streamTypeLive = false;
            debug(`[${this.sessionId}]: [ Switched from LIVE->V2L ]`);
            return false;
          }

          // Insert preroll, if available, for current channel
          if (this.prerollsCache[this.sessionId]) {
            const prerollSegments = this.prerollsCache[this.sessionId].segments;
            liveSegments.currMseqSegs = this._mergeSegments(prerollSegments, liveSegments.currMseqSegs, false);
            liveSegments.segCount += prerollSegments.length;
          }

          await session.setCurrentMediaAndDiscSequenceCount(liveCounts.mediaSeq, liveCounts.discSeq);
          await session.setCurrentMediaSequenceSegments(liveSegments.currMseqSegs, liveSegments.segCount);

          this.working = false;
          this.streamTypeLive = false;
          debug(`[${this.sessionId}]: [ Switched from LIVE->V2L ]`);
          return false;
        } catch (err) {
          this.streamTypeLive = false;
          this.working = false;
          this.eventId = null;
          debug(`[${this.sessionId}]: [ ERROR Switching from LIVE->V2L ]`);
          throw new Error(err);
        }
      case SwitcherState.LIVE_TO_VOD:
        try {
          debug(`[${this.sessionId}]: INIT Switching from LIVE->VOD`);
          // TODO: Not yet fully tested/supported
          this.eventId = scheduleObj.eventId;
          liveSegments = await sessionLive.getCurrentMediaSequenceSegments();
          liveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();
          await sessionLive.resetSession();
          sessionLive.resetLiveStoreAsync(RESET_DELAY); // In parallel

          eventSegments = await session.getTruncatedVodSegments(scheduleObj.uri, scheduleObj.duration / 1000);
          if (!eventSegments) {
            debug(`[${this.sessionId}]: [ ERROR Switching from LIVE->VOD ]`);
            this.streamTypeLive = false;
            this.working = false;
            this.eventId = null;
            return false;
          }

          await session.setCurrentMediaAndDiscSequenceCount(liveCounts.mediaSeq - 1, liveCounts.discSeq - 1);
          await session.setCurrentMediaSequenceSegments(liveSegments.currMseqSegs, liveSegments.segCount);

          // Insert preroll, if available, for current channel
          if (this.prerollsCache[this.sessionId]) {
            const prerollSegments = this.prerollsCache[this.sessionId].segments;
            eventSegments = this._mergeSegments(prerollSegments, eventSegments, true);
          }

          await session.setCurrentMediaSequenceSegments(eventSegments, 0, true);

          this.working = false;
          this.streamTypeLive = false;
          debug(`[${this.sessionId}]: Switched from LIVE->VOD`);
          return false;
        } catch (err) {
          this.streamTypeLive = false;
          this.working = false;
          this.eventId = null;
          debug(`[${this.sessionId}]: [ ERROR Switching from LIVE->VOD ]`);
          throw new Error(err);
        }
      case SwitcherState.LIVE_TO_LIVE:
        try {
          debug(`[${this.sessionId}]: INIT Switching from LIVE->LIVE`);
          // TODO: Not yet fully tested/supported
          this.eventId = scheduleObj.eventId;
          eventSegments = await sessionLive.getCurrentMediaSequenceSegments();
          currLiveCounts = await sessionLive.getCurrentMediaAndDiscSequenceCount();

          await sessionLive.resetSession();
          await sessionLive.resetLiveStoreAsync(0);

          // Insert preroll, if available, for current channel
          if (this.prerollsCache[this.sessionId]) {
            const prerollSegments = this.prerollsCache[this.sessionId].segments;
            eventSegments = this._mergeSegments(prerollSegments, eventSegments, false);
          }

          await sessionLive.setCurrentMediaAndDiscSequenceCount(currLiveCounts.mediaSeq, currLiveCounts.discSeq);
          await sessionLive.setCurrentMediaSequenceSegments(eventSegments.currMseqSegs);
          liveUri = await sessionLive.setLiveUri(scheduleObj.uri);

          if (!liveUri) {
            debug(`[${this.sessionId}]: [ ERROR Switching from LIVE->LIVE ]`);
            this.streamTypeLive = false;
            this.working = false;
            this.eventId = null;
            return false;
          }

          this.working = false;
          debug(`[${this.sessionId}]: Switched from LIVE->LIVE`);
          return true;
        } catch (err) {
          this.streamTypeLive = false;
          this.working = false;
          this.eventId = null;
          debug(`[${this.sessionId}]: [ ERROR Switching from LIVE->LIVE ]`);
          throw new Error(err);
        }
      default:
        debug(`[${this.sessionId}]: SwitcherState [${state}] not implemented`);
        this.streamTypeLive = false;
        this.working = false;
        this.eventId = null;
        return false;
    }
  }

  _isEmpty(obj) {
    if (!obj) {
      return true;
    }
    for (var key in obj) {
      if (obj.hasOwnProperty(key)) {
        return false;
      }
    }
    return true;
  }

  async _validURI(uri) {
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      debug(`[${this.sessionId}]: Request Timeout @ ${uri}`);
      controller.abort();
    }, FAIL_TIMEOUT);
    try {
      const online = await fetch(uri, { signal: controller.signal });

      if (online.status >= 200 && online.status < 300) {
        return true;
      }
      debug(`[${this.sessionId}]: Failed to validate URI: ${uri}\nERROR! Returned Status Code: ${online.status}`);
      return false;
    } catch (err) {
      debug(`[${this.sessionId}]: Failed to validate URI: ${uri}\nERROR: ${err}`);
      return false;
    } finally {
      clearTimeout(timeout);
    }
  }

  async _loadPreroll(uri) {
    const prerollSegments = {};
    const mediaM3UPlaylists = {};
    const mediaURIs = {};
    try {
      const m3u = await this._fetchParseM3u8(uri);
      debug(`[${this.sessionId}]: ...Fetched a New Preroll Slate Master Manifest from:\n${uri}`);
      // Is the first URI an actual Multivariant manifest
      if (m3u.items.StreamItem.length > 0) {
        // Process Master M3U. Collect Media URIs
        for (let i = 0; i < m3u.items.StreamItem.length; i++) {
          const streamItem = m3u.items.StreamItem[i];
          const bw = streamItem.get("bandwidth");
          const mediaUri = streamItem.get("uri");
          if (mediaUri.match("^http")) {
            mediaURIs[bw] = mediaUri;
          } else {
            mediaURIs[bw] = new URL(mediaUri, uri);
          }
        }

        // Fetch and parse Media URIs
        const bandwidths = Object.keys(mediaURIs);
        const loadMediaPromises = [];
        // Queue up...
        bandwidths.forEach((bw) => loadMediaPromises.push(this._fetchParseM3u8(mediaURIs[bw])));
        // Execute...
        const results = await Promise.allSettled(loadMediaPromises);
        // Process...
        results.forEach((item, idx) => {
          if (item.status === "fulfilled" && item.value) {
            const resultM3U = item.value;
            const bw = bandwidths[idx];
            mediaM3UPlaylists[bw] = resultM3U.items.PlaylistItem;
          }
        });
      } else if (m3u.items.PlaylistItem.length > 0) {
        // Process the Media M3U.
        const arbitraryBw = 1;
        mediaURIs[arbitraryBw] = uri;
        mediaM3UPlaylists[arbitraryBw] = m3u.items.PlaylistItem;
      } else {
        debug(`[${this.sessionId}]: WARNING! M3U has no variants nor playlist segments!`);
      }

      // Turn original m3u playlist items into custom-simple segment items list
      const bandwidths = Object.keys(mediaM3UPlaylists);
      for (let i = 0; i < bandwidths.length; i++) {
        const bw = bandwidths[i];
        if (!prerollSegments[bw]) {
          prerollSegments[bw] = [];
        }
        for (let k = 0; k < mediaM3UPlaylists[bw].length; k++) {
          let seg = {};
          let playlistItem = mediaM3UPlaylists[bw][k];
          let segmentUri;
          let cueData = null;
          let daterangeData = null;
          let attributes = playlistItem["attributes"].attributes;
          if (playlistItem.properties.discontinuity) {
            prerollSegments[bw].push({ discontinuity: true });
          }
          if ("cuein" in attributes) {
            if (!cueData) {
              cueData = {};
            }
            cueData["in"] = true;
          }
          if ("cueout" in attributes) {
            if (!cueData) {
              cueData = {};
            }
            cueData["out"] = true;
            cueData["duration"] = attributes["cueout"];
          }
          if ("cuecont" in attributes) {
            if (!cueData) {
              cueData = {};
            }
            cueData["cont"] = true;
          }
          if ("scteData" in attributes) {
            if (!cueData) {
              cueData = {};
            }
            cueData["scteData"] = attributes["scteData"];
          }
          if ("assetData" in attributes) {
            if (!cueData) {
              cueData = {};
            }
            cueData["assetData"] = attributes["assetData"];
          }
          if ("daterange" in attributes) {
            if (!daterangeData) {
              daterangeData = {};
            }
            let allDaterangeAttributes = Object.keys(attributes["daterange"]);
            allDaterangeAttributes.forEach((attr) => {
              if (attr.match(/DURATION$/)) {
                daterangeData[attr.toLowerCase()] = parseFloat(attributes["daterange"][attr]);
              } else {
                daterangeData[attr.toLowerCase()] = attributes["daterange"][attr];
              }
            });
          }
          if (playlistItem.properties.uri) {
            if (playlistItem.properties.uri.match("^http")) {
              segmentUri = playlistItem.properties.uri;
            } else {
              segmentUri = new URL(playlistItem.properties.uri, mediaURIs[bw]);
            }
            seg["duration"] = playlistItem.properties.duration;
            seg["uri"] = segmentUri;
            seg["cue"] = cueData;
            if (daterangeData) {
              seg["daterange"] = daterangeData;
            }
          }
          prerollSegments[bw].push(seg);
        }
      }
      debug(`[${this.sessionId}]: Loaded all Variants of the Preroll Slate!`);
      return prerollSegments;
    } catch (err) {
      throw new Error(err);
    }
  }

  // Input: hls vod uri. Output: an M3U object.
  async _fetchParseM3u8(uri) {
    const parser = m3u8.createStream();
    try {
      const res = await fetchWithRetry(uri, {}, 5, 1000, 1500, debug);
      res.body.pipe(parser);
      return new Promise((resolve, reject) => {
        parser.on("m3u", (m3u) => {
          resolve(m3u);
          parser.on("error", (exc) => {
            debug(`Parser Error: ${JSON.stringify(exc)}`);
            reject(exc);
          });
        });
      });
    } catch (err) {
      return Promise.reject(`[${this.sessionId}]: Failed to Fetch URI: ${uri}\nERROR, ${err}`);
    }
  }

  _mergeSegments(fromSegments, toSegments, prepend) {
    const OUTPUT_SEGMENTS = {};
    const fromBws = Object.keys(fromSegments);
    const toBws = Object.keys(toSegments);
    toBws.forEach((bw) => {
      const targetBw = findNearestValue(bw, fromBws);
      if (prepend) {
        OUTPUT_SEGMENTS[bw] = fromSegments[targetBw].concat(toSegments[bw]);
        OUTPUT_SEGMENTS[bw].unshift({ discontinuity: true });
      } else {
        const lastSeg = toSegments[bw][toSegments[bw].length - 1];
        if (lastSeg.uri && !lastSeg.discontinuity) {
          toSegments[bw].push({ discontinuity: true });
          OUTPUT_SEGMENTS[bw] = toSegments[bw].concat(fromSegments[targetBw]);
        } else {
          OUTPUT_SEGMENTS[bw] = toSegments[bw].concat(fromSegments[targetBw]);
          OUTPUT_SEGMENTS[bw].push({ discontinuity: true });
        }
      }
    });
    return OUTPUT_SEGMENTS;
  }
}

module.exports = StreamSwitcher;
