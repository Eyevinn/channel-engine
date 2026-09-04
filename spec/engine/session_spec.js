const Session = require("../../engine/session.js");
const HLSVod = require("@eyevinn/hls-vodtolive");
const fs = require("fs");

const { SessionStateStore } = require("../../engine/session_state.js");
const { PlayheadStateStore } = require("../../engine/playhead_state.js");

describe("Session", () => {
  let sessionLiveStore = undefined;
  beforeEach(() => {
    sessionLiveStore = {
      sessionStateStore: new SessionStateStore(),
      playheadStateStore: new PlayheadStateStore(),
    };
  });

  it("creates a unique session ID", () => {
    const id1 = new Session("dummy", null, sessionLiveStore).sessionId;
    const id2 = new Session("dummy", null, sessionLiveStore).sessionId;
    expect(id1).not.toEqual(id2);
  });

  describe("ad-break config surface (issue #367)", () => {
    it("defaults ad breaks to disabled when unconfigured", () => {
      const sessionNoConfig = new Session("dummy", null, sessionLiveStore);
      const sessionEmptyConfig = new Session("dummy", {}, sessionLiveStore);
      expect(sessionNoConfig.adBreak).toEqual({ enabled: false });
      expect(sessionEmptyConfig.adBreak).toEqual({ enabled: false });
    });

    it("stores an enabled ad-break config when provided", () => {
      const session = new Session("dummy", {
        adBreak: { enabled: true, adServerUri: "https://ads.example.com/vast" }
      }, sessionLiveStore);
      expect(session.adBreak.enabled).toEqual(true);
      expect(session.adBreak.adServerUri).toEqual("https://ads.example.com/vast");
    });

    it("treats an explicitly disabled ad-break config as disabled", () => {
      const session = new Session("dummy", {
        adBreak: { enabled: false, adServerUri: "https://ads.example.com/vast" }
      }, sessionLiveStore);
      expect(session.adBreak).toEqual({ enabled: false });
    });

    it("carries a slate reference through when present on an enabled config", () => {
      const session = new Session("dummy", {
        adBreak: {
          enabled: true,
          adServerUri: "https://ads.example.com/vast",
          slate: { uri: "https://slate.example.com/slate.mp4", repetitions: 5, duration: 3000 }
        }
      }, sessionLiveStore);
      expect(session.adBreak.slate).toEqual({
        uri: "https://slate.example.com/slate.mp4",
        repetitions: 5,
        duration: 3000
      });
    });
  });

  describe("HLS-interstitial EXT-X-DATERANGE emission (issue #368)", () => {
    // Loads a real content VOD from the vod-lib test vectors, applies the
    // session's interstitial metadata via the same addMetadata pathway the
    // engine uses at VOD creation, and renders the live media playlist so we
    // can assert the produced EXT-X-DATERANGE tag byte-for-byte.
    const VODLIB_TV = "node_modules/@eyevinn/hls-vodtolive/testvectors/hls1/";
    const masterLoader = () => fs.createReadStream(VODLIB_TV + "master.m3u8");
    const mediaLoader = (bandwidth) =>
      fs.createReadStream(VODLIB_TV + bandwidth + ".m3u8");
    const FIXED_TS = Date.UTC(2026, 8, 4, 0, 0, 0); // 2026-09-04T00:00:00.000Z

    async function renderFirstMediaSequence(session) {
      const vod = new HLSVod("http://mock.com/master.m3u8", [], FIXED_TS, 0);
      session._addInterstitialMetadata(vod, FIXED_TS);
      await vod.load(masterLoader, mediaLoader);
      const bw = vod.getBandwidths()[0];
      return vod.getLiveMediaSequences(0, bw, 0, 0);
    }

    it("emits an interstitial DATERANGE at the break start when ad breaks are enabled", async () => {
      const session = new Session("dummy", {
        adBreak: {
          enabled: true,
          adServerUri: "https://ads.example.com/vast",
          slate: { uri: "https://slate.example.com/slate.mp4", repetitions: 2, duration: 4000 }
        }
      }, sessionLiveStore);

      const m3u8 = await renderFirstMediaSequence(session);
      const daterange = m3u8.split("\n").find((l) => l.startsWith("#EXT-X-DATERANGE"));

      expect(daterange).toBeDefined();
      // Standard Apple HLS-interstitial class identifier.
      expect(daterange).toContain('CLASS="com.apple.hls.interstitial"');
      expect(daterange).toContain('START-DATE="2026-09-04T00:00:00.000Z"');
      // Slate reference is preferred as the asset source (2 x 4000ms = 8.000s).
      expect(daterange).toContain('X-ASSET-URI="https://slate.example.com/slate.mp4"');
      expect(daterange).toContain("PLANNED-DURATION=8.000");
      expect(daterange).toMatch(/ID="adbreak-/);
    });

    it("falls back to the ad server URI as the interstitial asset when no slate is configured", async () => {
      const session = new Session("dummy", {
        adBreak: { enabled: true, adServerUri: "https://ads.example.com/vast" }
      }, sessionLiveStore);

      const m3u8 = await renderFirstMediaSequence(session);
      const daterange = m3u8.split("\n").find((l) => l.startsWith("#EXT-X-DATERANGE"));

      expect(daterange).toBeDefined();
      expect(daterange).toContain('X-ASSET-URI="https://ads.example.com/vast"');
      // No slate duration configured -> no PLANNED-DURATION attribute emitted.
      expect(daterange).not.toContain("PLANNED-DURATION");
    });

    it("emits no DATERANGE and a byte-identical playlist when ad breaks are disabled", async () => {
      const enabledSession = new Session("dummy", {
        adBreak: { enabled: true, adServerUri: "https://ads.example.com/vast" }
      }, sessionLiveStore);
      const disabledSession = new Session("dummy", null, sessionLiveStore);

      // Baseline playlist with the feature disabled (default config).
      const disabledM3u8 = await renderFirstMediaSequence(disabledSession);
      // Same VOD/content with the feature enabled produces the interstitial tag.
      const enabledM3u8 = await renderFirstMediaSequence(enabledSession);

      expect(/#EXT-X-DATERANGE/.test(disabledM3u8)).toEqual(false);
      expect(/#EXT-X-DATERANGE/.test(enabledM3u8)).toEqual(true);

      // Proof that disabled output is unchanged: stripping the injected
      // interstitial lines from the enabled playlist yields the disabled one.
      const enabledStripped = enabledM3u8
        .split("\n")
        .filter((l) => !l.startsWith("#EXT-X-DATERANGE") && !l.startsWith("#EXT-X-PROGRAM-DATE-TIME"))
        .join("\n");
      const disabledStripped = disabledM3u8
        .split("\n")
        .filter((l) => !l.startsWith("#EXT-X-PROGRAM-DATE-TIME"))
        .join("\n");
      expect(enabledStripped).toEqual(disabledStripped);
    });

    it("is a no-op for _addInterstitialMetadata when ad breaks are disabled", () => {
      const session = new Session("dummy", null, sessionLiveStore);
      const calls = [];
      const fakeVod = { addMetadata: (k, v) => calls.push([k, v]) };
      session._addInterstitialMetadata(fakeVod, FIXED_TS);
      expect(calls.length).toEqual(0);
    });
  });

  it("sets the event flag when configured with { event: true }", () => {
    const session = new Session("dummy", { event: true }, sessionLiveStore);
    expect(session.event).toEqual(true);
  });

  it("defaults the event flag to false when not configured", () => {
    const sessionNoConfig = new Session("dummy", null, sessionLiveStore);
    const sessionEmptyConfig = new Session("dummy", {}, sessionLiveStore);
    expect(sessionNoConfig.event).toEqual(false);
    expect(sessionEmptyConfig.event).toEqual(false);
  });

  describe("end-of-schedule detection (event mode)", () => {
    // The "no more VODs" signal is the asset manager resolving with a falsy value
    // (null/undefined) or an object that has neither a `uri` nor `type === 'gap'`.
    const noMoreVodValues = [null, undefined, {}];

    it("does not record an ended state when event mode is off (preserves slate/retry)", async () => {
      const session = new Session(
        { getNextVod: async () => null },
        null,
        sessionLiveStore
      );
      expect(session.event).toEqual(false);
      await expectAsync(session._getNextVod()).toBeRejected();
      // No end-of-schedule state is recorded; current (slate/retry) behaviour is preserved.
      expect(session.isEndOfSchedule).toEqual(false);
    });

    noMoreVodValues.forEach((value) => {
      it(`records the ended state when event mode is on and nextVod yields ${JSON.stringify(value)}`, async () => {
        const session = new Session(
          { getNextVod: async () => value },
          { event: true },
          sessionLiveStore
        );
        expect(session.event).toEqual(true);
        expect(session.isEndOfSchedule).toEqual(false);
        await expectAsync(session._getNextVod()).toBeRejectedWith("END_OF_SCHEDULE");
        expect(session.isEndOfSchedule).toEqual(true);
      });
    });

    it("does not record an ended state in event mode when a valid VOD is returned", async () => {
      const session = new Session(
        { getNextVod: async () => ({ id: "1", uri: "https://example.com/vod.m3u8" }) },
        { event: true },
        sessionLiveStore
      );
      const vod = await session._getNextVod();
      expect(vod.uri).toEqual("https://example.com/vod.m3u8");
      expect(session.isEndOfSchedule).toEqual(false);
    });

    it("does not record an ended state in event mode for a gap marker", async () => {
      const session = new Session(
        { getNextVod: async () => ({ type: "gap", desiredDuration: 10 }) },
        { event: true },
        sessionLiveStore
      );
      const vod = await session._getNextVod();
      expect(vod.type).toEqual("gap");
      expect(session.isEndOfSchedule).toEqual(false);
    });
  });

  describe("EXT-X-ENDLIST on served media playlists (event mode, issue #363)", () => {
    // A served media playlist ends its last line with a newline (segment lines are
    // "\n"-terminated by the vod lib), so ENDLIST is appended on its own line.
    const mediaPlaylist =
      "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nseg0.ts\n";

    it("appends #EXT-X-ENDLIST once the schedule has ended in event mode", () => {
      const session = new Session("dummy", { event: true }, sessionLiveStore);
      session.isEndOfSchedule = true;
      const out = session._appendEndlistIfEnded(mediaPlaylist);
      expect(out.endsWith("#EXT-X-ENDLIST\n")).toEqual(true);
    });

    it("does not append #EXT-X-ENDLIST while content is still playing in event mode", () => {
      const session = new Session("dummy", { event: true }, sessionLiveStore);
      expect(session.isEndOfSchedule).toEqual(false);
      const out = session._appendEndlistIfEnded(mediaPlaylist);
      expect(out).toEqual(mediaPlaylist);
      expect(/#EXT-X-ENDLIST/.test(out)).toEqual(false);
    });

    it("never appends #EXT-X-ENDLIST when event mode is off, even at end of schedule", () => {
      const session = new Session("dummy", null, sessionLiveStore);
      session.isEndOfSchedule = true; // even if forced, off-mode never emits the tag
      const out = session._appendEndlistIfEnded(mediaPlaylist);
      expect(/#EXT-X-ENDLIST/.test(out)).toEqual(false);
    });

    it("is idempotent and does not add a second #EXT-X-ENDLIST", () => {
      const session = new Session("dummy", { event: true }, sessionLiveStore);
      session.isEndOfSchedule = true;
      const once = session._appendEndlistIfEnded(mediaPlaylist);
      const twice = session._appendEndlistIfEnded(once);
      expect(twice).toEqual(once);
      expect((twice.match(/#EXT-X-ENDLIST/g) || []).length).toEqual(1);
    });
  });

  describe("post-ENDLIST session behaviour (event mode, issue #364)", () => {
    // A served media playlist ends its last line with a newline, so ENDLIST is
    // appended on its own line (same fixture shape as the #363 specs above).
    const mediaPlaylist =
      "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nseg0.ts\n";

    // (a) flag off = no ENDLIST and normal looping.
    it("keeps looping (no end state, no ENDLIST) when event mode is off", async () => {
      let calls = 0;
      const session = new Session(
        { getNextVod: async () => { calls++; return null; } },
        null,
        sessionLiveStore
      );
      expect(session.event).toEqual(false);
      // Off-mode rejects but never records an end-of-schedule state, so the
      // normal slate/retry looping is preserved and each request re-queries.
      await expectAsync(session._getNextVod()).toBeRejected();
      await expectAsync(session._getNextVod()).toBeRejected();
      expect(session.isEndOfSchedule).toEqual(false);
      expect(calls).toEqual(2);
      // No ENDLIST is ever appended when event mode is off.
      expect(/#EXT-X-ENDLIST/.test(session._appendEndlistIfEnded(mediaPlaylist))).toEqual(false);
    });

    // (b) flag on + schedule ends = single ENDLIST appended and stable playlist
    // on repeat requests.
    it("appends a single ENDLIST and serves a stable playlist on repeat requests once ended", async () => {
      const session = new Session(
        { getNextVod: async () => null },
        { event: true },
        sessionLiveStore
      );
      await expectAsync(session._getNextVod()).toBeRejectedWith("END_OF_SCHEDULE");
      expect(session.isEndOfSchedule).toEqual(true);

      // Repeated manifest requests after end return the same ENDLIST-terminated
      // playlist, with exactly one ENDLIST tag each time.
      const first = session._appendEndlistIfEnded(mediaPlaylist);
      const second = session._appendEndlistIfEnded(mediaPlaylist);
      expect(first.endsWith("#EXT-X-ENDLIST\n")).toEqual(true);
      expect(second).toEqual(first);
      expect((second.match(/#EXT-X-ENDLIST/g) || []).length).toEqual(1);
    });

    // (c) session does not attempt further nextVod() calls after ending.
    it("does not request another VOD from the asset manager after the schedule has ended", async () => {
      let calls = 0;
      const session = new Session(
        { getNextVod: async () => { calls++; return null; } },
        { event: true },
        sessionLiveStore
      );
      // First call reaches the asset manager and records the ended state.
      await expectAsync(session._getNextVod()).toBeRejectedWith("END_OF_SCHEDULE");
      expect(calls).toEqual(1);
      expect(session.isEndOfSchedule).toEqual(true);

      // Subsequent calls short-circuit: they still reject with the end marker but
      // never query the asset manager again (no runaway advance loop).
      await expectAsync(session._getNextVod()).toBeRejectedWith("END_OF_SCHEDULE");
      await expectAsync(session._getNextVod()).toBeRejectedWith("END_OF_SCHEDULE");
      expect(calls).toEqual(1);
      expect(session.isEndOfSchedule).toEqual(true);
    });
  });

  it("for demuxed, returns the appropriate audio increment value when desync is within acceptable limit, case I", async () => {
    const session = new Session("dummy", null, sessionLiveStore);
    const mockFinalAudioIdx = 50; // current Vod has 50 media sequences to serve.
    const mockCurrentVideoPosition = 200.0 * 1000; // Video is 200s deep into its content.
    const mockMseqAudio = 25; // current mseq for audio on vod, 25 out of 50.
    const mock_getAudioPlayheadPosition = async (pos_n_current) => {
      const mockPositions = [196.0, 199.84, 203.68, 207.52];
      return mockPositions[pos_n_current - mockMseqAudio];
    };
    const output = await session._determineExtraMediaIncrement(
      "audio",
      mockCurrentVideoPosition,
      mockFinalAudioIdx,
      mockMseqAudio,
      mock_getAudioPlayheadPosition,
      24
    );
    expect(output.increment).toBe(1);
  });

  it("for demuxed, returns the appropriate audio increment value when desync is within acceptable limit, case II", async () => {
    const session = new Session("dummy", null, sessionLiveStore);
    const mockFinalAudioIdx = 50; // current Vod has 50 media sequences to serve.
    const mockCurrentVideoPosition = 200.0 * 1000; // Video is 200s deep into its content.
    const mockMseqAudio = 25; // current mseq for audio on vod, 25 out of 50.
    const mock_getAudioPlayheadPosition = async (pos_n_current) => {
      const mockPositions = [192.16, 196.0, 199.84, 203.68, 207.52];
      return mockPositions[pos_n_current - mockMseqAudio];
    };
    const output = await session._determineExtraMediaIncrement(
      "audio",
      mockCurrentVideoPosition,
      mockFinalAudioIdx,
      mockMseqAudio,
      mock_getAudioPlayheadPosition,
      24
    );
    expect(output.increment).toBe(2);
  });

  it("for demuxed, returns the appropriate audio increment value when they are in sync but there is a floating point error", async () => {
    const session = new Session("dummy", null, sessionLiveStore);
    const mockFinalAudioIdx = 50;
    const mockCurrentVideoPosition = 441.7599999999981697 * 1000; 
    const mockMseqAudio = 25;
    const mock_getAudioPlayheadPosition = async (pos_n_current) => {
      const mockPositions = [437.919999999999, 441.75999999999897];
      return mockPositions[pos_n_current - mockMseqAudio];
    };
    const output = await session._determineExtraMediaIncrement(
      "audio",
      mockCurrentVideoPosition,
      mockFinalAudioIdx,
      mockMseqAudio,
      mock_getAudioPlayheadPosition,
      24
    );
    expect(output.increment).toBe(1);
  });
  it("for demuxed, returns the appropriate audio increment value, normal case I", async () => {
    const session = new Session("dummy", null, sessionLiveStore);
    const mockFinalAudioIdx = 50;
    const mockCurrentVideoPosition = 3.840 * 8 * 1000;
    const mockMseqAudio = 5;
    const mock_getAudioPlayheadPosition = async (pos_n_current) => {
      const mockPositions = [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52];
      return mockPositions[pos_n_current];
    };
    const output = await session._determineExtraMediaIncrement(
      "audio",
      mockCurrentVideoPosition,
      mockFinalAudioIdx,
      mockMseqAudio,
      mock_getAudioPlayheadPosition
    );
    expect(output.increment).toBe(3);
  });
  it("for demuxed, returns the appropriate audio increment value, normal case II", async () => {
    const session = new Session("dummy", null, sessionLiveStore);
    const mockFinalAudioIdx = 50;
    const mockCurrentVideoPosition = 14 * 1000;
    const mockMseqAudio = 0;
    const mock_getAudioPlayheadPosition = async (pos_n_current) => {
      const mockPositions = [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52];
      return mockPositions[pos_n_current];
    };
    const output = await session._determineExtraMediaIncrement(
      "subtitle",
      mockCurrentVideoPosition,
      mockFinalAudioIdx,
      mockMseqAudio,
      mock_getAudioPlayheadPosition
    );
    expect(output.increment).toBe(4);
  });
});
