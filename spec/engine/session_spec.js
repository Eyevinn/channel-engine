const Session = require("../../engine/session.js");

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
