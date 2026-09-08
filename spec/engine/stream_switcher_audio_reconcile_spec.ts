const StreamSwitcher = require("../../engine/stream_switcher.js");

// #383: reconcile audio presentation at source transitions when muxing modes
// differ. Once #382 made muxing per-source, a demuxed-output channel can ingest
// a source that lacks demuxed audio (a muxed source). At a transition that
// crosses a muxed<->demuxed boundary the switcher must emit a single, consistent
// output presentation: only carry the demuxed audio rendition into the write
// when the SPECIFIC source being written actually provided demuxed audio, so the
// audio group / audio discontinuity-sequence stay coherent across the switch.
//
// These specs drive `_initSwitching` directly (the same seam init_switching_spec
// uses) with fully stubbed Session / SessionLive so the reconciliation DECISION
// is exercised deterministically, without standing up live playlist fetching.

const SwitcherState = Object.freeze({
  V2L_TO_LIVE: 1,
  V2L_TO_VOD: 2,
  LIVE_TO_V2L: 3,
  LIVE_TO_LIVE: 4,
  LIVE_TO_VOD: 5,
});

// A demuxed video-segment map (bandwidth -> segment list).
const mockVideoSegments = {
  "180000": [
    { duration: 7, uri: "http://mock/180000/seg09.ts" },
    { duration: 7, uri: "http://mock/180000/seg10.ts" },
    { discontinuity: true },
  ],
};
// A demuxed audio-segment map (group -> lang -> segment list).
const mockAudioSegments = {
  aac: {
    en: [
      { duration: 7, uri: "http://mock/aac/en/seg09.aac" },
      { duration: 7, uri: "http://mock/aac/en/seg10.aac" },
      { discontinuity: true },
    ],
  },
};

// Build a Session double whose truncated-VOD source is either demuxed or muxed.
function makeSessionDouble({ vodDemuxed }) {
  return {
    getCurrentMediaAndDiscSequenceCount: () =>
      Promise.resolve({ mediaSeq: 5, discSeq: 1, audioSeq: 5, audioDiscSeq: 1 }),
    getCurrentMediaSequenceSegments: () => Promise.resolve(mockVideoSegments),
    getCurrentAudioSequenceSegments: () => Promise.resolve(mockAudioSegments),
    getTruncatedVodSegments: () => Promise.resolve(JSON.parse(JSON.stringify(mockVideoSegments))),
    // A muxed VOD returns {} from getTruncatedVodAudioSegments (see session.ts).
    getTruncatedVodAudioSegments: () =>
      Promise.resolve(vodDemuxed ? JSON.parse(JSON.stringify(mockAudioSegments)) : {}),
    setCurrentMediaAndDiscSequenceCount: jasmine.createSpy("setCurrentMediaAndDiscSequenceCount").and.returnValue(Promise.resolve(true)),
    setCurrentMediaSequenceSegments: jasmine.createSpy("setCurrentMediaSequenceSegments").and.returnValue(Promise.resolve(true)),
  };
}

// Build a SessionLive double whose live source is either demuxed or muxed.
function makeSessionLiveDouble({ liveDemuxed }) {
  return {
    getCurrentMediaSequenceSegments: () => Promise.resolve({ currMseqSegs: JSON.parse(JSON.stringify(mockVideoSegments)), segCount: 2 }),
    // A muxed live source yields an empty currMseqSegs audio map.
    getCurrentAudioSequenceSegments: () =>
      Promise.resolve({ currMseqSegs: liveDemuxed ? JSON.parse(JSON.stringify(mockAudioSegments)) : {}, segCount: liveDemuxed ? 2 : 0 }),
    getCurrentMediaAndDiscSequenceCount: () =>
      Promise.resolve({ mediaSeq: 10, discSeq: 2, audioSeq: 10, audioDiscSeq: 2 }),
    setCurrentMediaAndDiscSequenceCount: jasmine.createSpy("live.setCurrentMediaAndDiscSequenceCount").and.returnValue(Promise.resolve(true)),
    setCurrentMediaSequenceSegments: jasmine.createSpy("live.setCurrentMediaSequenceSegments").and.returnValue(Promise.resolve(true)),
    setCurrentAudioSequenceSegments: jasmine.createSpy("live.setCurrentAudioSequenceSegments").and.returnValue(Promise.resolve(true)),
    resetSession: jasmine.createSpy("resetSession").and.returnValue(Promise.resolve(true)),
    resetLiveStoreAsync: jasmine.createSpy("resetLiveStoreAsync").and.returnValue(Promise.resolve(true)),
    setLiveUri: jasmine.createSpy("setLiveUri").and.returnValue(Promise.resolve("https://mock/live/master.m3u8")),
  };
}

function demuxedSwitcher() {
  const s = new StreamSwitcher({ streamSwitchManager: {}, useDemuxedAudio: true });
  s.streamTypeLive = true;
  s.prerollsCache = {};
  return s;
}

describe("Audio presentation reconciliation across muxing-mode boundaries (#383)", () => {
  const scheduleVod = {
    eventId: "vod-x",
    assetId: 1,
    uri: "https://mock/vod/master.m3u8",
    duration: 60 * 1000,
  };
  const scheduleLive = {
    eventId: "live-y",
    assetId: 2,
    uri: "https://mock/live2/master.m3u8",
  };

  // --- LIVE -> VOD -------------------------------------------------------------

  it("LIVE(demuxed)->VOD(muxed): does NOT push audio for the muxed VOD, but keeps the demuxed live audio", async () => {
    const switcher = demuxedSwitcher();
    const session = makeSessionDouble({ vodDemuxed: false });
    const sessionLive = makeSessionLiveDouble({ liveDemuxed: true });

    await switcher._initSwitching(SwitcherState.LIVE_TO_VOD, session, sessionLive, scheduleVod);

    // Two writes happen: (1) the outgoing live segments, (2) the incoming VOD.
    const calls = session.setCurrentMediaSequenceSegments.calls.allArgs();
    expect(calls.length).toBe(2);
    // (1) Outgoing demuxed live source -> audio carried (5 args, audio arg present).
    expect(calls[0][3]).toBeDefined();
    expect(calls[0][3]).toEqual(mockAudioSegments);
    // (2) Incoming MUXED VOD -> NO audio arg (video-only reconcile).
    expect(calls[1][3]).toBeUndefined();
  });

  it("LIVE(muxed)->VOD(demuxed): does NOT push audio for the muxed live, but DOES for the demuxed VOD", async () => {
    const switcher = demuxedSwitcher();
    const session = makeSessionDouble({ vodDemuxed: true });
    const sessionLive = makeSessionLiveDouble({ liveDemuxed: false });

    await switcher._initSwitching(SwitcherState.LIVE_TO_VOD, session, sessionLive, scheduleVod);

    const calls = session.setCurrentMediaSequenceSegments.calls.allArgs();
    expect(calls.length).toBe(2);
    // (1) Outgoing MUXED live -> video-only write, no audio arg.
    expect(calls[0][3]).toBeUndefined();
    // (2) Incoming demuxed VOD -> audio carried.
    expect(calls[1][3]).toBeDefined();
    expect(calls[1][3]).toEqual(mockAudioSegments);
  });

  // --- LIVE -> LIVE ------------------------------------------------------------

  it("LIVE(muxed)->LIVE(demuxed-channel): skips the empty audio write so the audio group stays coherent", async () => {
    const switcher = demuxedSwitcher();
    const session = makeSessionDouble({ vodDemuxed: true });
    const sessionLive = makeSessionLiveDouble({ liveDemuxed: false });

    const result = await switcher._initSwitching(SwitcherState.LIVE_TO_LIVE, session, sessionLive, scheduleLive);

    expect(result).toBe(true);
    // Video is always written.
    expect(sessionLive.setCurrentMediaSequenceSegments).toHaveBeenCalled();
    // The outgoing live source was muxed => no empty audio group is pushed.
    expect(sessionLive.setCurrentAudioSequenceSegments).not.toHaveBeenCalled();
  });

  it("LIVE(demuxed)->LIVE(demuxed-channel): carries the audio rendition through the switch", async () => {
    const switcher = demuxedSwitcher();
    const session = makeSessionDouble({ vodDemuxed: true });
    const sessionLive = makeSessionLiveDouble({ liveDemuxed: true });

    const result = await switcher._initSwitching(SwitcherState.LIVE_TO_LIVE, session, sessionLive, scheduleLive);

    expect(result).toBe(true);
    expect(sessionLive.setCurrentMediaSequenceSegments).toHaveBeenCalled();
    expect(sessionLive.setCurrentAudioSequenceSegments).toHaveBeenCalledWith(mockAudioSegments);
  });
});
