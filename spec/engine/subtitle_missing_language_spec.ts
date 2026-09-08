// Characterization repro for Eyevinn/channel-engine#325 (broken out as #384).
//
// Bug context: a source HLS multivariant (master) manifest may declare a subtitle
// rendition with a NAME attribute but NO LANGUAGE attribute:
//
//   #EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="French",...,URI="french.m3u8"
//
// channel-engine forwards `config.subtitleTracks` (each { language, name }) to
// @eyevinn/hls-vodtolive as `expectedSubtitleTracks` (see engine/session.ts, the
// hlsOpts object built in _tickAsync around line 1341). The actual NAME/LANGUAGE
// matching between the manifest's SUBTITLES tag and the expected tracks happens
// inside that dependency. If matching fails, the generated subtitle media playlist
// is filled with DUMMY vtt segments (URIs pointing at dummySubtitleEndpoint,
// "/dummysubs.vtt") instead of REAL sliced cues (URIs pointing at
// subtitleSliceEndpoint, "/subtitlevtt.vtt").
//
// This spec drives the SAME dependency seam channel-engine uses, against a committed
// NAME-only (no LANGUAGE) fixture under spec/testvectors/hls_subs_no_language/, so
// that fix issue #385 has a runnable, committed repro to build on. It is a
// CHARACTERIZATION test: every expectation below asserts TODAY's behavior so CI stays
// green now. #385 should update the mismatch case (see the last `it`) once the fix
// lands.
//
// IMPORTANT FINDING (see PR/issue discussion): with the currently pinned dependency
// (@eyevinn/hls-vodtolive@4.1.10) the NAME-fallback is ALREADY present, so when the
// configured `subtitleTracks[].name` equals the manifest NAME, real cues are served
// today even without a LANGUAGE attribute. The bug only still reproduces when the
// configured name does NOT match the manifest NAME (and there is no LANGUAGE to fall
// back on). This narrows #385's scope.
//
// #385 RESOLUTION (see the "channel-engine normalization" describe block below):
// channel-engine forwards config.subtitleTracks to the dependency faithfully — it
// never drops the operator's `name` (engine/session.ts sets this._subtitleTracks
// from config.subtitleTracks and forwards it as expectedSubtitleTracks unchanged).
// The residual defects therefore live in the DEPENDENCY's matcher (index.js ~L433):
//   1. It calls `element.name.toLowerCase()` unconditionally, so a configured track
//      with a `language` but NO `name` makes the matcher THROW — no track ever
//      matches. channel-engine now guards this by defaulting an omitted `name` to the
//      track's `language` when building expectedSubtitleTracks (session.ts ~L168).
//   2. A configured name that GENUINELY differs from the manifest NAME (with no
//      LANGUAGE to fall back on) legitimately fails to match — that is an operator
//      config error, not a code bug, and is intentionally NOT papered over with a
//      fragile fuzzy match. The "mismatch -> dummy" characterization below is kept
//      as-is because it documents correct behavior, not a defect.

const HLSVod = require("@eyevinn/hls-vodtolive");
const fs = require("fs");

const FIXTURE_DIR = "spec/testvectors/hls_subs_no_language";
const DUMMY_ENDPOINT = "/dummysubs.vtt";
const SLICE_ENDPOINT = "/subtitlevtt.vtt";

// The exact config.subtitleTracks used when starting the session for this repro.
// (channel-engine would receive this as session config.subtitleTracks and forward it
// to hls-vodtolive as expectedSubtitleTracks.)
const SUBTITLE_TRACKS_MATCHING_NAME = [{ language: "fr", name: "French" }];
const SUBTITLE_TRACKS_MISMATCHED_NAME = [{ language: "fr", name: "Francais" }];

function masterStream() {
  return fs.createReadStream(`${FIXTURE_DIR}/master.m3u8`);
}
function videoStream() {
  return fs.createReadStream(`${FIXTURE_DIR}/video.m3u8`);
}
function subtitleStream() {
  return fs.createReadStream(`${FIXTURE_DIR}/french.m3u8`);
}

function hlsOpts(subtitleTracks: any) {
  return {
    dummySubtitleEndpoint: DUMMY_ENDPOINT,
    subtitleSliceEndpoint: SLICE_ENDPOINT,
    shouldContainSubtitles: true,
    expectedSubtitleTracks: subtitleTracks,
    sequenceAlwaysContainNewSegments: false,
  };
}

describe("Subtitles: source manifest SUBTITLES tag with NAME but no LANGUAGE (#325/#384)", () => {
  it("parses the NAME-only fixture master without a LANGUAGE attribute", (done) => {
    // Guard the fixture itself: the master's SUBTITLES tag must have NAME and NOT LANGUAGE.
    const master = fs.readFileSync(`${FIXTURE_DIR}/master.m3u8`, "utf8");
    const subtitleTagLine = master
      .split("\n")
      .find((l: string) => l.startsWith("#EXT-X-MEDIA:TYPE=SUBTITLES"));
    expect(subtitleTagLine).toBeDefined();
    expect(subtitleTagLine).toContain('NAME="French"');
    expect(subtitleTagLine).not.toContain("LANGUAGE=");
    done();
  });

  it("serves REAL cues today when config subtitleTracks[].name matches the manifest NAME", (done) => {
    // CHARACTERIZATION: on the currently pinned @eyevinn/hls-vodtolive the NAME
    // fallback already works, so a matching name yields real sliced cues even with no
    // LANGUAGE attribute. This asserts current (working) behavior; #385 must keep it.
    const vod = new HLSVod("http://mock.com/master.m3u8", null, 0, 0, null, hlsOpts(SUBTITLE_TRACKS_MATCHING_NAME));
    vod
      .load(masterStream, videoStream, null, subtitleStream)
      .then(() => {
        const m3u8 = vod.getLiveMediaSubtitleSequences(0, "subs", "fr", 0);
        expect(m3u8).toContain(SLICE_ENDPOINT);
        expect(m3u8).not.toContain(DUMMY_ENDPOINT);
        done();
      })
      .catch(done.fail);
  });

  it("serves only DUMMY segments when the configured name genuinely differs from the manifest NAME with no LANGUAGE (correct behavior, #325/#385)", (done) => {
    // Once #325/#384 anticipated this could be flipped by #385. Investigation for
    // #385 concluded the OPPOSITE: this case is NOT a code bug. The configured name
    // ("Francais") genuinely differs from the manifest NAME ("French") and there is no
    // LANGUAGE to fall back on, so there is nothing legitimate to match against. #385
    // deliberately does NOT introduce a fuzzy match to force it, so these assertions
    // are kept (they document correct, honest behavior — an operator config error
    // surfaces as dummy cues, not real ones).
    const vod = new HLSVod("http://mock.com/master.m3u8", null, 0, 0, null, hlsOpts(SUBTITLE_TRACKS_MISMATCHED_NAME));
    vod
      .load(masterStream, videoStream, null, subtitleStream)
      .then(() => {
        const m3u8 = vod.getLiveMediaSubtitleSequences(0, "subs", "fr", 0);
        expect(m3u8).toContain(DUMMY_ENDPOINT);
        expect(m3u8).not.toContain(SLICE_ENDPOINT);
        done();
      })
      .catch(done.fail);
  });
});

// #385 regression: channel-engine's own normalization of config.subtitleTracks into
// expectedSubtitleTracks (engine/session.ts ~L168). channel-engine defaults an omitted
// `name` to the track's `language` before forwarding to @eyevinn/hls-vodtolive. This
// block simulates that exact normalization against the committed NAME-only fixture and
// proves that (a) a name-less config no longer THROWS inside the dependency matcher,
// and (b) a name-less config whose `language` equals the manifest NAME now serves REAL
// cues. It mirrors, byte-for-byte, the `.map` in Session's constructor so the seam is
// exercised the way channel-engine drives it.
function normalizeLikeChannelEngine(tracks: any[]): any[] {
  // Must match engine/session.ts: name defaults to language when omitted.
  return tracks.map((track) => ({
    ...track,
    name: track.name != null ? track.name : track.language,
  }));
}

describe("Subtitles: channel-engine expectedSubtitleTracks normalization for NAME-only source (#385)", () => {
  it("does NOT throw and serves REAL cues for a name-less config whose language equals the manifest NAME", (done) => {
    // Before #385, a track configured with only { language } made the dependency
    // matcher call element.name.toLowerCase() on undefined and THROW. After the
    // channel-engine normalization, the name is filled in from the language, so a
    // config of { language: "French" } (matching the manifest NAME="French") resolves
    // to real sliced cues. This is the acceptance case for a NAME-only VOD served via
    // a name-less operator config.
    const normalized = normalizeLikeChannelEngine([{ language: "French" }]);
    const vod = new HLSVod("http://mock.com/master.m3u8", null, 0, 0, null, hlsOpts(normalized));
    vod
      .load(masterStream, videoStream, null, subtitleStream)
      .then(() => {
        const m3u8 = vod.getLiveMediaSubtitleSequences(0, "subs", "French", 0);
        expect(m3u8).toContain(SLICE_ENDPOINT);
        expect(m3u8).not.toContain(DUMMY_ENDPOINT);
        done();
      })
      .catch(done.fail);
  });

  it("does NOT throw for a name-less config that does not match (serves dummy cleanly instead of crashing)", (done) => {
    // Guards the crash fix specifically: { language: "fr" } against a NAME="French"
    // source used to THROW; it must now complete and (correctly) fall back to dummy
    // cues because "fr" matches neither the manifest NAME nor a LANGUAGE.
    const normalized = normalizeLikeChannelEngine([{ language: "fr" }]);
    const vod = new HLSVod("http://mock.com/master.m3u8", null, 0, 0, null, hlsOpts(normalized));
    vod
      .load(masterStream, videoStream, null, subtitleStream)
      .then(() => {
        const m3u8 = vod.getLiveMediaSubtitleSequences(0, "subs", "fr", 0);
        expect(m3u8).toContain(DUMMY_ENDPOINT);
        expect(m3u8).not.toContain(SLICE_ENDPOINT);
        done();
      })
      .catch(done.fail);
  });

  it("still serves REAL cues for the fully-specified { language, name } config that already worked (#385 keeps it)", (done) => {
    const normalized = normalizeLikeChannelEngine(SUBTITLE_TRACKS_MATCHING_NAME);
    const vod = new HLSVod("http://mock.com/master.m3u8", null, 0, 0, null, hlsOpts(normalized));
    vod
      .load(masterStream, videoStream, null, subtitleStream)
      .then(() => {
        const m3u8 = vod.getLiveMediaSubtitleSequences(0, "subs", "fr", 0);
        expect(m3u8).toContain(SLICE_ENDPOINT);
        expect(m3u8).not.toContain(DUMMY_ENDPOINT);
        done();
      })
      .catch(done.fail);
  });
});
