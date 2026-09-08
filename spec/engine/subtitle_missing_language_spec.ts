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

  it("REPRODUCES THE BUG: only DUMMY segments when name does not match and there is no LANGUAGE (#325)", (done) => {
    // CHARACTERIZATION of the still-broken case: with no LANGUAGE attribute AND a
    // configured name that differs from the manifest NAME, matching fails entirely and
    // the subtitle media playlist is filled with dummy vtt segments instead of real
    // cues. This documents the residual #325 bug. #385 will flip these two assertions
    // (real=true / dummy=false) once matching is made robust.
    const vod = new HLSVod("http://mock.com/master.m3u8", null, 0, 0, null, hlsOpts(SUBTITLE_TRACKS_MISMATCHED_NAME));
    vod
      .load(masterStream, videoStream, null, subtitleStream)
      .then(() => {
        const m3u8 = vod.getLiveMediaSubtitleSequences(0, "subs", "fr", 0);
        // Current (buggy) behavior:
        expect(m3u8).toContain(DUMMY_ENDPOINT);
        expect(m3u8).not.toContain(SLICE_ENDPOINT);
        done();
      })
      .catch(done.fail);
  });
});
