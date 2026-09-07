const HLSVod = require("@eyevinn/hls-vodtolive");
const fs = require("fs");

/*
 * Regression coverage for issue #401 — Widevine DRM over HLS + CMAF.
 *
 * `examples/drm.ts` wires the engine up with `useDemuxedAudio: true` and feeds
 * it Widevine-encrypted HLS+CMAF (fMP4) VODs. The DRM signalling itself is not
 * something the engine rewrites: the engine leans on @eyevinn/hls-vodtolive to
 * convert each VOD into the live media sequences it serves, and that conversion
 * must carry the source's EXT-X-KEY (Widevine) and EXT-X-MAP (CMAF init
 * segment) tags through unchanged so a player can still acquire a licence.
 *
 * This spec drives the exact same seam the engine uses at VOD creation
 * (`new HLSVod(...).load(masterLoader, mediaLoader, audioLoader)` — see
 * engine/session.js) against the vod-lib `hls_widevine` fixture, which is a
 * genuine Shaka-packaged Widevine + CMAF asset (SAMPLE-AES-CTR, Widevine
 * system id urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed), and asserts the
 * DRM/CMAF signalling survives into both the video playlist and the demuxed
 * audio rendition.
 *
 * What this does NOT cover (documented honestly): it does not exercise a real
 * Widevine licence server, does not decrypt media, and does not assert end-to-end
 * playback in a browser. Those require a real licence proxy and DRM-capable
 * player, neither of which is available in CI. The strongest offline guarantee
 * — that the engine's VOD2Live conversion never strips or corrupts the Widevine
 * key material or the CMAF init segment for a demuxed HLS+CMAF asset — is what
 * is asserted here.
 */

// The Widevine DRM system id (Common PSSH / EME) as it appears in KEYFORMAT.
const WIDEVINE_SYSTEM_ID = "urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed";

// jasmine runs from the repo root, so paths are relative to it.
const WIDEVINE_TV = "node_modules/@eyevinn/hls-vodtolive/testvectors/hls_widevine/";

// The fixture master (index.m3u8) advertises each video variant by BANDWIDTH;
// hls-vodtolive asks its media loader for a playlist by bandwidth, so map them.
const BW_TO_PLAYLIST = {
  831086: "playlist_v-0144p-0100k-libx264.mp4.m3u8",
  8065760: "playlist_v-0576p-1400k-libx264.mp4.m3u8",
  6099164: "playlist_v-0480p-1000k-libx264.mp4.m3u8",
  2193558: "playlist_v-0240p-0400k-libx264.mp4.m3u8",
  4008262: "playlist_v-0360p-0750k-libx264.mp4.m3u8",
};
const AUDIO_PLAYLIST = "playlist_a-eng-0384k-aac-6c.mp4.m3u8";

const masterLoader = () => fs.createReadStream(WIDEVINE_TV + "index.m3u8");
const mediaLoader = (bandwidth) =>
  fs.createReadStream(WIDEVINE_TV + BW_TO_PLAYLIST[bandwidth]);
// audio loader is called with (groupId, language); this fixture has one of each.
const audioLoader = () => fs.createReadStream(WIDEVINE_TV + AUDIO_PLAYLIST);

const FIXED_TS = Date.UTC(2026, 8, 4, 0, 0, 0); // 2026-09-04T00:00:00.000Z

async function loadWidevineVod() {
  const vod = new HLSVod("http://mock.com/index.m3u8", [], FIXED_TS, 0);
  await vod.load(masterLoader, mediaLoader, audioLoader);
  return vod;
}

describe("DRM: Widevine over HLS + CMAF (issue #401)", () => {
  it("loads the Widevine+CMAF VOD used by examples/drm.ts without stripping DRM", async () => {
    const vod = await loadWidevineVod();
    // A successfully loaded VOD exposes variants; if the DRM/CMAF manifest had
    // failed to parse, load() would have rejected and we would not get here.
    expect(vod.getBandwidths().length).toBeGreaterThan(0);
    expect(vod.getAudioGroups().length).toBeGreaterThan(0);
  });

  it("carries the Widevine EXT-X-KEY through into the served video playlist", async () => {
    const vod = await loadWidevineVod();
    const bw = vod.getBandwidths().sort((a, b) => a - b)[0];
    const playlist = vod.getLiveMediaSequences(0, bw, 0, 0);

    const keyLine = playlist
      .split("\n")
      .find((l) => l.startsWith("#EXT-X-KEY"));

    expect(keyLine).toBeDefined();
    // Widevine, not FairPlay: assert the Widevine system id and the CMAF
    // sample encryption method the fixture uses.
    expect(keyLine).toContain(`KEYFORMAT="${WIDEVINE_SYSTEM_ID}"`);
    expect(keyLine).toContain("METHOD=SAMPLE-AES-CTR");
    // FairPlay would surface as com.apple.streamingkeydelivery; make sure the
    // conversion did not somehow relabel the key as FairPlay.
    expect(playlist).not.toContain("com.apple.streamingkeydelivery");
  });

  it("carries the CMAF EXT-X-MAP init segment through into the served video playlist", async () => {
    const vod = await loadWidevineVod();
    const bw = vod.getBandwidths().sort((a, b) => a - b)[0];
    const playlist = vod.getLiveMediaSequences(0, bw, 0, 0);

    const mapLine = playlist
      .split("\n")
      .find((l) => l.startsWith("#EXT-X-MAP"));

    // Presence of EXT-X-MAP is what distinguishes CMAF/fMP4 from TS here.
    expect(mapLine).toBeDefined();
    expect(mapLine).toContain('URI="');
    expect(mapLine).toContain("-init.mp4");
  });

  it("carries the Widevine key and CMAF init through the demuxed audio rendition", async () => {
    // examples/drm.ts runs with useDemuxedAudio: true, so the audio group is
    // served separately and must keep its own DRM/CMAF signalling.
    const vod = await loadWidevineVod();
    const group = vod.getAudioGroups()[0];
    const lang = vod.getAudioLangsForAudioGroup(group)[0];
    const audioSegments = vod.getLiveMediaSequenceAudioSegments(group, lang, 0);

    // Find a real (non-discontinuity) segment that carries key material.
    const keyed = audioSegments.find(
      (seg) => seg && seg.keys && seg.keys[WIDEVINE_SYSTEM_ID]
    );
    expect(keyed).toBeDefined();
    expect(keyed.keys[WIDEVINE_SYSTEM_ID].method).toEqual("SAMPLE-AES-CTR");
    expect(keyed.keys[WIDEVINE_SYSTEM_ID].keyId).toBeDefined();

    // And the demuxed audio must still reference its CMAF init segment.
    const withInit = audioSegments.find(
      (seg) => seg && typeof seg.initSegment === "string"
    );
    expect(withInit).toBeDefined();
    expect(withInit.initSegment).toContain("-init.mp4");
  });
});
