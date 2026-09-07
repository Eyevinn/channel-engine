# Design note: subtitles in the live-mix + demux setting

Scoping note for issue #402. This document defines what "subtitle support in the
live-mix + demux setting" should mean and the acceptance criteria a follow-up
implementation issue can be filed against. It contains no code.

## Problem

`README.md`, directly under the "Supported Source Formats" compliance table,
states:

> NOTE: The engine does not support subtitles in the live-mix + demux setting.

The table backs this up: the `Mix w. Live & Subtitles` column is `No` for both
the `HLS + TS` and `HLS + CMAF` rows.

This is grounded in the current code:

- The VOD-only path (`engine/session.ts`) **does** implement subtitles — it has
  a working `getCurrentSubtitleManifestAsync(...)` and the
  `useVTTSubtitles` / `dummySubtitleEndpoint` / `subtitleSliceEndpoint`
  configuration wiring.
- The live path (`engine/session_live.ts`) does **not**: its
  `getCurrentSubtitleManifestAsync(...)` is a stub that logs
  `"getCurrentSubtitleManifestAsync is NOT Implemented"` and returns nothing.
  A code comment in that file explicitly notes subtitle support for live-mix is
  future work.
- `engine/stream_switcher.ts` has no subtitle handling at all; it reconciles
  video and (when demuxed) audio across a transition, but nothing for a
  subtitle rendition.

So today, once a channel enters the live-mix path, any subtitle rendition the
VOD side was serving has no live-side counterpart and no switcher continuity —
the gap the README documents.

## (a) What "supported" means — the explicit target

Two candidate definitions:

1. **Continuity**: the subtitle track keeps playing across the mix transition —
   the output subtitle rendition stays present and valid through the VOD→live
   (and live→VOD) boundary so a player selecting subtitles never loses them.
2. **No-crash-only**: subtitles may drop during the live portion, but audio and
   video must not break and the manifest must stay valid.

**Chosen target: (1) continuity.**

Justification: the compliance table's `Subtitles` column is already `Yes` for
the non-live cases, so users who reach live-mix reasonably expect subtitles to
keep working, not to silently disappear. A player that is told (via
`EXT-X-MEDIA TYPE=SUBTITLES`) that a subtitle group exists and then finds it
empty/absent during the live window is a worse, more confusing experience than
a clean drop — and "no-crash-only" is largely already true in practice (the
current failure mode is a missing/blank subtitle manifest, not a video crash),
so it would be a near-no-op target. Committing to continuity is the change that
actually closes the documented gap.

Continuity is defined concretely as: across the mix boundary the output master
playlist continues to advertise the same `EXT-X-MEDIA TYPE=SUBTITLES` group,
and the subtitle media playlist for that group continues to return valid,
monotonically advancing segments (real cues where the source has them, and
timed empty/dummy WebVTT segments where the live source has no cues) with no
gap that a compliant player rejects.

## (b) Which compliance-table cells this closes, and format scope

The two currently-`No` cells are `Mix w. Live & Subtitles` for `HLS + TS` and
for `HLS + CMAF`.

**First-pass scope: `HLS + CMAF` only.** Recommendation is to close the
`HLS + CMAF` cell first and leave `HLS + TS` for a follow-up.

Rationale: the demux + subtitle plumbing on the VOD side (and the existing
livemix-demux support noted in the README) is exercised most with CMAF/fMP4
content, and segment-aligned CMAF makes the timed-empty-segment continuity
approach simpler to reason about at the boundary. `HLS + TS` can follow once the
CMAF path is proven, reusing the same switcher continuity logic. The
acceptance criteria below are written so the `HLS + TS` cell can be closed later
by re-running the same fixture shape against TS content without a redesign.

So: this note's implementation issue closes the **`HLS + CMAF`** `Mix w. Live &
Subtitles` cell; the `HLS + TS` cell is explicitly deferred.

## (c) Concrete test fixture

A live-mix session with an active subtitle track transitioning across a VOD
boundary:

- A `useDemuxedAudio: true`, `useVTTSubtitles: true` channel.
- Schedule: a **VOD source that carries a WebVTT subtitle rendition** →
  transition into a **live source** (the live-mix path) → transition back to a
  VOD source with subtitles.
- The live source is the `Mix w. Live` case, so it drives the
  `engine/session_live.ts` + `engine/stream_switcher.ts` paths rather than the
  VOD-only `engine/session.ts` path.
- Assertion seam: render the subtitle media playlist (the output of the
  subtitle-manifest path) at media sequences spanning the VOD→live→VOD
  boundaries and assert the subtitle group remains advertised in the master and
  the subtitle playlist stays valid and continuous across each boundary — using
  the same offline render-and-assert approach as the existing
  `spec/engine/subtitle_spec.ts` / `spec/engine/session_spec.ts`, so it runs in
  CI without a live encoder.

The fixture should reuse existing subtitle test vectors where possible (e.g. the
`hls_subs*` / `hls_subs7_demux` vectors in `@eyevinn/hls-vodtolive/testvectors`)
plus the existing local `spec/testvectors/subtitle_file*.webvtt` files, paired
with a live playlist fixture for the mix segment.

## Acceptance criteria

A follow-up `feat:` implementation issue can be validated against:

- [ ] "Supported" is resolved to a single explicit target — **subtitle
      continuity across the mix transition** (not merely no-crash) — as defined
      in section (a).
- [ ] Format scope is stated: **`HLS + CMAF` in the first pass; `HLS + TS`
      deferred to a follow-up.**
- [ ] A named test fixture exists: a live-mix session (`useDemuxedAudio: true`,
      `useVTTSubtitles: true`) with an active WebVTT subtitle track
      transitioning across a VOD→live→VOD boundary, asserted offline in CI.
- [ ] `engine/session_live.ts` `getCurrentSubtitleManifestAsync(...)` returns a
      valid subtitle manifest for the live window instead of the current
      "NOT Implemented" stub.
- [ ] `engine/stream_switcher.ts` reconciles the subtitle rendition across a
      transition the same way it already reconciles the demuxed audio
      rendition, so the subtitle group is never dropped from the output master
      mid-channel.
- [ ] Across the boundary the output subtitle playlist advances monotonically
      with no gap a compliant player rejects (real cues where present, timed
      empty WebVTT segments where the live source has none).
- [ ] The README compliance table's `Mix w. Live & Subtitles` cell for
      `HLS + CMAF` is changed from `No` to `Yes`, and the note under the table
      is updated to reflect the remaining `HLS + TS` gap.
- [ ] Existing subtitle and live-mix specs still pass (no regression to the
      VOD-only subtitle path or to livemix-demux without subtitles).
