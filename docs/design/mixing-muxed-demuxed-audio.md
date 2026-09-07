# Design note: mixing muxed- and demuxed-audio sources in one channel

Scoping note for issue #381 (parent #271; implementation sub-issues #382/#383).
This document defines the target behaviour and acceptance criteria before any
implementation is started. It contains no code.

## Problem

A channel is fed a sequence of VOD (and optionally live) sources by the asset
manager. Today those sources are assumed to be homogeneous with respect to
audio packaging: either every source is *muxed* (audio interleaved in the same
rendition as video) or every source is *demuxed* (audio delivered as a separate
`EXT-X-MEDIA` audio group). The channel-level `useDemuxedAudio` flag
(`engine/server.ts`) picks one mode for the whole channel and every source is
expected to comply with it.

When a channel's schedule mixes the two — e.g. a muxed VOD followed by a
demuxed VOD on a `useDemuxedAudio: true` channel — the mismatched source fails
to load rather than being reconciled. This is grounded in the current code (see
"Affected code paths" below): `engine/session.ts` passes
`forcedDemuxMode: this.use_demuxed_audio` into the underlying `HLSVod`
(`@eyevinn/hls-vodtolive`), and that library **rejects with
`Error("The vod is not a demux vod")`** when a source presented under forced
demux mode carries no audio group. So a muxed source on a demuxed channel is a
hard load failure today, not a soft mismatch.

## (1) Target behaviour

For a single channel whose sources mix muxed-audio and demuxed-audio inputs:

- The sources must play **back-to-back across the VOD boundary without breaking
  the manifest** — the output master playlist and the output media/audio
  playlists must remain valid and internally consistent across the transition,
  and a compliant player must continue playback across the boundary without a
  fatal error or a manifest reload failure.
- A brief, bounded audio artefact at the discontinuity (one segment of silence
  or a single hiccup, on the order of the average segment duration) is
  acceptable for a first pass; a persistent audio/video desync or a manifest
  that a player rejects is **not** acceptable.
- "Mix" here covers the VOD→VOD case as the primary target. Live-mix
  (VOD↔live) transitions with mixed packaging are in scope for the acceptance
  criteria only insofar as they must not regress the existing live-mix +
  demux behaviour; full live-mix mixed-packaging support may be deferred to a
  follow-up if it proves materially harder.

## (2) Output manifest mode — recommendation

**Normalise to a single, consistent output presentation for the whole channel.**
Do not preserve per-source packaging in the output. A channel that flips the
output between muxed and demuxed at each VOD boundary would force the player to
re-init its audio pipeline mid-stream and is the most likely thing to break
playback.

Concretely: the channel's output mode is decided once, by the existing
`useDemuxedAudio` flag.

- `useDemuxedAudio: true`  → output is **always demuxed**: an `EXT-X-MEDIA`
  audio group is present and every served segment (from every source,
  regardless of that source's own packaging) participates in that single
  consistent audio-group presentation.
- `useDemuxedAudio: false` → output is **always muxed**: no separate audio
  group; audio is carried in the video rendition for every source.

The flag stays the single source of truth for output presentation; the new work
is making non-conforming sources fit the chosen output mode rather than failing.

## (3) Cross-packaging handling (muxed source, demuxed output — and vice-versa)

**On-the-fly remultiplexing / transcoding is explicitly out of scope.** The
engine is a manifest-manipulation / VOD2Live layer; it does not open, decode,
demux, or re-encode media segments, and it should not start doing so here.

Given that constraint, the two cross-packaging directions resolve as:

- **Muxed source, demuxed output (`useDemuxedAudio: true`)**: require the muxed
  source to **already carry a separate audio rendition** (an `EXT-X-MEDIA`
  audio group) that the engine can route into the output audio group. A source
  that is *only* muxed (video-with-embedded-audio and no audio rendition)
  cannot be losslessly presented as demuxed without extracting the audio track,
  which is remux — out of scope. Such a source must be handled deterministically
  (see acceptance criteria: reject-and-skip via the existing error/unassign
  path, not a crash), not silently mis-served.
- **Demuxed source, muxed output (`useDemuxedAudio: false`)**: the engine
  selects a single audio rendition from the source's audio group and presents
  the source as muxed in the output. Where the source's video rendition does
  not already contain that audio, this again requires the player to be served
  audio it can associate with the video without a remux; if that cannot be done
  losslessly it falls under the same deterministic reject-and-skip rule.

In short: **the engine reconciles packaging by routing existing renditions, not
by producing new media.** A source that would require new media to fit the
output mode is rejected cleanly rather than breaking the channel.

## (4) Affected code paths

Verified against the current tree (the engine files are TypeScript, `*.ts`, not
`*.js` as the parent issue's shorthand suggested):

- **`engine/server.ts`** — declares and stores the `useDemuxedAudio` option
  (`ChannelEngineOpts.useDemuxedAudio`, the private `useDemuxedAudio` field,
  and its default-to-`false` handling), and forwards it into the session /
  session-live / stream-switcher configs it constructs. This is where the
  "output mode is a single channel-level decision" contract lives.
- **`engine/session.ts`** — reads the flag into `use_demuxed_audio` and, at VOD
  creation, passes `forcedDemuxMode: this.use_demuxed_audio` into `new
  HLSVod(...)`. This is the exact point where a muxed source on a demuxed
  channel currently causes the underlying library to reject with "The vod is
  not a demux vod". The reconciliation/validation decided above must be applied
  here (and it must feed the existing per-item error handling rather than
  throwing uncaught).
- **`engine/session_live.ts`** — mirrors the demuxed handling for the live
  path (`this.useDemuxedAudio`, the audio-disc-sequence and audio-segment
  branches). Any normalisation must keep the live path's audio group consistent
  with the VOD path across a live-mix transition.
- **`engine/stream_switcher.ts`** — gates transitions on the presence of audio
  segments when `useDemuxedAudio` is set (the `isValid` /
  `ItemIsEmpty(segments.audioSegments)` checks and the numerous
  `this.useDemuxedAudio` branches around the switch logic). This is where a
  packaging mismatch at a boundary is currently detected as "no audio
  segments"; the reject-and-skip vs. reconcile decision must be enforced
  consistently here too, so the switcher never advances into a state that
  produces an invalid manifest.

## Acceptance criteria

The implementation sub-issues (#382/#383) can be validated against this list:

- [ ] The channel's output presentation is decided solely by `useDemuxedAudio`
      and is identical for every source in the channel (never flips
      muxed/demuxed mid-stream).
- [ ] A schedule that alternates a demuxed source and a *muxed source that
      already carries an audio rendition* on a `useDemuxedAudio: true` channel
      plays across the VOD boundary and produces a valid master + media + audio
      playlist at and after the transition.
- [ ] The same alternation on a `useDemuxedAudio: false` channel produces valid
      muxed output across the boundary.
- [ ] A muxed-only source (no audio rendition) presented to a
      `useDemuxedAudio: true` channel is handled deterministically: it is
      rejected/skipped through the existing error → unassign → next-item path
      (per the repo's "on any unexpected failure" rule) and does **not** crash
      the session, wedge the stream switcher, or emit an invalid manifest.
- [ ] No segment-level remux/transcode is introduced anywhere in
      `engine/session.ts`, `engine/session_live.ts`, or
      `engine/stream_switcher.ts` (reconciliation is rendition routing only).
- [ ] Existing homogeneous-packaging behaviour (all-muxed channel, all-demuxed
      channel) is unchanged — the current session / session_live /
      stream_switcher specs still pass.
- [ ] A regression spec drives a mixed-packaging schedule through the same
      seam the engine uses (VOD load → live media sequence render, as in the
      existing `spec/engine/session_spec.ts`) and asserts a valid transition
      rather than the current `"The vod is not a demux vod"` rejection.
- [ ] The README "Supported Source Formats" section is updated to describe
      mixed-packaging support once implemented.
