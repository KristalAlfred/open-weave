---
id: OW-14
title: "A video-only WHIP sender leaves the gateway flow paused"
type: bug
status: done
depends_on: []
assignee: claude-webrtc
---

## Evidence

`whip_to_srt_flow` always links `whip_in:audio_out` into `mpegtssrt_output`'s
`audio_in_0` (`crates/strom/src/spec.rs`). A run with H264 available and no
microphone left the flow at `gst_state: Paused` with 0 bytes out and the audio
pad linked to nothing.

A fix needs the adapter to know which tracks the sender has, so it can set
`whip_input.mode` or leave the audio pad unlinked. A `DesiredHop` carries no
track list today.

## Done when

- [x] A sender with video and no audio reaches `flowing`.

## Easy to break

- Leaving the audio pad unlinked unconditionally drops the audio of every sender
  that does send some.

## Log

- 2026-09-25: moved from `BACKLOG.md` into its own file.
- 2026-09-25: started by claude-webrtc: the page declares its capture tracks,
  the controller copies them onto every hop, the Strom adapter builds the flow
  to match.
- 2026-09-25: landed in 7b2ce9d. A capture profile declares `tracks`
  (`DeviceClass.tracks`) and the page fills it from `#media`. The controller
  copies it, or the tracks an outside WHIP sender's declared `format` carries,
  onto every hop as `DesiredHop.tracks`. The Strom adapter sets
  `whip_input.mode` from it, gives the SRT and WHEP blocks no pad for a missing
  track, drops the encoder for audio alone, and recreates a flow built for other
  tracks. The sender profile's `accepts` (OW-10) is checked on the tracks the
  sender's hop carries, so a video-only H264 source is compatible and a
  video-only VP8 one is not. `PROTOCOL_VERSION` was already 5 on `main`;
  contracts regenerated. Unit tests: golden `whip-srt-video.json`, audio-only,
  WHEP fan-out, drift on tracks, `validate_node`, the planner copying tracks,
  and `a_whip_sender_declaring_one_track_is_built_and_checked_for_that_track`.
- 2026-09-25: ticked. Bench run on main at 7b2ce9d with this commit's bench
  changes, Strom 0.6.10, browser image from `bench/Dockerfile.browser` (Debian
  `chromium` 153). `just bench browser-up video`: the page registered `tracks:
  [video]`, adapter-1 deleted and recreated the gateway flow with `mode: video`
  and `num_audio_tracks: 0`, `browser-cam` read `flowing`, and `ffprobe` on the
  SRT output saw `h264` 640x480 and no audio. With the page sending both, the
  same output carried `h264` and `aac` (OW-13's run). Bench and unit tests.
