---
id: OW-14
title: "A video-only WHIP sender leaves the gateway flow paused"
type: bug
status: todo
priority: 3
depends_on: []
assignee:
branch:
pr:
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

- [ ] A sender with video and no audio reaches `flowing`.

## Easy to break

- Leaving the audio pad unlinked unconditionally drops the audio of every sender
  that does send some.

## Log

- 2026-09-25: moved from `BACKLOG.md` into its own file.
