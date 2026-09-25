---
id: OW-13
title: "browser-cam carries audio and no video"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

Strom's `whip_input` sets `video-codecs = ["H264"]`
(`backend/src/blocks/builtin/whip.rs` in Strom) and the bench's Playwright
Chromium on arm64 has no H264 encoder, so the session negotiates Opus alone.
Node 1's gateway hop then emits a ~5 kb/s audio-only trickle on its SRT output,
never leaves `gst_state: Paused`, reads `idle` on its WHIP ingress, and Strom's
inactivity monitor drops the session every ~20 s before the page reconnects.
The stream reports `degraded`.

The gateway flow itself is fine: Google Chrome on the macOS host, through the
`docker-host` attachment and `browser-cam-host`, put H264 640x480 plus AAC on
the SRT output with the flow `Playing` (`bench/README.md`, "A page in your own
browser").

Two routes: a Chromium that encodes H264 (Google Chrome on x86_64 — there is no
Linux arm64 build), or VP8/VP9 accepted by `whip_input`, which is a change to
Strom.

## Done when

- [ ] `just bench browser-stream` reaches `flowing` on `browser-cam` with video
      in the SRT output.

## Easy to break

- `bench/justfile` prints `browser-cam`'s status instead of waiting on it, and
  `bench/README.md` records that it does not settle, so both change with the
  fix.

## Log

- 2026-09-25: moved from `BACKLOG.md` into its own file.
