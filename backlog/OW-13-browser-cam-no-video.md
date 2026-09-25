---
id: OW-13
title: "browser-cam carries audio and no video"
type: bug
status: done
depends_on: []
assignee: claude-webrtc
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

- [x] `just bench browser-stream` reaches `flowing` on `browser-cam` with video
      in the SRT output.

## Easy to break

- `bench/justfile` prints `browser-cam`'s status instead of waiting on it, and
  `bench/README.md` records that it does not settle, so both change with the
  fix.

## Log

- 2026-09-25: moved from `BACKLOG.md` into its own file.
- 2026-09-25: started by claude-webrtc: Debian trixie's `chromium` in the bench
  browser image, driven by Playwright through `executablePath`.
- 2026-09-25: on the bench with Strom 0.6.6 (first session, same branch) the page
  sent H264 and Opus, Strom logged `Pad video_0` but never linked a video
  decoder, the gateway flow stayed `Paused`, and `ffprobe` on the SRT output saw
  AAC only. So the fix also needs the Strom 0.6.10 pin from OW-15.
- 2026-09-25: ticked. Bench run on main at 7b2ce9d with this commit's bench
  changes, Strom 0.6.10, browser image from `bench/Dockerfile.browser` (Debian
  `chromium` 153). `just bench browser-stream` ended with `browser-return:
  flowing` and `browser-cam: flowing`. `ffprobe` from inside `ow-consumer` on
  the SRT output: `h264` 640x480 and `aac` 48000 Hz 2 ch. `bench/justfile` now
  waits on `browser-cam`; `bench/README.md` and `bench/manifests/README.md` say
  what it reaches. Bench only.
