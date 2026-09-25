---
id: OW-16
title: "Browser-to-browser media has no supported Strom profile"
type: feature
status: done
depends_on: []
assignee: claude-webrtc
---

## Evidence

Strom can build `whip_input → whep_output`, but the adapter reads media progress
from SRT byte counters and this shape has no SRT side. Strom therefore does not
advertise a `whip → whep` hop profile, and planning fails before desired state
is sent.

## Done when

- [x] The adapter has a per-session media signal for a `whip → whep` flow.
- [x] Strom advertises the `whip → whep` profile and a browser-to-browser stream
      reaches `flowing`.

## Easy to break

- Advertising the profile without that signal makes a working path read
  `degraded`.

## Log

- 2026-09-25: moved from `BACKLOG.md` into its own file.
- 2026-09-25: started by claude-webrtc, box 1 first: WHIP/WHEP conditions from
  `webrtc-stats`.
- 2026-09-25: the adapter change landed in b5281f6. A WHIP or WHEP socket with
  no session reads `idle`, so a sender that leaves makes the stream read
  `awaiting_input`, where an SRT source that leaves reads `degraded` by the
  code. Filed as OW-40; not changed here.
- 2026-09-25: box 1 ticked. Unit tests on `webrtc-stats` payloads recorded from
  throwaway Strom 0.6.6 containers (WebKit sending over WHIP into `whip_in`,
  Chromium playing `whep_out_0`): the parser, and hop statuses for
  `whip-to-srt`, `srt-to-whep` and `whip-to-whep` hops, including a `Paused`
  flow with advancing bytes and an unfed `Playing` one. On the bench, first with
  0.6.6 and then in the bench run on main at 7b2ce9d with this commit's bench
  changes, Strom 0.6.10, browser image from `bench/Dockerfile.browser` (Debian
  `chromium` 153): `browser-return`'s WHEP egress and `browser-cam`'s WHIP
  ingress read `flowing` and the streams `flowing`. On 0.6.6 `browser-cam` also
  read `flowing` while its SRT output carried audio alone (OW-13): the signal is
  RTP and SRT bytes, and a track that never arrives is not seen.
- 2026-09-25: box 2 ticked. Since 7b2ce9d Strom advertises `whip-to-whep`
  (`whip_input` with decode, then `whep_output`; golden `whip-whep.json`); the
  planner test `browser_to_browser_relays_through_strom_whip_to_whep` places it
  between two pages. Bench run on main at 7b2ce9d with this commit's bench
  changes, Strom 0.6.10, browser image from `bench/Dockerfile.browser` (Debian
  `chromium` 153): a second page (`browser-2`, node `browser-bench-2`) and `just
  bench browser-b2b` reached `flowing` with node 1 as the relay, every hop
  `flowing` on both sockets for 30 s, and the second page receiving H264 and
  Opus at 0.85 Mb/s. The second page needed a route to the node subnets
  (`scripts/route-manager.sh`). A dip to `connected` for one poll when an old
  WHEP session ended is OW-47. Bench and unit tests.
