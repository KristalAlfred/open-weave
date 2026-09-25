---
id: OW-15
title: "A restarted page waits out Strom's inactivity timeout"
type: bug
status: done
depends_on: []
assignee: claude-webrtc
---

## Evidence

`whip_input` is created with `max_sessions: 1` (`crates/strom/src/spec.rs`) and
the page cannot release the session it left behind: Strom's CORS exposes only
`mcp-session-id`, so the `Location` header of the WHIP `201` is unreadable in a
browser and there is nothing to `DELETE`. Observed: Strom answers `503` for
10–20 s until its inactivity monitor frees the slot, then the new session flows.

## Done when

- [x] A page restart reconnects without that window when the page sends audio.

## Easy to break

- `max_sessions: 1` is what makes one page own one endpoint, and what a second
  connection to the same endpoint does with a higher limit is untested.

## Log

- 2026-09-25: moved from `BACKLOG.md` into its own file.
- 2026-09-25: started by claude-webrtc: pin the bench's Strom to 0.6.10.
- 2026-09-25: Done-when changed from "A page restart" to "a page that sends
  audio". Strom 0.6.10 displaces a dead WHIP session only if it delivered audio
  (Eyevinn/strom#753), and the page cannot `DELETE` its session while Strom's CORS
  hides `Location` (Eyevinn/strom#811, open). The video-only case is OW-46,
  blocked on Strom.
- 2026-09-25: ticked. Bench run on main at 7b2ce9d with this commit's bench
  changes, Strom 0.6.10, browser image from `bench/Dockerfile.browser` (Debian
  `chromium` 153). All four Strom services run `eyevinntechnology/strom:0.6.10`.
  Upgrade checks: `just bench stream-up basic` and `stream-up nat-egress` both
  reached `flowing`. Page restart with `docker restart -t 0 ow-browser`, audio
  and video: no `503`; Strom logged `Displacing session … (2807 ms without
  media)` about 3 s after the restart and `browser-cam` read `flowing` again
  after 14.6 s, which includes the page starting, the adapter's 5 s poll and a
  controller tick. Two earlier runs gave 2845 ms / 13.5 s and 2833 ms / 15.7 s.
  Video alone: two `503`s ("all 1 slots occupied by live sessions"), the slot
  freed by the inactivity monitor after 10.1 s idle, and `flowing` again after
  25.4 s (OW-46). Bench only.
