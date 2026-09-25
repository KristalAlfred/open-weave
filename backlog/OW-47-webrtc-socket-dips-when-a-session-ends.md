---
id: OW-47
title: "A WebRTC socket reads connected for a poll when one of its sessions ends"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

The Strom adapter sums a WHIP or WHEP block's RTP bytes over every session in
`webrtc-stats` (`parse_webrtc_stats` in `crates/strom/src/stats.rs`) and feeds
the sum to `StallTracker` (`crates/adapter-strom/src/main.rs`). When one session
ends, its entry drops out of the sum, the total falls below the last poll's, and
the socket reads `connected` for that poll although the remaining session is
still carrying media.

Seen on the bench with Strom 0.6.10 while working OW-16: after the second page
restarted during `browser-b2b`, node 1's WHEP egress had the new session and the
old one. When the old entry went, the egress read `connected` for one poll and
the stream `degraded`, then `flowing` again for as long as it was watched.

## Done when

- [ ] A WebRTC socket with a session still carrying media reads `flowing` on the
      poll where another of its sessions ends.

## Easy to break

- Every WHIP session reports consumer id `whip-client`, so only the webrtcbin
  name in the stats key tells two WHIP sessions apart.

## Log

- 2026-09-25: filed by claude-webrtc from OW-16.
