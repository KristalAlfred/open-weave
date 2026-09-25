---
id: OW-47
title: "A WebRTC socket reads connected for a poll when one of its sessions ends"
type: bug
status: done
depends_on: []
assignee: claude-webrtc
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

- [x] A WebRTC socket with a session still carrying media reads `flowing` on the
      poll where another of its sessions ends.

## Easy to break

- Every WHIP session reports consumer id `whip-client`, so only the webrtcbin
  name in the stats key tells two WHIP sessions apart.

## Log

- 2026-09-25: filed by claude-webrtc from OW-16.
- 2026-09-25: started by claude-webrtc.
- 2026-09-25: reproduced by unit test
  `a_whep_egress_keeps_flowing_when_an_old_session_ends` (constructed
  `webrtc-stats`: an old and a new WHEP session, then the new one alone): it
  read `connected` on the poll where the old entry went. Fixed:
  `parse_webrtc_stats` keeps each entry's bytes (`SessionStats.by_session`) and
  `StallTracker::session_total` adds each session's bytes since the last poll
  to a total that never falls, which is what the tracker sees. The test now
  reads `flowing` on every poll after the first, and
  `session_total_never_falls_when_a_session_ends` covers the fold. Unit tests
  only.
