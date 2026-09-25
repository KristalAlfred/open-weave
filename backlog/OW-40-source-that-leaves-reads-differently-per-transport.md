---
id: OW-40
title: "A source that leaves reads differently over WHIP and over SRT"
type: bug
status: done
depends_on: []
assignee: claude-webrtc
---

## Evidence

Found by reading the code while working OW-16; not run on the bench.

- `webrtc_condition` in `crates/adapter-strom/src/provision.rs` reads a WHIP or
  WHEP socket with no session carrying RTP as `idle`, whatever its bytes did
  before. A page that closes its WHIP session therefore leaves the gateway's
  ingress `idle`, and `roll_up_path` (`crates/core/src/lib.rs`) reports the
  stream `awaiting_input`.
- `socket_condition` in the same file puts `stalled` before everything else.
  `StallTracker` marks a side stalled after three polls without byte progress
  once it has flowed, unless the flow is `Paused`. An SRT producer that
  disconnects leaves its callers' byte total frozen or lower, so by the code the
  ingress reads `stalled` and the stream `degraded`.

The same event, the source going away, gives two different stream statuses
depending on the transport.

## Done when

- [x] A source that leaves gives the same stream status over WHIP and over SRT,
      and `README.md` says which.

## Unchecked

- Whether a Strom SRT listener flow goes back to `Paused` when its producer
  leaves. If it does, the stall is suppressed and SRT already reads
  `awaiting_input`.

## Log

- 2026-09-25: filed by claude-webrtc from OW-16.
- 2026-09-25: started by claude-webrtc: WHIP/WHEP follow the stall rule SRT uses.
- 2026-09-25: ticked. `webrtc_condition` puts a stall first, as
  `socket_condition` does, and a WHIP or WHEP side with no session feeds the
  stall tracker an unchanged byte total, so a socket that carried media and has
  had none for three polls reads `stalled` and the stream `degraded`. Unit test
  `a_source_that_leaves_stalls_over_whip_and_over_srt` drives a WHIP ingress
  (recorded `webrtc-stats`) and an SRT listener ingress through the same
  sequence on a `Playing` flow: flowing, the source leaves, `idle` for two
  polls, `stalled` on the third, `flowing` when media comes back. `README.md`
  ("Strom adapter and drift policy") says so. `rist_condition` keeps its old
  rule, stalled only with the SRT side connected. Unit tests only; the
  Unchecked question about `Paused` is still open.
