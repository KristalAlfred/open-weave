---
id: OW-40
title: "A source that leaves reads differently over WHIP and over SRT"
type: bug
status: todo
depends_on: []
assignee:
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

- [ ] A source that leaves gives the same stream status over WHIP and over SRT,
      and `README.md` says which.

## Unchecked

- Whether a Strom SRT listener flow goes back to `Paused` when its producer
  leaves. If it does, the stall is suppressed and SRT already reads
  `awaiting_input`.

## Log

- 2026-09-25: filed by claude-webrtc from OW-16.
