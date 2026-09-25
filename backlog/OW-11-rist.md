---
id: OW-11
title: "RIST as a transport"
type: feature
status: todo
priority: 3
depends_on: []
assignee:
branch:
pr:
---

## Evidence

The planner knows SRT, WHIP and WHEP (`TRANSPORT_PREFERENCE` in
`crates/controller/src/path.rs`). RIST is the other standard contribution
protocol (VSF TR-06). AWS MediaConnect offers it beside SRT, and Spalk takes
commentary ingest over SRT, Zixi or RIST
([AWS, 2023](https://aws.amazon.com/blogs/media/remote-sports-commentary-made-easy-with-spalk-and-aws/)).
The evidence is thinner than for SRT: none of the broadcaster cases found named
RIST as their link.

## Done when

- [ ] Hop profiles can declare RIST.
- [ ] The planner resolves which end connects for RIST as it does for SRT.
- [ ] One adapter builds RIST hops on the bench.

## Easy to break

- Where RIST goes in `TRANSPORT_PREFERENCE` decides whether existing streams
  change transport.

## Unchecked

- Whether Strom builds RIST flows.

## Log

- 2026-09-25: filed from broadcaster research.
