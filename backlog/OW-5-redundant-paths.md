---
id: OW-5
title: "One destination has one path"
type: feature
status: todo
priority: 2
depends_on: []
assignee:
branch:
pr:
---

## Evidence

Nothing plans a second copy of a destination; a grep for redundancy, 2022-7 and
failover in `crates/` finds nothing. Broadcasters send the same feed over two
routes and merge them at the receiver: Eurovision sends SRT over the internet as
two streams combined with SMPTE 2022-7
([Panorama, 2026](https://www.panoramaaudiovisual.com/en/2026/01/22/nuevas-necesidades-distribucion-grandes-eventos-deportivos-eurovision-services/)).

Merging is the receiving node's job (2022-7, libsrt socket groups). Choosing two
paths that share no relay or network is routing.

## Done when

- [ ] A destination can ask for two paths.
- [ ] The planner places them over disjoint relays and attachments when the
      topology allows.
- [ ] A hop profile declares that the receiver can merge.
- [ ] A stream that gets only one path reports it.

## Easy to break

- Hop ids and ports are stable across manifest edits (`README.md`, "Hop status
  and fan-out"). The second path needs ids of its own without renumbering the
  first.

## Log

- 2026-09-25: filed from broadcaster research.
