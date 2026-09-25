---
id: OW-24
title: "bench/README.md and the code disagree on a stream whose node went offline"
type: bug
status: done
depends_on: []
assignee: lead
---

## Evidence

`bench/README.md` says a stream "stays `pending`" when a node it names goes
offline. The controller reports it `degraded` (`crates/controller/src/main.rs`,
stream status from destination conditions). Which of the two is intended is not
written down anywhere found.

## Done when

- [x] The intended status is decided and recorded in `README.md`.
- [x] The code and `bench/README.md` agree with it.

## Log

- 2026-09-25: filed from research on OW-8.
- 2026-09-25: the user decided that a placed stream whose named node goes
  offline reads `degraded`, which is what the controller already does (the
  `offline_node` check in `crates/controller/src/main.rs`). `README.md`, "Hop
  status and fan-out", now says so, and `bench/README.md`'s troubleshooting no
  longer lists an offline node as a cause of `pending`. No code change.
