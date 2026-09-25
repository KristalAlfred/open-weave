---
id: OW-24
title: "bench/README.md and the code disagree on a stream whose node went offline"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`bench/README.md` says a stream "stays `pending`" when a node it names goes
offline. The controller reports it `degraded` (`crates/controller/src/main.rs`,
stream status from destination conditions). Which of the two is intended is not
written down anywhere found.

## Done when

- [ ] The intended status is decided and recorded in `README.md`.
- [ ] The code and `bench/README.md` agree with it.

## Log

- 2026-09-25: filed from research on OW-8.
