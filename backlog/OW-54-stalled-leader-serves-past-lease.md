---
id: OW-54
title: "A leader resumed after a pause could serve before it notices the lease lapsed"
type: bug
status: done
depends_on: [OW-9]
assignee: claude-ha
---

## Evidence

`Leadership::router` (`crates/controller/src/main.rs`) served the leader's
router until `hold_lease` stood it by. A process paused past the end of its
lease, by SIGSTOP or a paused VM, resumes with both the renewal task and any
queued requests ready to run, and nothing decided which ran first. A request
that ran first was answered from the old leader's memory, such as a desired
hops list, while another controller already led. Writes were fenced in
Postgres. Found in a review of the OW-9 code.

## Done when

- [x] A test shows whether a request can be served after the leader's hold
      deadline when the renewal task has not run.
- [x] If it can, it gets `503 not_leader`.

## Log

- 2026-09-26: filed from a review of OW-9, and fixed by claude-ha. A probe ran
  two `weave-controller` binaries on one Postgres with a 3 s lease, stopped the
  leader with SIGSTOP until the other took over, queued `GET /status` to the
  stopped one and resumed it. On the code before the fix the queued request got
  `503 not_leader` in 13 runs of 13: the renewal task ran first each time. That
  order is up to the runtime, so the fix does not rely on it. `Leadership` now
  keeps the hold deadline, which `hold_lease` moves on each renewal, and serves
  nothing past it. `a_leader_serves_nothing_past_its_hold_deadline_without_a_renewal`
  leads with a 200 ms deadline and no renewal task and gets `503 not_leader`
  after it. Unit tests, plus the probe on this host.
