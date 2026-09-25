---
id: OW-67
title: "Northbound and southbound wait forever on a controller that never answers"
type: bug
status: done
depends_on: [OW-9]
assignee: claude-ha
---

## Evidence

`Controllers` (`crates/core/src/upstream.rs`) set a connect timeout and no
other. A controller that accepts connections but never answers, because it is
paused, stopped with SIGSTOP or deadlocked, held every request sent to it. Each
request goes first to the controller that answered last, so once the leader
froze, every northbound and southbound request hung, also after the other
controller took the lease. README "Controller failover" said a timeout after
connecting was passed back, but there was none. Found in a review of the
OW-9 code, and checked there with a probe.

## Done when

- [x] A test shows whether a request to a controller that never answers
      returns.
- [x] If it does not, a `GET` moves on to the next controller and a write gets
      an answer without being sent again.

## Log

- 2026-09-26: filed from a review of OW-9, and fixed by claude-ha. A probe test
  listed a listener that never accepts first and a stub controller second: a
  `GET` was still waiting after 3 s. Now every request has ten seconds in all
  (`REQUEST_TIMEOUT`). A `GET` that times out or fails after connecting moves on
  to the next controller. Any other request is not sent again and gets
  `504 controller_timeout`, a new error code, and the next request goes to
  another controller first. `a_get_moves_past_a_controller_that_never_answers`
  and `a_write_that_times_out_is_not_sent_again` use a 300 ms timeout. A body
  that fails half way is now an error; it was passed back empty. Unit tests
  only; not run with `docker pause` on the bench.
