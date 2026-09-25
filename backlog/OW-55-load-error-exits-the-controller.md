---
id: OW-55
title: "A load error after taking the lease exits the controller"
type: bug
status: done
depends_on: [OW-9]
assignee: claude-ha
---

## Evidence

`run_with_lease` (`crates/controller/src/main.rs`) released the lease and
returned the error when loading state after taking the lease failed, so the
process exited. A load that fails for a moment, such as a Postgres restart
between taking the lease and reading the streams, took a controller out
instead of letting it stand by and try again. With Postgres flapping, each
controller could take the lease, fail, and exit. Found in a review of the OW-9
code.

## Done when

- [x] A test shows whether a controller keeps running after a failed load.
- [x] If it does not, it stands by and takes the lease again once the load
      works.

## Log

- 2026-09-26: filed from a review of OW-9, and fixed by claude-ha.
  `a_controller_that_cannot_load_its_state_stands_by_and_tries_again`
  (ignored, Postgres) stores a stream whose name fails validation, starts a
  controller, and checks that it is still running 3 s later; before the fix
  it had returned. Now it gives the lease up, logs the error, waits one retry
  interval and stands by again. Once the row is fixed it leads and serves the
  stream. Run against `postgres:16` in docker. README "Manifest validation"
  no longer says the controller refuses to start.
