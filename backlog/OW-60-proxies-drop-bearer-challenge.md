---
id: OW-60
title: "Northbound and southbound drop the controller's bearer challenge"
type: bug
status: done
depends_on: []
assignee: claude-security
---

## Evidence

`relay` in `crates/southbound/src/main.rs` copies only `Content-Type` from the
controller's answer, and `relay` in `crates/northbound/src/main.rs` copies
`Content-Type` and `ETag`. A `401` the controller answers therefore reaches the
caller without the `WWW-Authenticate: Bearer` challenge `README.md`
("Authentication") promises. Southbound can accept a token a controller then
refuses, for example when two controllers run with different
`WEAVE_SOUTHBOUND_MIN_EPOCHS` (`README.md`, "Controller failover").

## Done when

- [x] A `401` from the controller keeps its `WWW-Authenticate` header through
      southbound and northbound.

## Log

- 2026-09-26: filed from a read-only review of core, the proxies and the CLI;
  started by claude-security.
- 2026-09-26: both `relay`s copy `Content-Type`, `WWW-Authenticate` and, in
  northbound, `ETag` from the controller's answer. Unit tests
  `a_controller_refusal_keeps_its_bearer_challenge` in each proxy: a stub
  controller answering `auth::unauthorized()` to a token the proxy accepts
  gives the caller `401`, `WWW-Authenticate: Bearer` and code `unauthorized`.
  Both fail without the change. Unit tests only.
