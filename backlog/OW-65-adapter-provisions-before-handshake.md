---
id: OW-65
title: "The Strom adapter provisions before the version handshake"
type: bug
status: done
depends_on: []
assignee: claude-transport
---

## Evidence

`sync_once` in `crates/adapter-strom/src/main.rs` fetched desired hops and
reconciled them into Strom flows, creating and deleting flows, before it called
`register_node`. An adapter at another `PROTOCOL_VERSION` against a
Postgres-backed controller that still knows the node would get its desired
hops, change its Strom flows, and only then get `409` and exit. README
("Protocol version negotiation") says the version is checked at registration
so a stale process is rejected before it acts. Found by a read-only review of
the adapter changes; not run.

## Done when

- [x] The adapter creates and deletes no flow before its first registration is
      accepted.

## Log

- 2026-09-26: filed from a read-only review of the adapter changes, and started
  by claude-transport.
- 2026-09-26: ticked. Until a registration succeeds, `sync_once` registers
  with the status of the flows it already has, read without provisioning, and
  provisions from the next poll. Unit test
  `a_refused_first_registration_leaves_every_flow_alone`: a southbound that
  answers registration `409` and desired hops `[]` leaves a managed flow the
  old order would have deleted, and `sync_once` returns the rejection. Unit
  tests only.
