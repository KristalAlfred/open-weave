---
id: OW-48
title: "The Strom adapter reports no hops after a failed desired fetch"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`sync_once` in `crates/adapter-strom/src/main.rs` builds `hop_status` from
`provision`, and when `provision` fails, which includes any failure to fetch
the desired hops, it reports `Vec::new()`. The heartbeat or registration sent
next carries that empty list while the node's Strom flows keep running. The
same happens when Strom itself cannot be listed.

The controller plans from these reports. `derive_stream`
(`crates/controller/src/path.rs`) keeps a bridge on the relay whose report says
it runs it (OW-42), so a relay whose adapter misses one desired fetch and still
gets its heartbeat through reports no bridge, and the next tick can give the
bridge to the lowest-id relay. Stream conditions read the same reports, so the
stream reads `pending` for that poll. Since OW-44 the controller also stores
the empty report, so a restart or takeover right after reads it too.

Found by reading the code while working OW-39 and OW-44. On `bench/` (OW-9) the
adapters' failed fetches came with failed heartbeats, and the registration that
landed afterwards carried real hop status, so this was not seen there.

## Done when

- [ ] A Strom adapter that cannot fetch its desired hops still reports the hops
      it runs, or reports nothing the controller takes as "no hops".
- [ ] A test shows a relay that misses one desired fetch keeps its bridge.

## Unchecked

- What the browser node reports after a failed desired poll.

## Log

- 2026-09-25: filed by claude-ha from OW-39 and OW-44.
