---
id: OW-48
title: "The Strom adapter reports no hops after a failed desired fetch"
type: bug
status: done
depends_on: []
assignee: claude-tests
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

- [x] A Strom adapter that cannot fetch its desired hops still reports the hops
      it runs, or reports nothing the controller takes as "no hops".
- [x] A test shows a relay that misses one desired fetch keeps its bridge.

## Unchecked

- What the browser node reports after a failed desired poll.

## Log

- 2026-09-25: filed by claude-ha from OW-39 and OW-44.
- 2026-09-25: started by claude-tests.
- 2026-09-25: wire contract. `NodeHeartbeat.hop_status` and
  `NodeRegistration.hop_status` are `#[serde(default)]` lists, and
  `node_heartbeat` in `crates/controller/src/main.rs` replaces the stored
  report with whatever arrives. So omitting the field reads the same as an
  empty list, and "keep the previous report" would need a wire change. The fix
  reports the hops instead. `PROTOCOL_VERSION` stays 5.
- 2026-09-25: fix in `crates/adapter-strom/src/main.rs`. `sync_loop` keeps the
  last desired hops it fetched. When the desired fetch fails, `hop_status`
  provisions nothing and reports those hops as Strom shows them now (the same
  `hop_statuses` a normal poll uses, so a running flow reads `provisioned` with
  its conditions). When Strom cannot be listed, it reports them `pending` with
  idle sockets. A `pending` report keeps a relay's bridge (OW-42) while the
  stream reads `pending`, so nothing claims media flows that Strom cannot
  confirm. Before the first successful fetch there is nothing to report, and the
  list is empty as before. `README.md` ("HTTP API", the desired-hops
  paragraph) updated.
- 2026-09-25: boxes ticked on unit tests.
  `a_missed_desired_fetch_or_strom_listing_still_reports_the_last_desired_hops`
  (adapter) checks the empty report before any fetch, the `provisioned` report
  with its branch after a failed fetch (with no flow touched), and the
  `pending` report with Strom unlisted.
  `a_relay_that_misses_one_desired_fetch_keeps_its_bridge` (controller) puts a
  bridge on `relay-b`, registers the lower `relay-a`, then sends `relay-b`
  heartbeats. With the report the adapter now sends, the bridge stays on
  `relay-b`. With the empty report the old adapter sent, it moves to
  `relay-a`.
- 2026-09-25: Unchecked question: `nodes/browser/node.js` does not have this
  bug. `heartbeat` reports `hopStatuses()`, which reads the page's `hops` map.
  `pollDesired` only changes that map through `reconcile` after a `2xx`, and on
  a failed poll it logs and keeps observing the hops it has. Read, not run.
