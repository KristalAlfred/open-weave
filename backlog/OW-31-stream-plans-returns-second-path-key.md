---
id: OW-31
title: "POST /stream-plans returns the key of a second path"
type: bug
status: done
depends_on: []
assignee: claude-security
---

## Evidence

`withhold_passphrases` in `crates/controller/src/main.rs` clears the passphrase
on each hop's `ingress` and `egresses` and leaves `merge_ingress` alone.
`place_second_path` in `crates/controller/src/path.rs` sets `merge_ingress` to the
downstream socket `plan_link` returns, which carries the derived link key. A
security review's probe planned a stream with `paths: 2` to a merging studio
node and got
`"merge_ingress":{...,"passphrase":"…","pbkeylen":32}` from `POST /stream-plans`,
equal to `keys.link("weave-feed-receiver-studio.2")`. Northbound passes the plan
through, and `README.md` ("SRT encryption") says derived keys never reach
northbound. `key_exposure_tests` plans only `paths: 1`.

## Done when

- [x] `POST /stream-plans` returns no passphrase on any socket of any hop,
      `merge_ingress` included.
- [x] `key_exposure_tests` covers a `paths: 2` stream on every route it checks.

## Log

- 2026-09-25: filed from a security review of OW-2 and OW-3; started by
  claude-security.
- 2026-09-25: `DesiredHop::sockets` and `sockets_mut` in `crates/core/src/lib.rs`
  list the ingress, the merge ingress and every egress, destructuring the hop so
  a new field does not compile until it is placed. `withhold_passphrases` clears
  every socket they list. `key_exposure_tests` now plans `paths: 1` and
  `paths: 2` between two dual-homed nodes, reads every key from both nodes'
  desired hops through `sockets`, checks both ends carry each link key, and
  finds none of them in `/view`, `/status`, `GET /streams`, `/streams/feed`,
  `/streams/feed/endpoints`, `POST /stream-plans` (whose hops, `merge_ingress`
  included, carry no passphrase) or any webhook event the controller sent.
  With the old redaction the `paths: 2` case fails on the plan's merge ingress.
  `/view` shows no `merge_ingress`, and webhook events and `/status` carry no
  sockets. Unit tests only.
