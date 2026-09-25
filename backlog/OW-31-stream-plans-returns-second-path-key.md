---
id: OW-31
title: "POST /stream-plans returns the key of a second path"
type: bug
status: in-progress
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

- [ ] `POST /stream-plans` returns no passphrase on any socket of any hop,
      `merge_ingress` included.
- [ ] `key_exposure_tests` covers a `paths: 2` stream on every route it checks.

## Log

- 2026-09-25: filed from a security review of OW-2 and OW-3; started by
  claude-security.
