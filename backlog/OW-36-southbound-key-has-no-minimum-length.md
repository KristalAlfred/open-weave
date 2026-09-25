---
id: OW-36
title: "WEAVE_SOUTHBOUND_KEY has no minimum length"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`NodeKey::new` in `crates/core/src/auth.rs` accepts any key that is not blank,
and the bench default, `bench-southbound-key`, is 20 characters. A node token is
`<id>.<hex HMAC-SHA256(key, id)>`, so one leaked token, such as a browser guest's
`#token=` URL (`nodes/browser/`), gives an id and its MAC: enough to test guesses
of a weak key offline. The key then makes a token for every node.
`WEAVE_SRT_KEY_SECRET` must be at least 32 characters (`MIN_SECRET_LEN` in
`crates/controller/src/keys.rs`).

## Done when

- [ ] Southbound and the controller refuse to start with a
      `WEAVE_SOUTHBOUND_KEY` shorter than 32 characters, and say so.
- [ ] `weave node-token` refuses such a key.
- [ ] The keys in `README.md`, `AGENTS.md` and the bench defaults are at least
      32 characters.

## Easy to break

- The bench node tokens in `bench/docker-compose.yml` and `bench/justfile` are
  derived from the default key.

## Log

- 2026-09-25: filed from a security review of OW-3 by claude-security.
