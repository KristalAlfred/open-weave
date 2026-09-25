---
id: OW-36
title: "WEAVE_SOUTHBOUND_KEY has no minimum length"
type: bug
status: done
depends_on: []
assignee: claude-security
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

- [x] Southbound and the controller refuse to start with a
      `WEAVE_SOUTHBOUND_KEY` shorter than 32 characters, and say so.
- [x] `weave node-token` refuses such a key.
- [x] The keys in `README.md`, `AGENTS.md` and the bench defaults are at least
      32 characters.

## Easy to break

- The bench node tokens in `bench/docker-compose.yml` and `bench/justfile` are
  derived from the default key.

## Log

- 2026-09-25: filed from a security review of OW-3 by claude-security.
- 2026-09-25: started by claude-security.
- 2026-09-25: `NodeKey::new` in `crates/core/src/auth.rs` refuses a key shorter
  than `MIN_NODE_KEY_LEN` (32) after trimming, and `NodeGuard::from_env` returns
  `AuthError::ShortKey`. With `WEAVE_SOUTHBOUND_KEY=bench-southbound-key` the
  built `weave-southbound` and `weave-controller` exited 1 with
  "WEAVE_SOUTHBOUND_KEY must be at least 32 characters, for example `openssl
  rand -hex 32`", and `weave node-token` exited 1 naming the length. Unit tests:
  `blank_or_short_keys_are_refused` and
  `short_key_error_names_the_variable_and_the_length` in `weave-core`,
  `node_token_is_the_node_id_and_its_mac` in `weave-cli`. The test keys in
  southbound and the controller are now 32 characters or more.
- 2026-09-25: the bench key is now `bench-southbound-key-for-local-use-only`,
  and the five node tokens in `bench/docker-compose.yml` and `bench/justfile`
  equal `weave node-token <id> --key` under it. `README.md` states the minimum;
  `AGENTS.md` already used `openssl rand -hex 32`. Not run on `bench/`.
