---
id: OW-28
title: "One node's token cannot be revoked on its own"
type: feature
status: done
depends_on: [OW-3]
assignee: claude-security
---

## Evidence

A node token is derived from `WEAVE_SOUTHBOUND_KEY` and the node id
(`crates/core/src/auth.rs`, `NodeKey`), and southbound and the controller keep
no list of issued tokens. A token stays valid for as long as the key does. The
only way to shut out one node that leaked its token, or left, is to rotate the
key, which invalidates every node's token and means re-provisioning all of them.
`README.md`, "Authentication", says so.

## Done when

- [x] One node's token can be made invalid without changing any other node's.

## Log

- 2026-09-25: filed from OW-3, which left revocation out of scope.
- 2026-09-25: started by claude-security.
- 2026-09-25: a node token is now `<id>.<epoch>.<hex HMAC-SHA256(key,
  "<id>.<epoch>")>`, with the epoch a non-negative integer written without sign
  or leading zeros. The earlier `<id>.<mac>` form is not accepted: one form keeps
  one parser, and every existing token has to be re-minted, at epoch 0 for the
  same rights. `WEAVE_SOUTHBOUND_MIN_EPOCHS` (`MinEpochs` in
  `crates/core/src/auth.rs`), read by southbound and the controller through
  `NodeGuard::from_env`, holds `<id>=<epoch>` pairs; `NodeKey::verify_header`
  refuses a token below its node's minimum, so it gets `401` as a bad MAC does.
  A value that does not parse stops both services at startup.
  `weave node-token <id> --epoch <n>` mints a later token; the browser page
  still reads its node id off the part before the first `.`.
- 2026-09-25: unit tests: `a_token_below_its_nodes_minimum_epoch_is_refused`
  (with `strom-node-1=2`, epochs 0 and 1 refused, 2 and 3 accepted, strom-node-2
  at 0 accepted), `min_epochs_parse_pairs_and_refuse_anything_else` and
  `node_token_rejects_forgeries` (another epoch's MAC, a leading zero, a sign, a
  negative epoch, no epoch) in `weave-core`;
  `a_token_below_its_nodes_minimum_epoch_gets_401_and_others_pass` in the
  controller's `node_auth_tests`; `node_token_is_the_node_id_its_epoch_and_their_mac`
  in `weave-cli`. The README openssl one-liner matched `weave node-token` at
  epochs 0 and 3 under a random key. Locally, not on `bench/`: the built
  controller and southbound with `WEAVE_SOUTHBOUND_MIN_EPOCHS=strom-node-1=1`
  gave 401 for strom-node-1's epoch-0 token on `POST /nodes/register` through
  each, 202 for its epoch-1 token and 202 for strom-node-2's epoch-0 token; with
  `strom-node-1=two` or a node named twice each exited 1 naming the variable.
  The bench tokens are re-minted at epoch 0 and match `weave node-token`, and
  compose passes `WEAVE_SOUTHBOUND_MIN_EPOCHS` to southbound and the controller.
  Not run on `bench/`.
