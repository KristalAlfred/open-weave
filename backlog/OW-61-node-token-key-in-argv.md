---
id: OW-61
title: "weave node-token --key puts the southbound key in argv"
type: bug
status: done
depends_on: []
assignee: claude-security
---

## Evidence

`weave node-token` takes the southbound key as `--key` (`NodeToken` in
`crates/cli/src/main.rs`), and `README.md` ("Authentication") offers `--key` as
the alternative to `WEAVE_SOUTHBOUND_KEY`. A key given that way sits in the
process's arguments, where `ps` shows it to every user on the host, and in the
shell's history. The one key makes every node's token.

## Done when

- [x] `weave node-token` reads the key only from `WEAVE_SOUTHBOUND_KEY`, and
      `--key` is refused.

## Log

- 2026-09-26: filed from a read-only review of core, the proxies and the CLI;
  started by claude-security.
- 2026-09-26: `NodeToken` has no `--key`; the command reads
  `WEAVE_SOUTHBOUND_KEY` from the environment and says to set it when it is
  missing. No `--key-stdin`: the environment variable already keeps the key out
  of argv, and the bench recipe `just bench node-token` sets it that way. Unit
  test `node_token_takes_no_key_on_its_command_line` in `weave-cli` (`--key`
  fails to parse, `--epoch 2` parses); `node_token_is_the_node_id_its_epoch_and_their_mac`
  still passes. `README.md` drops `--key` and notes that the openssl one-liner
  passes the key on openssl's command line. Unit tests only.
