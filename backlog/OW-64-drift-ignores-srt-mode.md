---
id: OW-64
title: "Flow drift ignores the SRT mode"
type: bug
status: done
depends_on: []
assignee: claude-transport
---

## Evidence

`SrtUri::parse` in `crates/strom/src/spec.rs` read host, port, latency and the
key from a flow's `uri`/`srt_uri`, and dropped `mode`. `flow_drifted` in
`crates/adapter-strom/src/provision.rs` compares those parsed URIs, so a flow
whose `mode` was edited in Strom's UI, for example to `rendezvous`, matched its
desired hop and was adopted as it was. AGENTS.md: "Strom edits made outside
open-weave are drift". Found by a read-only review of the adapter changes.

## Done when

- [x] A flow whose SRT `mode` differs from the one its desired hop builds is
      deleted and created again.

## Unchecked

- Element properties outside the URI (`keep-listening`, `wait-for-connection`)
  are still not compared, so an outside edit to them is adopted.

## Log

- 2026-09-26: filed from a read-only review of the adapter changes, and started
  by claude-transport.
- 2026-09-26: ticked. `SrtUri` carries `mode` from the query, built as
  `listener` or `caller`, and `flow_drifted` compares it with the rest of the
  URI. Unit test `a_flow_whose_srt_mode_was_edited_is_recreated` in
  `provision.rs`: `rendezvous` on either socket, and a URI with no `mode`, are
  recreated; the existing `matching_flow_uris_are_adopted_not_recreated` still
  adopts. Unit tests only.
