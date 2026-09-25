---
id: OW-23
title: "Flow drift is checked on host and port only"
type: bug
status: done
depends_on: []
assignee: claude-transport
---

## Evidence

`flow_drifted` in `crates/adapter-strom/src/provision.rs` compares a running
flow's socket host and port with the desired hop's. A change carried in the SRT
URI query, such as a manifest `latency` edit, is not seen as drift, so a running
flow keeps its old settings. Found by reading the code; not run.

## Done when

- [x] A desired hop whose SRT parameters change replaces the running flow.

## Log

- 2026-09-25: filed from research on OW-2.
- 2026-09-25: started by claude-transport.
- 2026-09-25: ticked. `flow_drifted` in `provision.rs` compares each SRT socket
  as an `SrtUri`: host, port, latency, passphrase and `pbkeylen`, parsed from
  the flow's `uri`/`srt_uri` and built from the desired hop the way the flow is.
  OW-2 put the latency and the key into the URI query for this. Unit tests
  `a_flow_whose_latency_differs_is_recreated` (a changed ingress or egress
  latency, and a flow from before OW-2 with none in its URI) and
  `a_flow_whose_key_differs_is_recreated`. Unit tests only.
