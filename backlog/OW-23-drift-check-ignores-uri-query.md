---
id: OW-23
title: "Flow drift is checked on host and port only"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`flow_drifted` in `crates/adapter-strom/src/provision.rs` compares a running
flow's socket host and port with the desired hop's. A change carried in the SRT
URI query, such as a manifest `latency` edit, is not seen as drift, so a running
flow keeps its old settings. Found by reading the code; not run.

## Done when

- [ ] A desired hop whose SRT parameters change replaces the running flow.

## Log

- 2026-09-25: filed from research on OW-2.
