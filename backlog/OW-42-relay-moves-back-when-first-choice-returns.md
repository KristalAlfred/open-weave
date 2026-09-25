---
id: OW-42
title: "A relayed stream moves back to its first relay as soon as it returns"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`pick_relay` (`crates/controller/src/path.rs`) takes the first online node, in
node order, that can bridge both ends. Nothing prefers the relay a stream is
already using. On the bench on 2026-09-25, with `nat-transit` flowing through
strom-node-1, stopping adapter-1 moved the bridge to strom-node-2 and the stream
was `flowing` again in about 30 s. Starting adapter-1 again moved the bridge
back to strom-node-1 within about 20 s, and node 2's flow was removed. Each move
interrupted media, including the second one, where the path through node 2 was
working.

## Done when

- [ ] A stream whose relay is online and working keeps it when a node earlier
      in the order comes back.
- [ ] A stream still moves off a relay that goes offline.

## Easy to break

- Planning is side-effect free and allocates against the full candidate stream
  set (`BACKLOG.md`, "Scope guards"). Preferring the current relay needs the
  current placement as an input to planning, not state the planner keeps.
- A `via` pin still wins over any preference.

## Log

- 2026-09-25: filed from the OW-6 bench run.
