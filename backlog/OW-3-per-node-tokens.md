---
id: OW-3
title: "Every node shares one southbound token"
type: feature
status: in-progress
depends_on: []
assignee: claude-auth
---

## Evidence

One `WEAVE_SOUTHBOUND_TOKEN` covers every adapter and browser page
(`README.md`, "Authentication"), so any node can register as, or read the desired
hops of, any other. Distribution sends feeds to nodes run by other
organisations: Eurovision to national broadcasters, PBS to more than 330 member
stations
([TV Tech, 2026](https://www.tvtechnology.com/infrastructure/ip-networking/pbs-selects-ltn-to-power-nationwide-ip-video-network)).

## Done when

- [ ] Each node authenticates as itself.
- [ ] Registration, heartbeat and `/nodes/{id}/desired` refuse any other id.

## Easy to break

- A browser page gets its token through the URL fragment (`nodes/browser/`).
- Controller `GET /nodes` accepts either surface token.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: started by claude-auth.
