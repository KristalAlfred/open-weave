---
id: OW-3
title: "Every node shares one southbound token"
type: feature
status: done
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

- [x] Each node authenticates as itself.
- [x] Registration, heartbeat and `/nodes/{id}/desired` refuse any other id.

## Easy to break

- A browser page gets its token through the URL fragment (`nodes/browser/`).
- Controller `GET /nodes` accepts either surface token.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: started by claude-auth.
- 2026-09-25: node tokens are `<id>.<hex HMAC-SHA256(WEAVE_SOUTHBOUND_KEY, id)>`;
  southbound and the controller hold the key and stop accepting the shared
  token. Southbound forwards the node's own `Authorization` to the controller.
  `GET /nodes`, `/endpoints` and `/state` accept any node token; controller
  `GET /nodes` also the northbound token. `weave node-token <id>` mints one.
  Per-node revocation filed as OW-28.
- 2026-09-25: unit tests: `cargo test -p weave-core auth` (token equals the
  openssl one-liner, forgeries refused), `-p weave-southbound` (another node's
  id gets 403 on register, heartbeat and desired and is never proxied; the key
  and the old shared token get 401), `-p weave-controller node_auth` (403 on all
  three, a forged registration is not recorded, `GET /nodes` takes the north
  token or a node token).
- 2026-09-25: bench (`bench/`, after OW-17): `just bench up` registered
  strom-node-1..3 with their own tokens; `just bench auth-check` gave 200 for
  node 1's own desired hops, 403 from southbound for node 1's token on node 2's
  register, heartbeat and desired, 403 from the controller directly on node 2's
  heartbeat and desired, 401 for `bench-southbound-token` and for the key;
  `just bench stream-up basic` reached `flowing`. `just bench browser-up`
  registered `browser-bench` with the id taken from its token (no `--node`), and
  `browser-stream` got `browser-return` flowing (`browser-cam` stays `pending`,
  OW-13). A host page from `just bench page 8765 guest-1` registered as
  guest-1; the same token with `#node=guest-2` showed "the token belongs to node
  guest-1, not guest-2" and never registered; guest-3's token without `#node`
  registered as guest-3.
