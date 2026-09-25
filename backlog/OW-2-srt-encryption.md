---
id: OW-2
title: "SRT hops carry no encryption"
type: feature
status: todo
priority: 1
depends_on: []
assignee:
branch:
pr:
---

## Evidence

Nothing in `crates/` sets an SRT passphrase or key length, so every planned SRT
hop runs in the clear. Contribution and distribution over the public internet
are the common case:

- ESPN sent every camera of a 2023 college game over SRT into AWS
  ([SVG](https://www.sportsvideo.org/2023/01/24/espn-dmed-pull-off-first-end-to-end-cloud-based-live-production-in-u-s-with-a-10-college-hoops-game/)).
- Vivid Broadcast produces up to six Women's Super League matches a weekend over
  the public internet
  ([Intinor, 2026, vendor](https://intinor.com/securing-remote-production-for-the-womens-super-league/)).

The controller plans both ends of every hop, so it can give both the same key.

## Done when

- [ ] Each planned SRT hop carries a key, the same in both ends' desired hops.
- [ ] The Strom adapter sets it on both ends.
- [ ] A `remote` destination takes its key from the manifest.
- [ ] A bench caller with the wrong key is refused.

## Easy to break

- The key must stay out of `/view`, `/status`, logs, webhook events and every
  unauthenticated route.
- With one shared southbound token any node can read any node's desired hops, so
  the key is only as private as OW-3 makes it.

## Unchecked

- Whether Strom's SRT blocks expose GStreamer's `passphrase` property.

## Log

- 2026-09-25: filed from broadcaster research.
