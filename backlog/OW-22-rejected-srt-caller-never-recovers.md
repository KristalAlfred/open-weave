---
id: OW-22
title: "An srtsrc caller refused once never reconnects"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

On a throwaway `eyevinntechnology/strom:latest` (image from 2026-06-26), an
`srtsrc` caller that a listener refused for a wrong passphrase stayed dead after
the key was corrected on the listener, while Strom kept reporting the flow
`running: true`. An `srtsink` caller in the same position reconnected. The
adapter's stall detection only acts on a hop that has flowed before
(`crates/adapter-strom/src/provision.rs`), so nothing restarts it. This is the
same shape as EOS-dead flows reporting healthy.

It bites whenever two ends of a keyed link are provisioned at different polls:
key rollout and rotation, and reversed links where the receiver dials.

## Done when

- [ ] A caller refused by its listener reconnects once the listener accepts it,
      shown on the bench.

## Log

- 2026-09-25: filed from research on OW-2.
