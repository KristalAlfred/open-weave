---
id: OW-22
title: "An srtsrc caller refused once never reconnects"
type: bug
status: done
depends_on: []
assignee: claude-transport
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

- [x] A caller refused by its listener reconnects once the listener accepts it,
      shown on the bench.

## Log

- 2026-09-25: filed from research on OW-2.
- 2026-09-25: started by claude-transport.
- 2026-09-25: reproduced on `bench/` with `nat-ingress`, whose receiver on
  strom-node-3 dials the sender on strom-node-1. With adapter-1 stopped, the
  controller was restarted with another `WEAVE_SRT_KEY_SECRET`; adapter-3
  rebuilt the receiver with the new key and strom-3 logged `Failed to
  authenticate: Incorrect passphrase (10)`. adapter-1 was started ~40s later and
  rebuilt the sender with the same new key. For the next 2.5 minutes the
  receiver flow read `running: true`, `gst_state: Paused`, `srtsrc_0`
  `connected: false`, and the stream `degraded`. On a throwaway Strom container
  (not the bench), a refused `srtsrc` caller flow stayed unconnected 15s after
  its listener was restarted with the matching key; `POST
  /api/flows/{id}/stop` then `/start` connected it (`Playing`). Strom's flow JSON
  showed nothing that tells this flow from one whose caller is still waiting for
  a listener that does not exist yet.
- 2026-09-25: the Strom adapter now stops and starts a running flow whose SRT
  caller ingress Strom reports unconnected (`connected: false` in `srt-stats`)
  for six polls in a row (~30s at the default poll), then counts again
  (`redial_unconnected_callers` in `crates/adapter-strom/src/main.rs`). A caller
  still waiting for a listener that is not up yet is restarted on the same
  cadence, which only makes it dial again. Unit tests
  `an_unconnected_caller_is_due_a_restart_every_redial_polls` and
  `a_caller_ingress_left_unconnected_is_restarted_and_a_listener_is_not`.
- 2026-09-25: ticked, on `bench/`, with the reproduction above. A first try
  read `connections > 0` and restarted nothing: a refused caller keeps a stale
  entry in `callers`, so it counts one connection. Reading Strom's `connected`
  flag instead: strom-3 logged two refusals while adapter-1 was stopped,
  adapter-3 restarted the receiver at 12:34:51 and again at 12:35:21, and the
  stream read `flowing` from 12:35:28, 35s after adapter-1 came back and
  rebuilt the sender with the new key.
