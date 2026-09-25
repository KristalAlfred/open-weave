---
id: OW-41
title: "RIST hops carry no encryption"
type: feature
status: todo
depends_on: []
assignee:
---

## Evidence

OW-2 keys every SRT link between nodes. A link the planner puts on RIST
(OW-11) carries no key: `RistSocket` in `crates/core/src/lib.rs` has no
parameters, and the Strom adapter builds RIST hops from GStreamer's `ristsrc`
and `ristsink`, which have no passphrase or PSK property (`gst-inspect-1.0` in
`eyevinntechnology/strom:latest`, gst-plugins-bad 1.26.5). RIST main profile
defines encryption; the simple profile these elements implement does not.

## Done when

- [ ] A RIST link between nodes is encrypted, or the planner refuses to put a
      link on RIST unless the stream allows it in the clear.

## Unchecked

- Whether any GStreamer or Strom element a node could use speaks RIST main
  profile with a PSK.

## Log

- 2026-09-25: filed from work on OW-11.
