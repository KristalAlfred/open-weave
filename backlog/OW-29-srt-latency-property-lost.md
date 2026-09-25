---
id: OW-29
title: "SRT latency set as an element property is lost on raw-element flows"
type: bug
status: done
depends_on: []
assignee: claude-transport
---

## Evidence

Before OW-2, `src_props` and `sink_props` in `crates/strom/src/spec.rs` set a
raw `srtsrc`/`srtsink` element's latency as a `latency` property beside `uri`.
Setting an srt element's `uri` resets its latency, and Strom sets element
properties in `HashMap` order (strom `backend/src/gst/pipeline/construction.rs`),
so the property only survives when Strom happens to set it last.

On a throwaway `eyevinntechnology/strom:latest` (0.6.6, GStreamer 1.26.6), not
the bench: 21 raw-element flows created through `POST /api/flows` with
`latency: 2000` or `1500` as a property. Reading the live element back through
`GET /api/flows/{id}/elements/{element}/properties` gave libsrt's default `125`
in 11 of them, on both `srtsrc` and `srtsink`. `keep-listening`, `auto-reconnect`
and `wait-for-connection` were kept in every flow. With `latency=` in the URI
query, 7 of 7 flows read back the latency asked for.

Strom's `mpegtssrt_input` and `mpegtssrt_output` blocks set `uri` before
`latency` in code (`mpegtssrt_input.rs:178-179`, `mpegtssrt.rs:260-261`), so
block flows are not affected.

## Done when

- [x] Raw-element SRT flows run with the latency their desired hop carries,
      read back from the live element on the bench.

## Log

- 2026-09-25: filed from work on OW-2 and started by claude-transport. OW-2
  moves the latency into the `srt://` URI query with the key, which is the fix;
  the bench check is pending.
- 2026-09-25: ticked, on `bench/`. With `basic` and `srt-latency` flowing,
  `GET /api/flows/{id}/elements/{element}/properties` on strom-1 and strom-2
  read back the latency in each element's URI for all 8 `srtsrc`/`srtsink`
  elements: 200/1000 and 1000/200 for `basic`, 120/2000 and 2000/200 for
  `srt-latency`.
