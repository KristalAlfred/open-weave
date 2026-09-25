---
id: OW-63
title: "The adapter restarts an SRT caller that is connected but carries nothing"
type: bug
status: done
depends_on: []
assignee: claude-transport
---

## Evidence

OW-22 made the Strom adapter stop and start a running flow whose SRT caller
ingress Strom reports `connected: false` for six polls. Strom's `connected`
means the socket has carried data, not that the handshake finished:
`caller_is_active` in strom `backend/src/gst/pipeline/srt.rs` (v0.6.6 and
v0.6.8) is `packets_sent > 0 || packets_received > 0 || bytes > 0`. So a caller
whose peer has nothing to send reads unconnected, and its flow is restarted
every ~30s, dropping whatever else the hop carries: `nat-ingress` with no
producer, relay bridges, pulls from an idle remote listener. The OW-22 bench run
had the producer running throughout. Found by a read-only review of the adapter
changes.

On `bench/` (Strom 0.6.10), `nat-ingress` with the consumer attached and no
producer: the receiver's `srtsrc_0` (mode `caller`) read `connected: false`
with `rtt_ms: 100`, `negotiated_latency_ms: 1000`, `bandwidth_mbps: 12` and zero
packets. A caller its listener refused, on a throwaway Strom 0.6.6 container,
read `rtt_ms`, `negotiated_latency_ms` and `bandwidth_mbps` null.

## Done when

- [x] A caller whose handshake finished but carries no media is not restarted,
      shown on the bench.
- [x] Restarts that are still needed back off per hop.
- [x] README says what a restart drops.

## Log

- 2026-09-26: filed from a read-only review of the adapter changes, and started
  by claude-transport.
- 2026-09-26: the adapter now counts a caller as waiting only while no peer of
  its `srtsrc` reports a `negotiated_latency_ms` and Strom's `connected` is
  false (`ElementStats::handshaken` in `crates/strom/src/stats.rs`). The first
  restart comes after 6 polls without a handshake, each later one after twice
  as many, up to 96 polls (~8 minutes); a handshake resets both.
- 2026-09-26: ticked "not restarted", on `bench/` (Strom 0.6.10, images built
  after touching every `.rs` file for OW-66). `nat-ingress` applied with only
  the consumer attached: for 3 minutes, sampled every 15s, the receiver flow on
  strom-3 kept its `started_at`, its `srtsrc_0` read `connected: false` with
  `negotiated_latency_ms: 1000`, the consumer stayed on the same address
  (`10.97.29.31:43442`) with no retry in its log, and adapter-3 logged no
  restart. The same stats would have counted toward a restart under the OW-22
  rule, which read only `connected`; the old image was not run again. Then the
  OW-22 scenario, producer running: with adapter-1 stopped and the key secret
  changed, strom-3 logged 6 refusals. On 0.6.10 the refused flow stopped
  running, the adapter's usual start retry brought it back, and the stream read
  `flowing` 21s after adapter-1 returned, with no restart needed.
- 2026-09-26: ticked "back off", with unit tests only:
  `a_caller_without_a_handshake_is_restarted_with_backoff` (restarts at polls 6,
  18, 42, 90, 186, 282, 378; a handshake resets it),
  `a_caller_that_shook_hands_but_carries_nothing_is_never_restarted`,
  `a_caller_that_shook_hands_but_carries_nothing_is_not_restarted` in the
  adapter's `main.rs`, and `a_negotiated_latency_marks_a_finished_handshake` in
  `stats.rs`.
- 2026-09-26: ticked "README": "Strom adapter and drift policy" says a restart
  stops the whole flow, so a consumer on its output and the peers of its egress
  callers lose their connection and dial again.
