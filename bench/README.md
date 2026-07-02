# Network-impairment media test bench

A docker-compose bench that runs a real SRT/MPEG-TS media chain across an
impairable L3 hop between two `strom` nodes. Use it to observe strom's SRT
telemetry (loss, retransmits, RTT, latency) and the downstream media impact
under packet loss / latency / jitter / reorder / blackout.

This is the "real bench first" step: the media plane + impairment + telemetry
surface. open-weave's own Rust binaries are **not** wired in yet — flows are
provisioned directly against strom's HTTP API (`scripts/flows.sh`, a temporary
stand-in for future `weave-adapter-strom` command application).

## Topology

```
        net_ingress 172.30.0.0/24                 net_egress 172.31.0.0/24
  ┌───────────────────────────────┐         ┌───────────────────────────────┐
  │ producer .20                   │         │                   consumer .20 │
  │  ffmpeg testsrc2+TC+tone       │         │  ffmpeg freeze/black/continuity│
  │  H.264+AAC MPEG-TS             │         │        ▲                       │
  │        │ SRT caller            │         │        │ SRT caller            │
  │        ▼ :7001                 │         │        │ :7003                 │
  │ strom-ingress .10  ──srtsink──┐│         │┌── strom-egress .10            │
  │  srtsrc(listen) → queue →     ││         ││  srtsrc(listen :7002) →       │
  │                     srtsink   ││         ││       queue → srtsink(listen) │
  └───────────────────────┼───────┘│         │└───────▲───────────────────────┘
                          │ .2 router .2 │  ← netem qdiscs applied here
                          └──────┼───────┘
                    caller :7002 (crosses the impaired hop)
```

- **producer → strom-ingress**: SRT (producer=caller, srtsrc=listener :7001). Same subnet, not impaired.
- **strom-ingress → strom-egress**: SRT (srtsink=caller → srtsrc=listener :7002). **Crosses the router — this is the impaired contribution hop.** Negotiated latency 1000 ms.
- **strom-egress → consumer**: SRT (srtsink=listener :7003, consumer=caller). Same subnet, not impaired.

Routes to the opposite subnet are added into each strom container's network
namespace by one-shot `route-*` sidecars (`network_mode: service:...`), because
the strom image has no `ip` tool. netem impairment is applied on the router's
two interfaces, resolved by IP so eth0/eth1 ordering doesn't matter.

## Prerequisites

- Docker (compose v2) with the Linux VM able to run `NET_ADMIN` + `sysctl net.ipv4.ip_forward`.
- `just`, `curl`, `jq` on the host.
- Images (pulled on first `up`): `eyevinntechnology/strom:latest`,
  `nicolaka/netshoot:latest`, `linuxserver/ffmpeg:latest`.
- Host ports **18080** (strom-ingress UI/API) and **18081** (strom-egress) free.

## Recipes

Run from the repo root as `just bench <recipe>` (or `cd bench && just <recipe>`).

| Recipe | Effect |
|--------|--------|
| `up` | compose up -d + wait for both strom nodes |
| `flows` | create+start ingress/egress flows, persist IDs to `.flow-ids.env` |
| `stats` | poll `srt-stats` from both nodes, print key per-connection fields |
| `logs [svc]` | follow a service's logs (default `consumer`) |
| `degrade-loss PCT` | `netem loss PCT%` on both router interfaces |
| `degrade-latency MS [JITTER]` | `netem delay MS [JITTER]` (e.g. `degrade-latency 120 30ms`) |
| `reorder` | `netem delay 10ms reorder 25% 50%` |
| `blackout` | `netem loss 100%` |
| `heal` / `clean` | clear all netem (restore the link) |
| `netem-show` | show current qdiscs |
| `down` | compose down -v + remove `.flow-ids.env` |

## Run a scenario

```sh
just bench up          # topology + strom nodes
just bench flows       # provision + start both flows; producer/consumer auto-connect
sleep 5
just bench stats       # baseline: connected=true, ~0 loss/retx, ~2.4 Mbps

just bench degrade-loss 10     # 10% loss both directions on the impaired hop
just bench stats               # watch packets_sent_lost / packets_retransmitted climb
docker logs -f bench-consumer  # observe decode / continuity impact

just bench heal        # clear impairment
just bench stats       # loss/retx counters stop climbing; rates steady again

just bench down        # tear everything down
```

**What to observe** (impaired ingress→egress hop):

- `ingress.srtsink` (sender): `rtt_ms`, `negotiated_latency_ms=1000`,
  `packets_sent_lost` and `packets_retransmitted` rising together under loss.
- `egress.srtsrc` (receiver): `packets_received_lost` and
  `packets_received_retransmitted` rising together; `packets_received_dropped`
  should stay 0 while loss is recoverable within the latency window.
- `recv_rate_mbps` holds ~steady while SRT recovers; consumer shows no
  continuity/freeze errors until loss exceeds what the latency window can repair.
- Under `blackout`, the connection stalls; recovery may require re-running
  `flows` if the SRT caller does not re-handshake on its own.

`bandwidth_mbps` is noisy — ignore it. Telemetry is **poll-only** (no ws/SSE).

## Measured baseline vs 10% loss vs heal (real run)

| phase | ingress sent_lost / retx | egress recv_lost / recv_retx | recv Mbps | drops |
|-------|--------------------------|------------------------------|-----------|-------|
| before | 0 / 0 | 0 / 0 | 2.33 | 0 |
| +20s @ 10% loss | 492 / 492 | 443 / 443 | 2.33 | 0 |
| +35s @ 10% loss | 886 / 886 | 786 / 786 | 2.33 | 0 |
| after heal (frozen) | 1429 / 1429 | 1276 / 1276 | 2.33 | 0 |

SRT retransmitted every lost packet and the receiver recovered all of them
(retx == lost, 0 drops); recv rate never dipped and the consumer logged no
continuity/freeze errors. After heal the counters froze — recovery confirmed.
