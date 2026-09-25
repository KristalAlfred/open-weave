---
id: OW-6
title: "Automatic relay insertion is only tested in the planner"
type: verification
status: done
depends_on: [OW-17]
assignee: claude-bench
---

## Evidence

The bench has one NAT'd site, so `nat-relay` pins node 1 with `via`
(`bench/manifests/nat-relay.yaml`), and `README.md`'s Status section says only
planner tests cover automatic relay insertion. NAT and caller/listener setup are
the SRT problems vendors document most
([Haivision](https://www.haivision.com/blog/all/basics-getting-real-time-video-through-firewall/),
[Vizrt](https://docs.vizrt.com/viz-now-launchpad/1.2/Sending_and_Receiving_SRT_Video_Feeds.html)),
and a search of orchestration products found none that derives roles from
declared reachability.

## Done when

- [x] The bench has a second NAT'd site.
- [x] A manifest between the two sites, with no `via`, reaches `flowing` through
      a relay the controller chose.
- [x] `README.md`'s Status section no longer makes an exception for automatic
      relay insertion.

## Log

- 2026-09-25: filed from broadcaster research.
- 2026-09-25: depends on OW-17, since the bench does not start until its subnets move.
- 2026-09-25: started by claude-bench.
- 2026-09-25: box 1 checked on `bench/`. Node 4 sits on net_node4
  (10.97.30.0/24) behind router-4, which masquerades like router-3, with its
  own `node-4-local` site network. After `just bench up`, strom-node-4
  registered `ready`. TCP connects to `10.97.29.10:8080` and `10.97.30.10:8080`
  from ow-controller, ow-strom-1 and ow-strom-2 time out, as do ow-strom-3 to
  `10.97.30.10:8080` and ow-strom-4 to `10.97.29.10:8080`; ow-strom-3 and
  ow-strom-4 both connect to `10.97.26.10:8080` and `10.97.27.10:8080`. No
  route to either NAT'd subnet exists in routers 1 to 4 (other than each NAT
  router's own subnet) or in any container outside that subnet.
- 2026-09-25: box 2 checked on `bench/`. `just bench stream-up nat-transit`
  (node 3 to node 4, no `via`) reached `flowing`, with
  `weave-nat-transit-bridge-output-0` on strom-node-1 listening on both sockets
  and both NAT'd hops dialling `10.97.26.10`. With `ow-adapter-1` stopped, the
  bridge moved to strom-node-2 once node 1 went offline and the stream was
  `flowing` again about 12 s later; with it started again the bridge moved back
  to node 1. A reversed copy (node 4 to node 3, not committed) also reached
  `flowing` through node 1. `basic`, `nat-relay`, `nat-ingress`, `nat-egress`,
  `via` and `fanout` still reach `flowing` on the four-node bench.
- 2026-09-25: box 3 checked by reading `README.md`: the Status section no longer
  names an exception. Added
  `bench_nat_sites_bridge_through_the_lowest_online_node_both_dial` in
  `crates/controller/src/path.rs` (unit test): the bench's nodes 1 to 4 place
  the bridge on strom-node-1, and on strom-node-2 with node 1 offline.
- 2026-09-25: re-run on `bench/` after rebasing on `main` at f768383: `nat-transit`
  and `basic` reach `flowing`, with the bridge on strom-node-1.
- 2026-09-25: re-run on `bench/` at `main` 1f4eed7, after SRT links between nodes
  became keyed: `nat-transit` reached `flowing`; the bridge on strom-node-1 and
  both NAT'd hops carried `passphrase` and `pbkeylen=32` in their link URIs.
  Stopping `ow-adapter-1` moved the bridge to strom-node-2 and the stream read
  `flowing` about 30 s later; starting it moved the bridge back in about 20 s.
  TCP connects between the two NAT'd sites, and into each from nodes 1 and 2,
  still fail.
