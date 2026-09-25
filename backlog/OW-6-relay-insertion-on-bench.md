---
id: OW-6
title: "Automatic relay insertion is only tested in the planner"
type: verification
status: todo
depends_on: []
assignee:
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

- [ ] The bench has a second NAT'd site.
- [ ] A manifest between the two sites, with no `via`, reaches `flowing` through
      a relay the controller chose.
- [ ] `README.md`'s Status section no longer makes an exception for automatic
      relay insertion.

## Log

- 2026-09-25: filed from broadcaster research.
