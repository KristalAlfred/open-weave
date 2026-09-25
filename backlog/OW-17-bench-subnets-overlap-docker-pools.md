---
id: OW-17
title: "Bench subnets overlap Docker's default address pools"
type: bug
status: in-progress
depends_on: []
assignee: claude-bench
---

## Evidence

`bench/docker-compose.yml` puts net_core, net_node1, net_node2 and net_node3 on
172.25, 172.26, 172.27 and 172.29 `/24`s. Docker's default address pools hand
out 172.17.0.0/16 through 172.31.0.0/16, and 192.168.0.0/16 in `/20`s, to any
compose project that does not pin a subnet. On 2026-09-25 `just bench up`
failed on a host where three other compose projects held 172.25/16, 172.26/16
and 172.27/16:

```
failed to create network ow-bench_net_node1: Error response from daemon:
invalid pool request: Pool overlaps with other one on this address space
```

## Done when

- [ ] The bench's subnets sit outside Docker's default address pools.
- [ ] `just bench up` and `just bench stream-up basic` reach `flowing` on a host
      where those pools are taken.

## Easy to break

- `bench/scripts/route-manager.sh`, `inside.sh`, `netem.sh`, the adapter configs
  in `bench/config/` and the docs name the addresses, and node 3's NAT boundary
  depends on no route to its subnet being installed anywhere outside it.

## Log

- 2026-09-25: filed when the bench would not start during backlog work.
- 2026-09-25: started by claude-bench.
