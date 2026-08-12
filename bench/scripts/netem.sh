#!/usr/bin/env bash
# Apply/clear netem impairment on a node's router. Each router forwards all of a
# Strom node's traffic (SRT media, adapter heartbeats, controller->Strom API), so
# impairing its two interfaces emulates that node's bad network.
# Interfaces are resolved by IP, so eth0/eth1 ordering does not matter.
set -euo pipefail

usage() {
  cat >&2 <<EOF
usage: netem.sh <node1|node2|node3> <command|netem-spec>
  netem.sh node1 delay 200ms 20ms      symmetric delay+jitter on node1's router
  netem.sh node1 loss 5%               packet loss on node1's router
  netem.sh node1 delay 200ms loss 5%   any raw 'tc netem' spec
  netem.sh node1 clear                 remove netem on node1's router
  netem.sh node1 show                  show current qdiscs
EOF
  exit 2
}

node="${1:-}"; shift || usage
case "$node" in
  node1) router="ow-router-1"; ips="172.26.0.2 172.25.0.11" ;;
  node2) router="ow-router-2"; ips="172.27.0.2 172.25.0.12" ;;
  node3) router="ow-router-3"; ips="172.29.0.2 172.25.0.13" ;;
  *) usage ;;
esac

[ $# -ge 1 ] || usage
action="$1"

run() {
  docker exec -e IPS="$ips" -e SPEC="$*" "$router" sh -c '
    for ip in $IPS; do
      dev=$(ip -o -4 addr show | awk -v ip="$ip" "{split(\$4,a,\"/\"); if (a[1]==ip) print \$2}")
      [ -n "$dev" ] || { echo "no interface for $ip" >&2; exit 1; }
      eval "$SPEC"
    done'
}

case "$action" in
  clear)
    run 'tc qdisc del dev "$dev" root 2>/dev/null && echo "$dev ($ip): cleared" || true'
    ;;
  show)
    run 'printf "%s (%s): " "$dev" "$ip"; tc qdisc show dev "$dev" | head -1'
    ;;
  *)
    docker exec -e IPS="$ips" -e SPEC="$*" "$router" sh -c '
      for ip in $IPS; do
        dev=$(ip -o -4 addr show | awk -v ip="$ip" "{split(\$4,a,\"/\"); if (a[1]==ip) print \$2}")
        [ -n "$dev" ] || { echo "no interface for $ip" >&2; exit 1; }
        tc qdisc replace dev "$dev" root netem $SPEC
        echo "$dev ($ip): netem $SPEC"
      done'
    ;;
esac
