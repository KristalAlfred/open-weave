#!/usr/bin/env bash
# Apply/clear netem impairment on the router's two node-facing interfaces.
# Interfaces are resolved by IP so the eth0/eth1 ordering does not matter.
set -euo pipefail

ROUTER="${BENCH_ROUTER:-bench-router}"
ING_IP="${BENCH_ING_IP:-172.30.0.2}"
EGR_IP="${BENCH_EGR_IP:-172.31.0.2}"

usage() {
  cat >&2 <<EOF
usage: netem.sh <command> [args]
  delay <ms> [jitter_ms]   symmetric delay (+ optional jitter) on both interfaces
  loss <pct>               packet loss % on both interfaces
  reorder [ms] [pct]       reorder (default 10ms 25%) on both interfaces
  blackout                 100% loss on both interfaces
  clear                    remove all netem qdiscs
  show                     show current qdiscs
EOF
  exit 2
}

apply() {
  local spec="$1"
  docker exec -e ING="$ING_IP" -e EGR="$EGR_IP" -e SPEC="$spec" "$ROUTER" sh -c '
    for ip in "$ING" "$EGR"; do
      dev=$(ip -o -4 addr show | awk -v ip="$ip" "{split(\$4,a,\"/\"); if (a[1]==ip) print \$2}")
      [ -n "$dev" ] || { echo "no interface for $ip" >&2; exit 1; }
      tc qdisc replace dev "$dev" root netem $SPEC
      echo "$dev ($ip): netem $SPEC"
    done'
}

clear_all() {
  docker exec -e ING="$ING_IP" -e EGR="$EGR_IP" "$ROUTER" sh -c '
    for ip in "$ING" "$EGR"; do
      dev=$(ip -o -4 addr show | awk -v ip="$ip" "{split(\$4,a,\"/\"); if (a[1]==ip) print \$2}")
      [ -n "$dev" ] && tc qdisc del dev "$dev" root 2>/dev/null && echo "$dev ($ip): cleared" || true
    done; true'
}

show() {
  docker exec -e ING="$ING_IP" -e EGR="$EGR_IP" "$ROUTER" sh -c '
    for ip in "$ING" "$EGR"; do
      dev=$(ip -o -4 addr show | awk -v ip="$ip" "{split(\$4,a,\"/\"); if (a[1]==ip) print \$2}")
      printf "%s (%s): " "$dev" "$ip"; tc qdisc show dev "$dev" | head -1
    done'
}

cmd="${1:-}"; shift || true
case "$cmd" in
  delay)    [ $# -ge 1 ] || usage; apply "delay ${1}ms ${2:+${2}ms}" ;;
  loss)     [ $# -ge 1 ] || usage; apply "loss ${1}%" ;;
  reorder)  apply "delay ${1:-10}ms reorder ${2:-25}% 50%" ;;
  blackout) apply "loss 100%" ;;
  clear)    clear_all ;;
  show)     show ;;
  *)        usage ;;
esac
