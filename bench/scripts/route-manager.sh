#!/usr/bin/env bash
# Continuously (re)assert the bench's inter-subnet routes inside each service's
# network namespace.
#
# Node subnets are reachable from net_core only through the netem routers, and
# several service images (Strom, ffmpeg) have no `ip` tool, so services cannot
# install these routes themselves. The routes live in the service's netns, so
# they vanish whenever its container is restarted or recreated -- a one-shot
# sidecar has already exited by then and the service silently loses reachability.
# Asserting on a loop instead means anything that comes back, or comes up later
# (the profiled producer/consumer), is repaired within one interval.
#
# Runs in a container with the Docker socket, host PID namespace, and
# NET_ADMIN/SYS_ADMIN/SYS_PTRACE: it resolves each target's PID via the Docker
# API and enters its netns with nsenter. `ip route replace` is idempotent, so a
# pass over already-routed containers is a no-op.
#
# This file is bind-mounted, and the running shell read it once at startup, so
# editing it changes nothing until the container is recreated. `docker compose
# up -d` will not do that on its own — a changed mounted file is not a changed
# service definition. Force it:
#   docker compose -f bench/docker-compose.yml up -d --force-recreate route-manager
set -uo pipefail

interval="${ROUTE_INTERVAL:-3}"
sock="${DOCKER_SOCK:-/var/run/docker.sock}"

# Routes are grouped by which subnet a container sits on, mirroring the topology:
# core reaches each node subnet via that node's router; a node subnet reaches
# everything else via its own router's leg on that subnet.
#
# Node 3 is the deliberate exception. It gets routes *out* to every other subnet,
# and no other group gets a route to 172.29.0.0/24 — not core, not node 1, not
# node 2. Docker's inter-network isolation blocks the bridge-level path, so with
# no route installed nothing outside net_node3 can open a connection to it, while
# router-3's MASQUERADE lets node 3 reach out and get answered. That asymmetry is
# the NAT the bench tests against: adding a 172.29.0.0/24 route to any group
# below silently removes the boundary and the NAT manifests start passing for the
# wrong reason.
core_containers="ow-controller ow-southbound ow-producer ow-consumer ow-consumer-2"
core_routes="172.26.0.0/24=172.25.0.11 172.27.0.0/24=172.25.0.12"

node1_containers="ow-strom-1 ow-adapter-1"
node1_routes="172.25.0.0/24=172.26.0.2 172.27.0.0/24=172.26.0.2"

node2_containers="ow-strom-2 ow-adapter-2"
node2_routes="172.25.0.0/24=172.27.0.2 172.26.0.0/24=172.27.0.2"

node3_containers="ow-strom-3 ow-adapter-3 ow-producer-3 ow-consumer-3"
node3_routes="172.25.0.0/24=172.29.0.2 172.26.0.0/24=172.29.0.2 172.27.0.0/24=172.29.0.2"

# PID of a running container, or empty if it is absent or stopped. Profiled
# services (producer/consumer) legitimately do not exist most of the time.
container_pid() {
  curl -s --unix-socket "$sock" "http://localhost/containers/$1/json" 2>/dev/null |
    jq -r 'if .State.Running then .State.Pid else empty end' 2>/dev/null
}

# Install any route that is missing or points at the wrong gateway. Checking
# first keeps a converged bench quiet, so log lines mark real repairs.
assert_routes() {
  local name="$1" pid cidr via
  shift
  pid="$(container_pid "$name")"
  [ -n "$pid" ] && [ "$pid" != 0 ] || return 0

  for spec in "$@"; do
    cidr="${spec%%=*}"
    via="${spec#*=}"
    if nsenter -t "$pid" -n ip route show "$cidr" 2>/dev/null | grep -q "via $via"; then
      continue
    fi
    if nsenter -t "$pid" -n ip route replace "$cidr" via "$via" 2>/dev/null; then
      echo "$name: installed $cidr via $via"
    else
      echo "$name: failed to install $cidr via $via" >&2
    fi
  done
}

echo "route-manager: asserting bench routes every ${interval}s"
while :; do
  for c in $core_containers; do assert_routes "$c" $core_routes; done
  for c in $node1_containers; do assert_routes "$c" $node1_routes; done
  for c in $node2_containers; do assert_routes "$c" $node2_routes; done
  for c in $node3_containers; do assert_routes "$c" $node3_routes; done
  sleep "$interval"
done
