#!/usr/bin/env bash
# Print the container-name suffix of the verification endpoint that can reach
# `host:port`, given where that address lives.
#
# Most bench addresses sit on a routed node subnet and are dialable from
# net_core, so the plain `producer`/`consumer` reach them and the suffix is
# empty. Nodes 3 and 4 are each behind a NAT with no route in, so only a peer
# already inside that node's subnet can dial it: those addresses select the `-3`
# or `-4` containers.
#
# This mirrors the reachability the nodes themselves declare (see
# config/adapter-3.yaml and config/adapter-4.yaml). Another NAT'd node gets a
# case here and a matching pair of endpoint containers.
set -euo pipefail

addr="${1:-}"
if [ -z "$addr" ]; then
  echo "usage: inside.sh <host[:port]>" >&2
  exit 2
fi

case "${addr%%:*}" in
  10.97.29.*) echo "-3" ;;
  10.97.30.*) echo "-4" ;;
  *) echo "" ;;
esac
