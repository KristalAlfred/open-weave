#!/usr/bin/env bash
# Print the container-name suffix of the verification endpoint that can reach
# `host:port`, given where that address lives.
#
# Most bench addresses sit on a routed node subnet and are dialable from
# net_core, so the plain `producer`/`consumer` reach them and the suffix is
# empty. Node 3 is behind a NAT with no route in, so only a peer already inside
# net_node3 can dial it: those addresses select the `-3` containers.
#
# This mirrors the reachability the node itself declares (see
# config/adapter-3.yaml). If a fourth node ever hides behind its own NAT, it
# gets a case here and a matching pair of endpoint containers.
set -euo pipefail

addr="${1:-}"
if [ -z "$addr" ]; then
  echo "usage: inside.sh <host[:port]>" >&2
  exit 2
fi

case "${addr%%:*}" in
  172.29.0.*) echo "-3" ;;
  *) echo "" ;;
esac
