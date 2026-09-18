#!/usr/bin/env bash
# Resolve a placed stream's concrete SRT data-plane address via the controller's
# discovery API. Polls until the stream is placed, then prints host:port for the
# producer ingress or a consumer output. Used by the bench producer/consumer
# recipes so no addresses are hardcoded.
set -euo pipefail

ctrl="${WEAVE_CONTROLLER_URL:-http://localhost:29082}"
interval=2
attempts=30

# The discovery route is part of the northbound API surface, so it needs that
# surface's bearer token. Unset is only valid against a stack running with
# WEAVE_AUTH_DISABLED=1.
auth=()
if [ -n "${WEAVE_NORTHBOUND_TOKEN:-}" ]; then
  auth=(-H "Authorization: Bearer ${WEAVE_NORTHBOUND_TOKEN}")
fi

usage() {
  cat >&2 <<EOF
usage: endpoints.sh <stream> ingress|destination|destinations|destination-ids [id]
  endpoints.sh basic ingress       producer target (source ingress) host:port
  endpoints.sh basic destination output
  endpoints.sh fanout destination studio
  endpoints.sh fanout destinations
EOF
  exit 2
}

stream="${1:-}"; [ -n "$stream" ] || usage
role="${2:-}"; [ -n "$role" ] || usage
destination="${3:-}"

# `destinations` reports how many consumers a stream needs; the others resolve one
# concrete address. Both wait for placement, so they share the polling loop.
case "$role" in
  ingress) filter='.ingress | "\(.host):\(.port)"' ;;
  destination) [ -n "$destination" ] || usage
               filter=".destinations[] | select(.id == \"$destination\") | .endpoint | \"\(.host):\(.port)\"" ;;
  destinations) filter='.destinations | length' ;;
  destination-ids) filter='[.destinations[].id] | sort | join(" ")' ;;
  *) usage ;;
esac

url="$ctrl/streams/$stream/endpoints"

for _ in $(seq 1 "$attempts"); do
  body="$(curl -s -o - -w '\n%{http_code}' "${auth[@]}" "$url" 2>/dev/null || true)"
  code="${body##*$'\n'}"
  json="${body%$'\n'*}"
  # Retrying a rejected token never converges, so fail fast and say why.
  if [ "$code" = "401" ]; then
    echo "controller rejected the bearer token (401) for $url" >&2
    if [ -n "${WEAVE_NORTHBOUND_TOKEN:-}" ]; then
      echo "WEAVE_NORTHBOUND_TOKEN does not match the controller's" >&2
    else
      echo "WEAVE_NORTHBOUND_TOKEN is unset" >&2
    fi
    exit 1
  fi
  if [ "$code" = "200" ]; then
    value="$(printf '%s' "$json" | jq -r "$filter" 2>/dev/null || true)"
    if [ -n "$value" ] && [ "$value" != "null" ] && [ "$value" != "null:null" ]; then
      printf '%s\n' "$value"
      exit 0
    fi
    echo "stream '$stream' has no $role ${destination:-}" >&2
    exit 1
  fi
  sleep "$interval"
done

echo "timed out after $((attempts * interval))s waiting for stream '$stream' to be placed" >&2
echo "stream not placed — is the node registered? (last HTTP ${code:-none} from $url)" >&2
exit 1
