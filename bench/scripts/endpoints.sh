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
usage: endpoints.sh <stream> ingress|output [index]
  endpoints.sh basic ingress      producer target (source ingress) host:port
  endpoints.sh basic output        first consumer output host:port
  endpoints.sh fanout output 1     second consumer output host:port
EOF
  exit 2
}

stream="${1:-}"; [ -n "$stream" ] || usage
role="${2:-}"; [ -n "$role" ] || usage
index="${3:-0}"

case "$role" in
  ingress) filter='.ingress' ;;
  output)  filter=".outputs[$index]" ;;
  *) usage ;;
esac

# The operator contract lives under /v1 (weave_core::API_V1); only /health and
# the controller's dashboard routes sit outside it.
url="$ctrl/v1/streams/$stream/endpoints"

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
    addr="$(printf '%s' "$json" | jq -r "$filter | \"\(.host):\(.port)\"" 2>/dev/null || true)"
    if [ -n "$addr" ] && [ "$addr" != "null:null" ]; then
      printf '%s\n' "$addr"
      exit 0
    fi
    echo "stream '$stream' has no $role[$index]" >&2
    exit 1
  fi
  sleep "$interval"
done

echo "timed out after $((attempts * interval))s waiting for stream '$stream' to be placed" >&2
echo "stream not placed — is the node registered? (last HTTP ${code:-none} from $url)" >&2
exit 1
