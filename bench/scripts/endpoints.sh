#!/usr/bin/env bash
# Resolve a placed stream's concrete SRT data-plane address via the controller's
# discovery API. Polls until the stream is placed, then prints host:port for the
# producer ingress or a consumer output. Used by the bench producer/consumer
# recipes so no addresses are hardcoded.
set -euo pipefail

ctrl="${WEAVE_CONTROLLER_URL:-http://localhost:29082}"
interval=2
attempts=30

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

url="$ctrl/streams/$stream/endpoints"

for _ in $(seq 1 "$attempts"); do
  body="$(curl -s -o - -w '\n%{http_code}' "$url" 2>/dev/null || true)"
  code="${body##*$'\n'}"
  json="${body%$'\n'*}"
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
