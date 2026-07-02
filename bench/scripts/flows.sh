#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLOWS_DIR="$DIR/flows"
IDS_FILE="$DIR/.flow-ids.env"

INGRESS_URL="${INGRESS_URL:-http://localhost:18080}"
EGRESS_URL="${EGRESS_URL:-http://localhost:18081}"

wait_ready() {
  local base="$1" name="$2"
  for i in $(seq 1 60); do
    if curl -sf "$base/api/version" >/dev/null 2>&1; then
      echo "[flows] $name ready ($base)"
      return 0
    fi
    sleep 1
  done
  echo "[flows] $name not ready after 60s ($base)" >&2
  return 1
}

create_and_start() {
  local base="$1" json="$2"
  local id
  id=$(curl -sf -X POST "$base/api/flows" \
        -H 'content-type: application/json' \
        --data-binary @"$json" | jq -r '.flow.id')
  [ -n "$id" ] && [ "$id" != "null" ] || { echo "[flows] create failed for $json" >&2; return 1; }
  curl -sf -X POST "$base/api/flows/$id/start" >/dev/null
  echo "$id"
}

wait_ready "$EGRESS_URL" strom-egress
wait_ready "$INGRESS_URL" strom-ingress

# Egress first so its listeners are up before ingress dials out.
EGRESS_FLOW_ID=$(create_and_start "$EGRESS_URL" "$FLOWS_DIR/egress.json")
echo "[flows] egress flow  = $EGRESS_FLOW_ID"
INGRESS_FLOW_ID=$(create_and_start "$INGRESS_URL" "$FLOWS_DIR/ingress.json")
echo "[flows] ingress flow = $INGRESS_FLOW_ID"

cat > "$IDS_FILE" <<EOF
INGRESS_URL=$INGRESS_URL
EGRESS_URL=$EGRESS_URL
INGRESS_FLOW_ID=$INGRESS_FLOW_ID
EGRESS_FLOW_ID=$EGRESS_FLOW_ID
EOF
echo "[flows] wrote $IDS_FILE"
