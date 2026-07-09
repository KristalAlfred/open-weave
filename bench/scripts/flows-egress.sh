#!/usr/bin/env bash
# Create + start ONLY the egress flow, so open-weave's controller can own the ingress
# side (its srtsrc binds :7001). Persists the egress id to .flow-ids.env.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLOWS_DIR="$DIR/flows"
IDS_FILE="$DIR/.flow-ids.env"
EGRESS_URL="${EGRESS_URL:-http://localhost:18081}"

for _ in $(seq 1 60); do
  curl -sf "$EGRESS_URL/api/version" >/dev/null 2>&1 && break
  sleep 1
done

EGRESS_FLOW_ID=$(curl -sf -X POST "$EGRESS_URL/api/flows" \
  -H 'content-type: application/json' \
  --data-binary @"$FLOWS_DIR/egress.json" | jq -r '.flow.id')
[ -n "$EGRESS_FLOW_ID" ] && [ "$EGRESS_FLOW_ID" != "null" ] || {
  echo "[flows-egress] egress create failed" >&2; exit 1; }
curl -sf -X POST "$EGRESS_URL/api/flows/$EGRESS_FLOW_ID/start" >/dev/null
echo "[flows-egress] egress flow = $EGRESS_FLOW_ID (started)"

cat > "$IDS_FILE" <<EOF
EGRESS_URL=$EGRESS_URL
EGRESS_FLOW_ID=$EGRESS_FLOW_ID
EOF
echo "[flows-egress] wrote $IDS_FILE"
