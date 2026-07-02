#!/usr/bin/env bash
# Poll srt-stats from both strom nodes and print the key per-connection fields.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IDS_FILE="$DIR/.flow-ids.env"
[ -f "$IDS_FILE" ] || { echo "no $IDS_FILE; run flows first" >&2; exit 1; }
# shellcheck disable=SC1090
. "$IDS_FILE"

JQ_FILTER='
  .stats.connections | to_entries[] |
  "  \(.key) [\(.value.role)/\(.value.mode // "?")] connected=\(.value.connected)",
  (.value.callers[] |
   "    addr=\(.address // "-") rtt_ms=\(.rtt_ms) neg_lat_ms=\(.negotiated_latency_ms) " +
   "send=\((.send_rate_mbps*1000|round)/1000)Mbps recv=\((.recv_rate_mbps*1000|round)/1000)Mbps " +
   "sent_lost=\(.packets_sent_lost) recv_lost=\(.packets_received_lost) " +
   "retx=\(.packets_retransmitted) recv_retx=\(.packets_received_retransmitted) " +
   "sent_drop=\(.packets_sent_dropped) recv_drop=\(.packets_received_dropped)")'

dump() {
  local label="$1" base="$2" id="$3"
  echo "== $label ($id) =="
  curl -sf "$base/api/flows/$id/srt-stats" | jq -r "$JQ_FILTER" || echo "  (no stats)"
}

dump "INGRESS" "$INGRESS_URL" "$INGRESS_FLOW_ID"
dump "EGRESS"  "$EGRESS_URL"  "$EGRESS_FLOW_ID"
