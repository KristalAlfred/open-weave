#!/usr/bin/env bash
# Full-loop open-weave demo: docker bench + northbound + controller, actuating a
# real Strom flow from examples/contribution.yaml. See `just demo-*` recipes.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH="$REPO/bench"
RUN="$REPO/.demo"
INGRESS_URL="http://localhost:18080"
EGRESS_URL="http://localhost:18081"
NB_URL="http://127.0.0.1:9080"
CTRL_URL="http://127.0.0.1:8082"

STATS_JQ='
  .stats.connections | to_entries[] |
  "  \(.key) [\(.value.role)] connected=\(.value.connected)",
  (.value.callers[]? |
   "    send=\((.send_rate_mbps*100|round)/100)Mbps recv=\((.recv_rate_mbps*100|round)/100)Mbps " +
   "sent_lost=\(.packets_sent_lost) retx=\(.packets_retransmitted) " +
   "recv_lost=\(.packets_received_lost) recv_retx=\(.packets_received_retransmitted) " +
   "drop=\(.packets_received_dropped)")'

mkdir -p "$RUN"
bin() { echo "$REPO/target/debug/$1"; }

wait_http() {
  local url="$1" name="$2"
  for _ in $(seq 1 60); do
    curl -sf "$url" >/dev/null 2>&1 && { echo "[demo] $name ready"; return 0; }
    sleep 1
  done
  echo "[demo] $name not ready: $url" >&2
  return 1
}

start_bin() {
  local name="$1"
  shift
  local log="$RUN/$name.log" pidf="$RUN/$name.pid"
  if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    echo "[demo] $name already running (pid $(cat "$pidf"))"
    return 0
  fi
  nohup "$@" </dev/null >"$log" 2>&1 &
  echo $! >"$pidf"
  echo "[demo] started $name (pid $(cat "$pidf")) -> $log"
}

stop_bin() {
  local name="$1"
  local pidf="$RUN/$name.pid"
  [ -f "$pidf" ] || return 0
  local pid
  pid="$(cat "$pidf")"
  kill "$pid" 2>/dev/null || true
  rm -f "$pidf"
  echo "[demo] stopped $name (pid $pid)"
}

flow_id_by_name() {
  curl -sf "$1/api/flows" 2>/dev/null | jq -r --arg n "$2" '.flows[] | select(.name==$n) | .id' | head -1
}

dump_stats() {
  local label="$1" base="$2" id="$3"
  echo "== $label ($id) =="
  if [ -z "$id" ]; then
    echo "  (no flow yet)"
    return
  fi
  curl -sf "$base/api/flows/$id/srt-stats" | jq -r "$STATS_JQ" 2>/dev/null || echo "  (no stats)"
}

cmd_up() {
  (cd "$BENCH" && just up)
  (cd "$BENCH" && just flows-egress)
  start_bin northbound "$(bin weave-northbound)"
  wait_http "$NB_URL/health" northbound
  start_bin controller "$(bin weave-controller)" --strom-url "$INGRESS_URL" --northbound-url "$NB_URL"
  wait_http "$CTRL_URL/health" controller
  echo
  echo "[demo] control plane up. Apply intent:"
  echo "         just demo-apply       # weave apply -f examples/contribution.yaml"
  echo "       Then observe:"
  echo "         just demo-stats | just demo-loss 10 | just demo-heal"
}

cmd_apply() {
  "$(bin weave)" apply -f "$REPO/examples/contribution.yaml"
}

cmd_stats() {
  local iid eid
  iid=$(flow_id_by_name "$INGRESS_URL" contribution)
  eid=$(flow_id_by_name "$EGRESS_URL" bench-egress)
  dump_stats "contribution/ingress" "$INGRESS_URL" "$iid"
  dump_stats "bench-egress" "$EGRESS_URL" "$eid"
}

cmd_status() {
  curl -sf "$CTRL_URL/status" | jq .
}

cmd_down() {
  stop_bin controller
  stop_bin northbound
  (cd "$BENCH" && just down) || true
  rm -rf "$RUN"
}

case "${1:-}" in
  up) cmd_up ;;
  apply) cmd_apply ;;
  stats) cmd_stats ;;
  status) cmd_status ;;
  down) cmd_down ;;
  *)
    echo "usage: demo.sh {up|apply|stats|status|down}" >&2
    exit 2
    ;;
esac
