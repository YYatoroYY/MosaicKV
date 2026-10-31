#!/usr/bin/env bash
# Run once on each participating storage host, and once on the proxy host.
set -euo pipefail
role=${1:-}
if [[ "$role" != node && "$role" != proxy ]] || [[ "$role" == node && ! ${2:-} =~ ^[0-9]+$ ]]; then
  echo 'Usage: bash scripts/start_physical.sh node NODE_ID | proxy' >&2
  exit 2
fi
inventory=$(realpath "${NODES_FILE:-nodes.conf}")
strategy=${UPDATE_STRATEGY:-mosaic}
batch=0
if [[ "$strategy" == mosaic ]]; then batch=1; fi
common=(--network host --read-only -v "$inventory:/app/nodes.conf:ro"
  -e "EC_K=${EC_K:-6}" -e "EC_M=${EC_M:-3}"
  -e STORAGE_BACKEND=native -e "NATIVE_MEMORY_MB=${NATIVE_MEMORY_MB:-8192}"
  -e EC_WORKLOAD_PHASE=load -e EC_TRAFFIC=1 -e CODING_UNIT=object
  -e "NODE_TIMEOUT_MS=${NODE_TIMEOUT_MS:-10000}" -e NODES_FILE=/app/nodes.conf
  -e "MOSAIC_BATCH_IO=$batch" -e "MOSAIC_IO_THREADS=${MOSAIC_IO_THREADS:-4}")
if [[ "$role" == node ]]; then
  port=$(awk -v id="$2" '$1 == id {print $3}' "$inventory")
  [[ "$port" =~ ^[0-9]+$ ]] || { echo 'Node ID missing or duplicated in inventory' >&2; exit 2; }
  exec docker run -d --name mosaickv-node "${common[@]}" -e "NODE_ID=$2" \
    -e "BIND=tcp://*:$port" "${EC_IMAGE:-mosaickv:local}" ec_node_service
else
  exec docker run -d --name mosaickv-proxy "${common[@]}" \
    -e "UPDATE_STRATEGY=$strategy" -e "ALIGNMENT_CHUNK=${ALIGNMENT_CHUNK:-4096}" \
    -e "MOSAIC_FMAX=${MOSAIC_FMAX:-4}" -e "MOSAIC_TAU=${MOSAIC_TAU:-8}" \
    -e "MOSAIC_APPEND=${MOSAIC_APPEND:-0}" -e MOSAIC_PLACEMENT=parity_bandwidth \
    -e "MOSAIC_REBALANCE_EVERY=${MOSAIC_REBALANCE_EVERY:-1}" -e "GC_EVERY_UPDATES=${GC_EVERY_UPDATES:-5000}" \
    -e "BIND=tcp://*:${PROXY_PORT:-5555}" "${EC_IMAGE:-mosaickv:local}" ec_proxy
fi
