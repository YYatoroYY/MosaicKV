#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d --build --wait
docker compose exec -T proxy ec_client --endpoint tcp://127.0.0.1:5555 stats
