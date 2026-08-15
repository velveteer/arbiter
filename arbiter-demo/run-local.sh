#!/usr/bin/env bash
# Run the demo from this working tree behind the deployment's Caddy routes.
#
#   arbiter-demo/run-local.sh          # start the stack, run the demo in the foreground
#   arbiter-demo/run-local.sh --down   # stop the stack
#
# Ctrl-C stops the demo and leaves the stack up for the next run.
set -euo pipefail

cd "$(dirname "$0")"

STACK=compose.local.yml
DB_PORT="${DB_PORT:-5433}"
PORT="${PORT:-8080}"
PROXY_PORT="${PROXY_PORT:-8000}"
export DB_PORT PORT PROXY_PORT

if [[ "${1:-}" == "--down" ]]; then
  exec docker compose -f "$STACK" down -v
fi

docker compose -f "$STACK" up -d --wait
until curl -sf -o /dev/null "http://localhost:$PROXY_PORT/dash/api/health"; do sleep 1; done

echo "Demo:      http://localhost:$PROXY_PORT"
echo "Dashboard: http://localhost:$PROXY_PORT/dash"

# A short export interval, so panels fill in without a minute of waiting.
DATABASE_URL="host=localhost port=$DB_PORT user=postgres password=master dbname=postgres" \
RESET_INTERVAL_MINUTES="${RESET_INTERVAL_MINUTES:-600}" \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
OTEL_SERVICE_NAME=arbiter-demo \
OTEL_METRIC_EXPORT_INTERVAL=10000 \
  exec cabal run -v0 arbiter-demo
