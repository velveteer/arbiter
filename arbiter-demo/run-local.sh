#!/usr/bin/env bash
# Run the demo from this working tree against a local LGTM stack.
#
#   arbiter-demo/run-local.sh          # start everything, run the demo in the foreground
#   arbiter-demo/run-local.sh --down   # stop the stack and the database
#
# Ctrl-C stops the demo and leaves the stack up for the next run.
set -euo pipefail

cd "$(dirname "$0")/.."

STACK="arbiter-otel/deploy/observability/compose.yaml"
DB_CONTAINER=arbiter-demo-local-db
DB_PORT="${DB_PORT:-5433}"
PORT="${PORT:-8080}"

if [[ "${1:-}" == "--down" ]]; then
  docker compose -f "$STACK" down -v
  docker rm -f "$DB_CONTAINER" >/dev/null 2>&1 || true
  exit 0
fi

docker compose -f "$STACK" up -d

if ! docker inspect "$DB_CONTAINER" >/dev/null 2>&1; then
  docker run -d --name "$DB_CONTAINER" -p "$DB_PORT:5432" -e POSTGRES_PASSWORD=master postgres:17.4 >/dev/null
fi
docker start "$DB_CONTAINER" >/dev/null 2>&1 || true

until docker exec "$DB_CONTAINER" pg_isready -q -U postgres; do sleep 1; done
until curl -sf -o /dev/null http://localhost:3000/api/health; do sleep 1; done

echo "Demo:      http://localhost:$PORT"
echo "Dashboard: http://localhost:3000/d/arbiter/arbiter"

# A short export interval, so panels fill in without a minute of waiting.
DATABASE_URL="host=localhost port=$DB_PORT user=postgres password=master dbname=postgres" \
PORT="$PORT" \
RESET_INTERVAL_MINUTES="${RESET_INTERVAL_MINUTES:-600}" \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
OTEL_SERVICE_NAME=arbiter-demo \
OTEL_METRIC_EXPORT_INTERVAL=10000 \
  exec cabal run -v0 arbiter-demo
