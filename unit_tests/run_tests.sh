#!/bin/bash

COMPOSE="docker compose"
COMPOSE_FILE="unit_tests/docker-compose.yml"
TESTS_PATH="${1:-middleware/tests/}"; shift || true
PYTEST_ARGS="${*:-}"
TIMEOUT_SECS=60

cd "$(dirname "$0")/.."

cleanup() {
  $COMPOSE -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

$COMPOSE -f "$COMPOSE_FILE" up -d rabbit

RABBIT_CID="$($COMPOSE -f "$COMPOSE_FILE" ps -q rabbit)"
if [[ -z "${RABBIT_CID:-}" ]]; then
  $COMPOSE -f "$COMPOSE_FILE" logs rabbit || true
  exit 1
fi

deadline=$((SECONDS + TIMEOUT_SECS))
while :; do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$RABBIT_CID" 2>/dev/null || echo unknown)"
  if [[ "$status" == "healthy" ]]; then
    break
  fi
  if (( SECONDS >= deadline )); then
    $COMPOSE -f "$COMPOSE_FILE" logs rabbit | tail -n 200 || true
    exit 1
  fi
  sleep 1
done

TEST_PATH="$TESTS_PATH" PYTEST_ARGS="--timeout=30 ${PYTEST_ARGS}" \
  $COMPOSE -f "$COMPOSE_FILE" run --rm tests
