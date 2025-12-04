#!/bin/bash

set -e

cd "$(dirname "$0")"

cleanup() {
    docker compose -f docker-compose-bully-tests.yml down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f docker-compose-bully-tests.yml build --quiet
docker compose -f docker-compose-bully-tests.yml run --rm bully_tests
