#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

IMAGE_NAME="pgsqlite-e2e-local"
CONTAINER_NAME="pgsqlite-e2e"
VOLUME_NAME="pgsqlite-e2e-data"
HOST_PORT="55432"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker volume rm "${VOLUME_NAME}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

echo "[e2e] Building image: ${IMAGE_NAME}"
docker build -t "${IMAGE_NAME}" "${ROOT_DIR}" >/dev/null

cleanup

echo "[e2e] Starting container: ${CONTAINER_NAME}"
docker run -d --name "${CONTAINER_NAME}" -p "${HOST_PORT}:5432" \
  -v "${VOLUME_NAME}:/var/lib/postgresql/data" \
  "${IMAGE_NAME}" \
  sh -lc "set -eu; mkdir -p /var/lib/postgresql/data; \
    pgsqlite --migrate --database /var/lib/postgresql/data --pragma-journal-mode WAL --default-database default; \
    exec pgsqlite --database /var/lib/postgresql/data --pragma-journal-mode WAL --default-database default" \
  >/dev/null

echo "[e2e] Waiting for ready"
for _ in $(seq 1 30); do
  if docker run --rm -e PGPASSWORD=postgres postgres:16 \
      pg_isready -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "[e2e] Smoke: information_schema.schemata count"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select count(*) from information_schema.schemata;" \
  >/dev/null

echo "[e2e] Schema: create schema foo and verify reflected"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "create schema foo;" \
  >/dev/null

docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select schema_name from information_schema.schemata where schema_name = 'foo';" \
  | grep -q "foo"

echo "[e2e] Session: SET search_path=foo; current_schema() should return foo"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "set search_path=foo; select current_schema();" \
  | grep -q "foo"

echo "[e2e] Session: SHOW search_path should return foo"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "set search_path=foo; show search_path;" \
  | grep -q "foo"

echo "[e2e] Session: current_setting('search_path') should return foo"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "set search_path=foo; select current_setting('search_path');" \
  | grep -q "foo"

echo "[e2e] Compatibility: SELECT * FROM current_schema() should return foo"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "set search_path=foo; select * from current_schema();" \
  | grep -q "foo"

echo "[e2e] Session: set_config('search_path','bar',false) should affect current_schema()"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "create schema bar;" \
  >/dev/null

docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select set_config('search_path','bar',false); select current_schema();" \
  | grep -q "bar"

docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select set_config('search_path','bar',false); show search_path;" \
  | grep -q "bar"

echo "[e2e] OK"
