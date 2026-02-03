#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

IMAGE_NAME="pgsqlite-e2e-local-uuid-ossp"
CONTAINER_NAME="pgsqlite-e2e-uuid-ossp"
VOLUME_NAME="pgsqlite-e2e-data-uuid-ossp"
HOST_PORT="55433"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker volume rm "${VOLUME_NAME}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

echo "[e2e] Building image: ${IMAGE_NAME}"
docker rmi -f "${IMAGE_NAME}" >/dev/null 2>&1 || true
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

echo "[e2e] CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "create extension if not exists \"uuid-ossp\";" \
  >/dev/null

echo "[e2e] CREATE EXTENSION idempotency"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "create extension if not exists \"uuid-ossp\";" \
  >/dev/null

echo "[e2e] uuid_generate_v4()"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select uuid_generate_v4();" \
  | grep -Eq "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}"

echo "[e2e] uuid_generate_v1()/uuid_generate_v1mc()"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select uuid_generate_v1();" \
  | grep -Eq "[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}"

docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select uuid_generate_v1mc();" \
  | grep -Eq "[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}"

echo "[e2e] uuid_ns_*() constants"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select uuid_nil();" \
  | grep -q "00000000-0000-0000-0000-000000000000"

docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select uuid_ns_dns();" \
  | grep -Eq "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select uuid_ns_url();" \
  | grep -Eq "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select uuid_ns_oid();" \
  | grep -Eq "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select uuid_ns_x500();" \
  | grep -Eq "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

echo "[e2e] uuid_generate_v3/v5 deterministic versions"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select uuid_generate_v3(uuid_ns_url(), 'http://www.postgresql.org');" \
  | grep -Eq "[0-9a-f]{8}-[0-9a-f]{4}-3[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}"

docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select uuid_generate_v5(uuid_ns_url(), 'http://www.postgresql.org');" \
  | grep -Eq "[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}"

echo "[e2e] OK"
