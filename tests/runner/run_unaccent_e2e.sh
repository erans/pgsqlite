#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

IMAGE_NAME="pgsqlite-e2e-local-unaccent"
CONTAINER_NAME="pgsqlite-e2e-unaccent"
VOLUME_NAME="pgsqlite-e2e-data-unaccent"
HOST_PORT="55434"

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

echo "[e2e] CREATE EXTENSION unaccent"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "create extension if not exists unaccent;" \
  >/dev/null

echo "[e2e] unaccent(text)"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -tAc "select unaccent('Hôtel');" \
  | grep -qx "Hotel"

echo "[e2e] unaccent(regdictionary,text)"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -tAc "select unaccent('public.unaccent'::regdictionary, 'Hôtel');" \
  | grep -qx "Hotel"

echo "[e2e] CREATE OR REPLACE FUNCTION wrapper + call"
docker run --rm -i -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -f - \
  >/dev/null <<'SQL'
CREATE OR REPLACE FUNCTION public.unaccent_immutable(input text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
SELECT public.unaccent('public.unaccent'::regdictionary, input)
$$;
SQL

docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -tAc "select public.unaccent_immutable('Hôtel');" \
  | grep -qx "Hotel"

echo "[e2e] OK"
