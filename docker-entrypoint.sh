#!/bin/sh
set -eu

# Emulate enough of the official Postgres container contract:
# - honors POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_DB
# - persists data under PGDATA (/var/lib/postgresql/data)
# - provides a unix socket for `pg_isready` default behavior

: "${PGDATA:=/var/lib/postgresql/data}"
: "${POSTGRES_DB:=postgres}"
: "${POSTGRES_USER:=postgres}"
: "${POSTGRES_PASSWORD:=}"

mkdir -p "${PGDATA}" /var/run/postgresql
chmod 0777 /var/run/postgresql

# Map postgres-style env to pgsqlite env.
: "${PGSQLITE_DATABASE:=${PGDATA}}"
: "${PGSQLITE_DEFAULT_DATABASE:=${POSTGRES_DB}}"
: "${PGSQLITE_SOCKET_DIR:=/var/run/postgresql}"

# Reasonable defaults for concurrency.
: "${PGSQLITE_JOURNAL_MODE:=WAL}"
: "${PGSQLITE_SYNCHRONOUS:=NORMAL}"

export PGDATA POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD
export PGSQLITE_DATABASE PGSQLITE_DEFAULT_DATABASE PGSQLITE_SOCKET_DIR
export PGSQLITE_JOURNAL_MODE PGSQLITE_SYNCHRONOUS

# If invoked with "pgsqlite" (default CMD), apply migrations then start server.
if [ "$#" -ge 1 ] && [ "$1" = "pgsqlite" ]; then
  shift
  # Apply pgsqlite internal migrations for the default database so catalogs work.
  /usr/local/bin/pgsqlite --migrate || true
  exec /usr/local/bin/pgsqlite "$@" --pragma-journal-mode "${PGSQLITE_JOURNAL_MODE}" --pragma-synchronous "${PGSQLITE_SYNCHRONOUS}"
fi

exec "$@"
