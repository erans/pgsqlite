#!/bin/bash

# Test PostgreSQL type OID mapping
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DB="$SCRIPT_DIR/test_type_oids.db"
PORT=15509
PGSQLITE_PID=""

cleanup() {
    if [[ -n "$PGSQLITE_PID" && "$PGSQLITE_PID" != "0" ]]; then
        kill "$PGSQLITE_PID" 2>/dev/null || true
        wait "$PGSQLITE_PID" 2>/dev/null || true
    fi
    rm -f "$TEST_DB"
}

trap cleanup EXIT INT TERM

echo "🧪 PostgreSQL Type OID Debug Test"
echo "================================="
echo ""

# Build and start pgsqlite
echo "Building and starting pgsqlite..."
cd "$PROJECT_ROOT"
cargo build --release
rm -f "$TEST_DB"
./target/release/pgsqlite --database "$TEST_DB" --port $PORT > "$SCRIPT_DIR/type_oid_test.log" 2>&1 &
PGSQLITE_PID=$!

# Wait for startup
sleep 3

if ! kill -0 "$PGSQLITE_PID" 2>/dev/null; then
    echo "❌ pgsqlite failed to start"
    exit 1
fi

echo "✅ pgsqlite is running on port $PORT"
echo ""

# Run type OID test
cd "$SCRIPT_DIR"
if poetry run python test_type_oids.py --port $PORT; then
    echo ""
    echo "🎉 SUCCESS: Type OID test completed!"
    exit 0
else
    echo ""
    echo "❌ Type OID test failed"
    exit 1
fi