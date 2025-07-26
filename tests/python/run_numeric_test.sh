#!/bin/bash

# Test NUMERIC type specifically  
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DB="$SCRIPT_DIR/test_numeric.db"
PORT=15506
PGSQLITE_PID=""

cleanup() {
    if [[ -n "$PGSQLITE_PID" && "$PGSQLITE_PID" != "0" ]]; then
        kill "$PGSQLITE_PID" 2>/dev/null || true
        wait "$PGSQLITE_PID" 2>/dev/null || true
    fi
    rm -f "$TEST_DB"
}

trap cleanup EXIT INT TERM

echo "🧪 Testing NUMERIC Type"
echo "======================="
echo ""

# Build and start pgsqlite
echo "Building and starting pgsqlite..."
cd "$PROJECT_ROOT"
cargo build --release
rm -f "$TEST_DB"
./target/release/pgsqlite --database "$TEST_DB" --port $PORT > "$SCRIPT_DIR/numeric_test.log" 2>&1 &
PGSQLITE_PID=$!

# Wait for startup
sleep 3

if ! kill -0 "$PGSQLITE_PID" 2>/dev/null; then
    echo "❌ pgsqlite failed to start"
    exit 1
fi

echo "✅ pgsqlite is running on port $PORT"
echo ""

# Test NUMERIC type
cd "$SCRIPT_DIR"
if poetry run python test_numeric_type.py --port $PORT; then
    echo ""
    echo "🎉 SUCCESS: NUMERIC type test passed!"
    exit 0
else
    echo ""
    echo "❌ NUMERIC type test failed"
    exit 1
fi