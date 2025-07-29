#!/bin/bash

# Test ANY operator specifically  
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DB="$SCRIPT_DIR/test_any.db"
PORT=15505
PGSQLITE_PID=""

cleanup() {
    if [[ -n "$PGSQLITE_PID" && "$PGSQLITE_PID" != "0" ]]; then
        kill "$PGSQLITE_PID" 2>/dev/null || true
        wait "$PGSQLITE_PID" 2>/dev/null || true
    fi
    rm -f "$TEST_DB"
}

trap cleanup EXIT INT TERM

echo "🧪 Testing ANY Operator"
echo "======================="
echo ""

# Build and start pgsqlite
echo "Building and starting pgsqlite..."
cd "$PROJECT_ROOT"
cargo build --release
rm -f "$TEST_DB"
./target/release/pgsqlite --database "$TEST_DB" --port $PORT > "$SCRIPT_DIR/any_test.log" 2>&1 &
PGSQLITE_PID=$!

# Wait for startup
sleep 3

if ! kill -0 "$PGSQLITE_PID" 2>/dev/null; then
    echo "❌ pgsqlite failed to start"
    exit 1
fi

echo "✅ pgsqlite is running on port $PORT"
echo ""

# Test ANY operator
cd "$SCRIPT_DIR"
if poetry run python test_any_operator.py --port $PORT; then
    echo ""
    echo "🎉 SUCCESS: ANY operator test passed!"
    exit 0
else
    echo ""
    echo "❌ ANY operator test failed"
    exit 1
fi