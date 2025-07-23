#!/bin/bash

# Minimal test runner that just verifies the SQLAlchemy fix works
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DB="$SCRIPT_DIR/test_minimal.db"
PORT=15502
PGSQLITE_PID=""

cleanup() {
    if [[ -n "$PGSQLITE_PID" && "$PGSQLITE_PID" != "0" ]]; then
        kill "$PGSQLITE_PID" 2>/dev/null || true
        wait "$PGSQLITE_PID" 2>/dev/null || true
    fi
    rm -f "$TEST_DB"
    rm -f "/tmp/.s.PGSQL.$PORT"
}

trap cleanup EXIT INT TERM

echo "🧪 Minimal SQLAlchemy Compatibility Test"
echo "========================================"
echo ""

# Build and start pgsqlite
echo "Building pgsqlite..."
cd "$PROJECT_ROOT"
cargo build --release

echo "Starting pgsqlite on port $PORT..."
rm -f "$TEST_DB"
./target/release/pgsqlite --database "$TEST_DB" --port $PORT > "$SCRIPT_DIR/minimal.log" 2>&1 &
PGSQLITE_PID=$!

# Wait for startup
sleep 3

# Check if running
if ! kill -0 "$PGSQLITE_PID" 2>/dev/null; then
    echo "❌ pgsqlite failed to start"
    cat "$SCRIPT_DIR/minimal.log"
    exit 1
fi

# Test connectivity
if ! timeout 5 bash -c "echo > /dev/tcp/localhost/$PORT" 2>/dev/null; then
    echo "❌ Cannot connect to pgsqlite"
    exit 1
fi

echo "✅ pgsqlite is running"
echo ""

# Run the minimal test
cd "$SCRIPT_DIR"
chmod +x test_minimal.py

if poetry run python test_minimal.py --port $PORT; then
    echo ""
    echo "🎉 SUCCESS: SQLAlchemy compatibility confirmed!"
    echo "✅ The version() function issue has been resolved"
    exit 0
else
    echo ""
    echo "❌ Test failed - check logs above"
    exit 1
fi