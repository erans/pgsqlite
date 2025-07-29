#!/bin/bash

# Test table extraction with debug output
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DB="$SCRIPT_DIR/test_table_debug.db"
PORT=15513
PGSQLITE_PID=""

cleanup() {
    if [[ -n "$PGSQLITE_PID" && "$PGSQLITE_PID" != "0" ]]; then
        kill "$PGSQLITE_PID" 2>/dev/null || true
        wait "$PGSQLITE_PID" 2>/dev/null || true
    fi
    rm -f "$TEST_DB"
}

trap cleanup EXIT INT TERM

echo "🧪 Table Extraction Debug Test"
echo "=============================="
echo ""

# Build and start pgsqlite with debug logging
echo "Starting pgsqlite with debug logging..."
cd "$PROJECT_ROOT"
rm -f "$TEST_DB"
RUST_LOG=debug ./target/release/pgsqlite --database "$TEST_DB" --port $PORT > "$SCRIPT_DIR/table_debug.log" 2>&1 &
PGSQLITE_PID=$!

# Wait for startup
sleep 3

if ! kill -0 "$PGSQLITE_PID" 2>/dev/null; then
    echo "❌ pgsqlite failed to start"
    exit 1
fi

echo "✅ pgsqlite is running on port $PORT with debug logging"
echo ""

# Run extraction test
cd "$SCRIPT_DIR"
if poetry run python test_table_extraction_debug.py --port $PORT; then
    echo ""
    echo "🎉 SUCCESS: Table extraction debug test completed!"
    echo ""
    echo "Debug output from pgsqlite:"
    echo "==========================="
    grep -E "(extract_table_name_from_select|Type inference)" "$SCRIPT_DIR/table_debug.log" | tail -20
    exit 0
else
    echo ""
    echo "❌ Table extraction debug test failed"
    echo ""
    echo "Debug output from pgsqlite:"
    echo "==========================="
    grep -E "(extract_table_name_from_select|Type inference)" "$SCRIPT_DIR/table_debug.log" | tail -20
    exit 1
fi