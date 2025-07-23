#!/bin/bash

# Test SHOW commands specifically
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DB="$SCRIPT_DIR/test_show.db"
PORT=15503
PGSQLITE_PID=""

cleanup() {
    if [[ -n "$PGSQLITE_PID" && "$PGSQLITE_PID" != "0" ]]; then
        kill "$PGSQLITE_PID" 2>/dev/null || true
        wait "$PGSQLITE_PID" 2>/dev/null || true
    fi
    rm -f "$TEST_DB"
}

trap cleanup EXIT INT TERM

echo "🧪 Testing SHOW Commands Fix"
echo "============================"
echo ""

# Build and start pgsqlite
echo "Building and starting pgsqlite..."
cd "$PROJECT_ROOT"
cargo build --release
rm -f "$TEST_DB"
./target/release/pgsqlite --database "$TEST_DB" --port $PORT > "$SCRIPT_DIR/show_test.log" 2>&1 &
PGSQLITE_PID=$!

# Wait for startup
sleep 3

if ! kill -0 "$PGSQLITE_PID" 2>/dev/null; then
    echo "❌ pgsqlite failed to start"
    exit 1
fi

echo "✅ pgsqlite is running on port $PORT"
echo ""

# Test SHOW commands using Poetry
cd "$SCRIPT_DIR"
if poetry run python -c "
import psycopg2
conn = psycopg2.connect(host='localhost', port=$PORT, database='main', user='postgres', password='postgres')
cursor = conn.cursor()

print('Testing SHOW commands...')

# Test the specific command that was failing
cursor.execute('SHOW transaction isolation level')
result = cursor.fetchone()
print(f'✅ SHOW transaction isolation level: {result[0]}')

# Test other SHOW commands
cursor.execute('SHOW server_version')
result = cursor.fetchone()
print(f'✅ SHOW server_version: {result[0]}')

cursor.execute('SHOW client_encoding')
result = cursor.fetchone()
print(f'✅ SHOW client_encoding: {result[0]}')

cursor.execute('SHOW is_superuser')
result = cursor.fetchone()
print(f'✅ SHOW is_superuser: {result[0]}')

cursor.close()
conn.close()

print()
print('🎉 SUCCESS: All SHOW commands working!')
"; then
    echo ""
    echo "🎉 SUCCESS: SHOW command fix confirmed!"
    echo "✅ 'show transaction isolation level' now works correctly"
    exit 0
else
    echo ""
    echo "❌ SHOW command test failed"
    exit 1
fi