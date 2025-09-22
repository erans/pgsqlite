#!/bin/bash
set -e

echo "🚀 Running comprehensive driver benchmark comparison"
echo "======================================================"

# Kill any existing pgsqlite processes
pkill -f pgsqlite || true
sleep 2

# Build in release mode
cargo build --release --bin pgsqlite

# Test all three drivers
drivers=("psycopg2" "psycopg3-text" "psycopg3-binary")
port=43210

for driver in "${drivers[@]}"; do
    echo ""
    echo "🔧 Testing driver: $driver"
    echo "--------------------------------"

    # Start pgsqlite server
    ./target/release/pgsqlite --database :memory: --port $port &
    server_pid=$!

    # Wait for server to start
    sleep 2

    # Run benchmark
    cd benchmarks
    python3 benchmark_drivers.py --driver $driver --port $port --pgsqlite-only --iterations 500
    cd ..

    # Stop server
    kill $server_pid || true
    wait $server_pid 2>/dev/null || true

    echo "✅ Completed $driver test"
    sleep 1
done

echo ""
echo "🎉 All driver tests completed!"