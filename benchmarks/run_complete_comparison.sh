#!/bin/bash

# Function to run a benchmark
run_benchmark() {
    local format=$1
    local binary_flag=$2
    local port=15432
    
    echo "======================================"
    echo "Running Unix socket benchmark with $format format..."
    echo "======================================"
    
    # Clean up
    pkill -f "pgsqlite.*port $port" 2>/dev/null || true
    rm -f benchmark.db* 2>/dev/null
    rm -f /tmp/.s.PGSQL.$port 2>/dev/null
    sleep 1
    
    # Start pgsqlite server
    cd /home/eran/work/pgsqlite
    cargo build --release 2>&1 | grep -E "(error|warning|Finished)" || true
    ./target/release/pgsqlite --database benchmarks/benchmark.db --port $port > /tmp/pgsqlite_$format.log 2>&1 &
    PGSQLITE_PID=$!
    echo "Started pgsqlite with PID: $PGSQLITE_PID on port $port"
    sleep 2
    
    # Run benchmark
    cd benchmarks
    if [ "$binary_flag" = "--binary-format" ]; then
        poetry run python benchmark.py --file-based --socket-dir /tmp --port $port --binary-format
    else
        poetry run python benchmark.py --file-based --socket-dir /tmp --port $port
    fi
    
    # Stop server
    kill $PGSQLITE_PID 2>/dev/null || true
    wait $PGSQLITE_PID 2>/dev/null || true
    
    echo ""
}

# Run benchmarks
run_benchmark "TEXT" ""
echo ""
run_benchmark "BINARY" "--binary-format"

echo "All benchmarks completed!"