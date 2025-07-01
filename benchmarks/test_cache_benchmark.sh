#!/bin/bash

echo "Starting pgsqlite server in background..."
# Start pgsqlite on port 5433 to avoid conflicts
../target/debug/pgsqlite --port 5433 --in-memory &
PGSQLITE_PID=$!

# Wait for server to start
sleep 2

echo "Running benchmark with cached queries..."
poetry run python benchmark.py -i 200 -b 50 --port 5433

# Kill the pgsqlite server
kill $PGSQLITE_PID

echo "Benchmark complete!"