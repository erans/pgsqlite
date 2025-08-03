#!/bin/bash

# Clean up any existing processes and files
pkill -f pgsqlite 2>/dev/null || true
rm -f benchmark.db* 2>/dev/null

echo "Running Unix socket benchmark with TEXT format..."
poetry run python benchmark.py --file-based --socket-dir /tmp

echo ""
echo "Cleaning up for next test..."
pkill -f pgsqlite 2>/dev/null || true
rm -f benchmark.db* 2>/dev/null

echo ""
echo "Running Unix socket benchmark with BINARY format..."
poetry run python benchmark.py --file-based --socket-dir /tmp --binary-format

echo ""
echo "Benchmarks completed!"