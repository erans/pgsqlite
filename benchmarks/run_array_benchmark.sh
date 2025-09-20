#!/bin/bash

# Run array binary protocol benchmark
# This script runs the array benchmark comparing SQLite direct access
# vs pgsqlite with psycopg3 binary protocol

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🚀 Array Binary Protocol Benchmark"
echo "=================================="

# Default values
ITERATIONS=100
PORT=15500
PGSQLITE_BIN="$PROJECT_ROOT/target/release/pgsqlite"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --iterations|-i)
            ITERATIONS="$2"
            shift 2
            ;;
        --port|-p)
            PORT="$2"
            shift 2
            ;;
        --debug)
            PGSQLITE_BIN="$PROJECT_ROOT/target/debug/pgsqlite"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --iterations, -i NUM    Number of iterations (default: 100)"
            echo "  --port, -p PORT         pgsqlite server port (default: 15500)"
            echo "  --debug                 Use debug build instead of release"
            echo "  --help, -h              Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Check if pgsqlite binary exists
if [[ ! -f "$PGSQLITE_BIN" ]]; then
    echo "❌ pgsqlite binary not found at: $PGSQLITE_BIN"
    echo "Build it first with: cargo build --release"
    exit 1
fi

# Check if Poetry is available and dependencies are installed
if ! command -v poetry &> /dev/null; then
    echo "❌ Poetry not found. Install with:"
    echo "curl -sSL https://install.python-poetry.org | python3 -"
    exit 1
fi

# Install dependencies if needed
if [[ ! -d .venv ]] || ! poetry run python -c "import psycopg, tabulate, colorama" 2>/dev/null; then
    echo "📦 Installing Python dependencies with Poetry..."
    poetry install
fi

# Create temporary database file
TEMP_DB=$(mktemp /tmp/array_benchmark_XXXXXX.db)
echo "📁 Using temporary database: $TEMP_DB"

# Function to cleanup on exit
cleanup() {
    echo "🧹 Cleaning up..."
    if [[ -n "$PGSQLITE_PID" ]]; then
        kill "$PGSQLITE_PID" 2>/dev/null || true
        wait "$PGSQLITE_PID" 2>/dev/null || true
    fi
    rm -f "$TEMP_DB"
}
trap cleanup EXIT

# Start pgsqlite server
echo "🚀 Starting pgsqlite server on port $PORT..."
"$PGSQLITE_BIN" --database "$TEMP_DB" --port "$PORT" &
PGSQLITE_PID=$!

# Wait for server to start
echo "⏳ Waiting for server to start..."
for i in {1..30}; do
    if nc -z localhost "$PORT" 2>/dev/null; then
        echo "✅ Server is ready!"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "❌ Server failed to start within 30 seconds"
        exit 1
    fi
    sleep 1
done

# Run the benchmark
echo "📊 Running array benchmark..."
echo "Settings:"
echo "  - Iterations: $ITERATIONS"
echo "  - Port: $PORT"
echo "  - Database: $TEMP_DB"
echo ""

cd "$SCRIPT_DIR"
poetry run python benchmark_array_binary.py --iterations "$ITERATIONS" --port "$PORT"

echo ""
echo "🎉 Benchmark completed successfully!"