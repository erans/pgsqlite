#!/bin/bash
set -e

# Colors
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${YELLOW}Running Comprehensive Benchmarks${NC}"
echo "=================================="
echo ""

cd "$(dirname "$0")/.."

# Build
echo -e "${GREEN}Building pgsqlite...${NC}"
cargo build --release 2>/dev/null

# Find port
PORT=$((RANDOM % 50000 + 10000))
echo -e "${CYAN}Using port: $PORT${NC}"

# Setup
cd benchmarks
poetry install 2>/dev/null

# Start server with all logging suppressed
cd ..
RUST_LOG=error ./target/release/pgsqlite -p $PORT --in-memory --socket-dir /tmp >/dev/null 2>&1 &
PID=$!
sleep 2

# Run psycopg2 benchmark
echo -e "\n${GREEN}Running psycopg2 benchmark...${NC}"
cd benchmarks
poetry run python benchmark_drivers.py --port $PORT --socket-dir /tmp --driver psycopg2 --iterations 1000 2>/dev/null | tee psycopg2_results.txt

# Run psycopg3-text benchmark  
echo -e "\n${GREEN}Running psycopg3-text benchmark...${NC}"
poetry run python benchmark_drivers.py --port $PORT --socket-dir /tmp --driver psycopg3-text --iterations 1000 2>/dev/null | tee psycopg3_results.txt

# Cleanup
kill $PID 2>/dev/null
wait $PID 2>/dev/null
rm -f /tmp/.s.PGSQL.$PORT

echo -e "\n${YELLOW}Results saved to:${NC}"
echo "  - benchmarks/psycopg2_results.txt"
echo "  - benchmarks/psycopg3_results.txt"