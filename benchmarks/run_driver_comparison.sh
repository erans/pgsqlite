#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${YELLOW}pgsqlite Driver Comparison Benchmark${NC}"
echo "======================================="

# Change to project root
cd "$(dirname "$0")/.."

# Step 1: Build pgsqlite in release mode
echo -e "\n${GREEN}[1/5] Building pgsqlite in release mode...${NC}"
cargo build --release

# Step 2: Set up Python environment
echo -e "\n${GREEN}[2/5] Setting up Python environment...${NC}"
cd benchmarks

# Check if Poetry is installed
if ! command -v poetry &> /dev/null; then
    echo -e "${RED}Poetry is not installed. Please install Poetry first:${NC}"
    echo "curl -sSL https://install.python-poetry.org | python3 -"
    exit 1
fi

# Install dependencies
poetry install

# Step 3: Start pgsqlite server
echo -e "\n${GREEN}[3/5] Starting pgsqlite server...${NC}"
cd ..

# Find a free port
find_free_port() {
    local port
    for i in {1..3}; do
        port=$((RANDOM % 50000 + 10000))
        if ! lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            echo $port
            return 0
        fi
    done
    return 1
}

PGSQLITE_PORT=$(find_free_port)
if [ $? -ne 0 ]; then
    echo -e "${RED}Could not find a free port after 3 attempts${NC}"
    exit 1
fi

echo -e "${YELLOW}Using port: $PGSQLITE_PORT${NC}"

# Start pgsqlite with in-memory database
SOCKET_DIR="/tmp"
echo -e "${YELLOW}Starting pgsqlite with in-memory database${NC}"
./target/release/pgsqlite -p $PGSQLITE_PORT --in-memory --socket-dir $SOCKET_DIR &
PGSQLITE_PID=$!

# Give the server time to start
sleep 2

# Check if server is running
if ! kill -0 $PGSQLITE_PID 2>/dev/null; then
    echo -e "${RED}Failed to start pgsqlite server${NC}"
    exit 1
fi

echo -e "${GREEN}pgsqlite server started with PID: $PGSQLITE_PID on port $PGSQLITE_PORT${NC}"

# Step 4: Run benchmarks with psycopg2
echo -e "\n${GREEN}[4/5] Running benchmarks with psycopg2...${NC}"
cd benchmarks
echo ""
poetry run python benchmark_drivers.py --port $PGSQLITE_PORT --socket-dir $SOCKET_DIR --driver psycopg2

# Step 5: Run benchmarks with psycopg3-text
echo -e "\n${GREEN}[5/5] Running benchmarks with psycopg3-text...${NC}"
echo ""
poetry run python benchmark_drivers.py --port $PGSQLITE_PORT --socket-dir $SOCKET_DIR --driver psycopg3-text

# Cleanup
echo -e "\n${YELLOW}Cleaning up...${NC}"
kill $PGSQLITE_PID 2>/dev/null || true
wait $PGSQLITE_PID 2>/dev/null || true

# Remove Unix socket file if it exists
rm -f $SOCKET_DIR/.s.PGSQL.$PGSQLITE_PORT

echo -e "\n${GREEN}Driver comparison complete!${NC}"
echo -e "${BLUE}The results above show the performance comparison between psycopg2 and psycopg3-text drivers${NC}"