#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}pgsqlite Benchmark Runner${NC}"
echo "=========================="

# Change to project root
cd "$(dirname "$0")/.."

# Step 1: Build pgsqlite in release mode
echo -e "\n${GREEN}[1/4] Building pgsqlite in release mode...${NC}"
cargo build --release

# Step 2: Set up Python environment
echo -e "\n${GREEN}[2/4] Setting up Python environment...${NC}"
cd benchmarks

# Check if Poetry is installed
if ! command -v poetry &> /dev/null; then
    echo -e "${RED}Poetry is not installed. Please install Poetry first:${NC}"
    echo "curl -sSL https://install.python-poetry.org | python3 -"
    exit 1
fi

# Install dependencies using Poetry with local .venv
poetry config virtualenvs.in-project true
poetry install

# Step 3: Start pgsqlite server with random port
echo -e "\n${GREEN}[3/4] Starting pgsqlite server...${NC}"
cd ..

# Function to find a free port
find_free_port() {
    local port
    for i in {1..3}; do
        # Generate random port between 10000-60000
        port=$((RANDOM % 50000 + 10000))
        
        # Check if port is free
        if ! lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            echo $port
            return 0
        fi
    done
    return 1
}

# Find a free port
PGSQLITE_PORT=$(find_free_port)
if [ $? -ne 0 ]; then
    echo -e "${RED}Could not find a free port after 3 attempts${NC}"
    exit 1
fi

echo -e "${YELLOW}Using port: $PGSQLITE_PORT${NC}"

# Check connection mode - default to Unix socket
SOCKET_DIR="/tmp"
if [[ "$@" == *"--tcp"* ]]; then
    SOCKET_DIR=""
    echo -e "${YELLOW}Using TCP mode${NC}"
else
    echo -e "${YELLOW}Using Unix socket mode (default)${NC}"
fi

# Check driver mode - default to psycopg2
DRIVER="psycopg2"
if [[ "$@" == *"--driver psycopg3-text"* ]]; then
    DRIVER="psycopg3-text"
    echo -e "${YELLOW}Using psycopg3 text protocol${NC}"
elif [[ "$@" == *"--driver psycopg3-binary"* ]]; then
    DRIVER="psycopg3-binary"
    echo -e "${YELLOW}Using psycopg3 binary protocol${NC}"
else
    echo -e "${YELLOW}Using psycopg2 (default)${NC}"
fi

# Check if --file-based flag was passed to use file-based mode
if [[ "$@" == *"--file-based"* ]]; then
    echo -e "${YELLOW}Starting pgsqlite with file-based database${NC}"
    if [ -n "$SOCKET_DIR" ]; then
        ./target/release/pgsqlite -p $PGSQLITE_PORT -d benchmark_test.db --socket-dir $SOCKET_DIR &
    else
        ./target/release/pgsqlite -p $PGSQLITE_PORT -d benchmark_test.db &
    fi
    PGSQLITE_PID=$!
else
    echo -e "${YELLOW}Starting pgsqlite with in-memory database (default)${NC}"
    if [ -n "$SOCKET_DIR" ]; then
        ./target/release/pgsqlite -p $PGSQLITE_PORT --in-memory --socket-dir $SOCKET_DIR &
    else
        ./target/release/pgsqlite -p $PGSQLITE_PORT --in-memory &
    fi
    PGSQLITE_PID=$!
fi

# Give the server time to start
sleep 2

# Check if server is running
if ! kill -0 $PGSQLITE_PID 2>/dev/null; then
    echo -e "${RED}Failed to start pgsqlite server${NC}"
    exit 1
fi

echo -e "${GREEN}pgsqlite server started with PID: $PGSQLITE_PID on port $PGSQLITE_PORT${NC}"

# Step 4: Run benchmarks
echo -e "\n${GREEN}[4/4] Running benchmarks...${NC}"
cd benchmarks

# Run the benchmark with Poetry, passing the port, driver, and socket dir if applicable
if [ -n "$SOCKET_DIR" ]; then
    poetry run python benchmark.py --port $PGSQLITE_PORT --socket-dir $SOCKET_DIR --driver $DRIVER "$@"
else
    poetry run python benchmark.py --port $PGSQLITE_PORT --driver $DRIVER "$@"
fi

# Cleanup
echo -e "\n${YELLOW}Cleaning up...${NC}"
kill $PGSQLITE_PID 2>/dev/null || true
wait $PGSQLITE_PID 2>/dev/null || true

# Remove the database file (only if using file-based mode)
if [[ "$@" == *"--file-based"* ]]; then
    rm -f ../benchmark_test.db
fi

# Remove Unix socket file if it exists
if [ -n "$SOCKET_DIR" ]; then
    rm -f $SOCKET_DIR/.s.PGSQL.$PGSQLITE_PORT
fi

echo -e "${GREEN}Benchmark complete!${NC}"