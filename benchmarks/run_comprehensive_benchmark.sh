#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Comprehensive pgsqlite Benchmark ===${NC}"
echo -e "${CYAN}Date: $(date)${NC}"
echo "======================================="

# Change to project root
cd "$(dirname "$0")/.."

# Build pgsqlite in release mode
echo -e "\n${GREEN}Building pgsqlite in release mode...${NC}"
cargo build --release --quiet

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
    echo -e "${RED}Could not find a free port${NC}"
    exit 1
fi

echo -e "${CYAN}Using port: $PGSQLITE_PORT${NC}"

# Function to run benchmark with specific driver
run_benchmark() {
    local driver=$1
    local label=$2
    
    echo -e "\n${YELLOW}=== Running $label Benchmark ===${NC}"
    
    # Start pgsqlite
    RUST_LOG=error ./target/release/pgsqlite -p $PGSQLITE_PORT --in-memory --socket-dir /tmp &
    PGSQLITE_PID=$!
    sleep 2
    
    # Check if server is running
    if ! kill -0 $PGSQLITE_PID 2>/dev/null; then
        echo -e "${RED}Failed to start pgsqlite server${NC}"
        exit 1
    fi
    
    # Run benchmark
    cd benchmarks
    poetry run python benchmark_drivers.py --port $PGSQLITE_PORT --socket-dir /tmp --driver $driver --iterations 1000
    cd ..
    
    # Kill server
    kill $PGSQLITE_PID 2>/dev/null || true
    wait $PGSQLITE_PID 2>/dev/null || true
    rm -f /tmp/.s.PGSQL.$PGSQLITE_PORT
    
    sleep 1
}

# Install dependencies
echo -e "\n${GREEN}Setting up Python environment...${NC}"
cd benchmarks
poetry install --quiet
cd ..

# Run benchmarks
run_benchmark "psycopg2" "psycopg2"
run_benchmark "psycopg3-text" "psycopg3 (text mode)"

# Summary
echo -e "\n${YELLOW}=== Benchmark Summary ===${NC}"
echo -e "${CYAN}Documented Performance Targets (2025-07-27):${NC}"
echo "- SELECT: ~674.9x overhead (0.669ms)"
echo "- SELECT (cached): ~17.2x overhead (0.046ms)"
echo "- UPDATE: ~50.9x overhead (0.059ms)"
echo "- DELETE: ~35.8x overhead (0.034ms)"
echo "- INSERT: ~36.6x overhead (0.060ms)"
echo ""
echo -e "${CYAN}Documented Current Performance (2025-08-01):${NC}"
echo "- SELECT: ~389,541.9% overhead (4.016ms)"
echo "- SELECT (cached): ~2,892.9% overhead (0.079ms)"
echo "- UPDATE: ~4,591.1% overhead (0.053ms)"
echo "- DELETE: ~3,560.5% overhead (0.033ms)"
echo "- INSERT: ~3,665.4% overhead (0.060ms)"
echo ""
echo -e "${CYAN}Historical Benchmark Data (from FINAL_PERFORMANCE_ANALYSIS.md):${NC}"
echo "- Baseline INSERT: 0.174ms"
echo "- With optimizations INSERT: 0.596ms (3.4x regression)"
echo "- Baseline SELECT: 3.827ms"
echo "- With optimizations SELECT: 3.031ms (21% improvement)"
echo ""
echo -e "${GREEN}Benchmark complete! Compare the results above with historical data.${NC}"