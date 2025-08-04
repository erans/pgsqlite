#!/bin/bash

# Comprehensive Binary Protocol Test Suite
# Tests all implemented binary types with psycopg3

set -e

echo "🚀 Starting Comprehensive Binary Protocol Test Suite"
echo "=================================================="

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to run a test
run_test() {
    local test_name="$1"
    local test_file="$2"
    
    echo -e "\n${BLUE}🧪 Running: $test_name${NC}"
    echo "----------------------------------------"
    
    if cd "$SCRIPT_DIR" && python3 "$test_file"; then
        echo -e "${GREEN}✅ $test_name PASSED${NC}"
        return 0
    else
        echo -e "${RED}❌ $test_name FAILED${NC}"
        return 1
    fi
}

# Function to start pgsqlite server
start_server() {
    echo -e "\n${YELLOW}🔧 Starting pgsqlite server...${NC}"
    cd "$PROJECT_ROOT"
    
    # Build the project
    cargo build --release
    
    # Start server in background
    ./target/release/pgsqlite --database ":memory:" --port 15500 > /tmp/pgsqlite_test.log 2>&1 &
    SERVER_PID=$!
    
    # Wait for server to start
    sleep 2
    
    # Check if server is running
    if kill -0 $SERVER_PID 2>/dev/null; then
        echo -e "${GREEN}✅ pgsqlite server started (PID: $SERVER_PID)${NC}"
        return 0
    else
        echo -e "${RED}❌ Failed to start pgsqlite server${NC}"
        cat /tmp/pgsqlite_test.log
        return 1
    fi
}

# Function to stop pgsqlite server
stop_server() {
    if [ ! -z "$SERVER_PID" ] && kill -0 $SERVER_PID 2>/dev/null; then
        echo -e "\n${YELLOW}🛑 Stopping pgsqlite server...${NC}"
        kill $SERVER_PID
        wait $SERVER_PID 2>/dev/null || true
        echo -e "${GREEN}✅ Server stopped${NC}"
    fi
}

# Trap to ensure server is stopped on exit
trap stop_server EXIT

# Test execution
FAILED_TESTS=0
TOTAL_TESTS=0

# Start the server
if ! start_server; then
    echo -e "${RED}❌ Could not start server, aborting tests${NC}"
    exit 1
fi

# Core binary types test
((TOTAL_TESTS++))
run_test "Core Binary Types" "test_psycopg3_binary.py" || ((FAILED_TESTS++))

# Array types test
((TOTAL_TESTS++))
run_test "Array Binary Types" "test_psycopg3_array_binary.py" || ((FAILED_TESTS++))

# Range types test
((TOTAL_TESTS++))
run_test "Range Binary Types" "test_psycopg3_range_binary.py" || ((FAILED_TESTS++))

# Network types test
((TOTAL_TESTS++))
run_test "Network Binary Types" "test_psycopg3_network_binary.py" || ((FAILED_TESTS++))

# Comprehensive test
((TOTAL_TESTS++))
run_test "Comprehensive Binary Protocol" "test_psycopg3_comprehensive_binary.py" || ((FAILED_TESTS++))

# Performance benchmark
((TOTAL_TESTS++))
if run_test "Binary Protocol Benchmark" "test_binary_protocol_benchmark.py"; then
    echo -e "${GREEN}📊 Benchmark completed successfully${NC}"
else
    echo -e "${YELLOW}⚠️  Benchmark had issues but continuing...${NC}"
    ((FAILED_TESTS++))
fi

# Final results
echo -e "\n${BLUE}📋 TEST SUITE SUMMARY${NC}"
echo "=================================================="
echo -e "Total Tests: $TOTAL_TESTS"
echo -e "Passed: $((TOTAL_TESTS - FAILED_TESTS))"
echo -e "Failed: $FAILED_TESTS"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "\n${GREEN}🎉 ALL BINARY PROTOCOL TESTS PASSED!${NC}"
    echo -e "${GREEN}✅ pgsqlite binary protocol implementation is working correctly${NC}"
    echo -e "${GREEN}✅ psycopg3 binary format compatibility confirmed${NC}"
    exit 0
else
    echo -e "\n${RED}❌ Some tests failed ($FAILED_TESTS/$TOTAL_TESTS)${NC}"
    echo -e "${RED}Please check the output above for details${NC}"
    exit 1
fi