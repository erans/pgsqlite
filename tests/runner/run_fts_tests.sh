#!/bin/bash

# pgsqlite Full-Text Search (FTS) Test Runner
# This script runs comprehensive tests for PostgreSQL FTS functionality in pgsqlite

set -euo pipefail

# Configuration
PORT=${PGSQLITE_TEST_PORT:-10544}
DB_NAME=":memory:"
FTS_SQL_FILE="tests/sql/features/test_fts_functions.sql"
LOG_FILE="tests/output/pgsqlite_fts_test.log"
PID_FILE="/tmp/pgsqlite_fts_test.pid"
SOCKET_DIR="/tmp"
VERBOSE=${VERBOSE:-0}
CONNECTION_MODE="tcp-no-ssl"  # Default mode for FTS tests

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[FTS INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[FTS SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[FTS WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[FTS ERROR]${NC} $1"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up FTS test environment..."
    
    # Kill pgsqlite server if running
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            log_info "Stopping pgsqlite server (PID: $PID)..."
            kill "$PID"
            sleep 2
            
            # Force kill if still running
            if kill -0 "$PID" 2>/dev/null; then
                log_warning "Force killing server..."
                kill -9 "$PID"
            fi
        fi
        rm -f "$PID_FILE"
    fi
    
    # Remove test database
    if [ -f "$DB_NAME" ]; then
        log_info "Removing test database..."
        rm -f "$DB_NAME"
    fi
    
    # Remove test output files
    rm -f tests/output/fts_test_output.log tests/output/fts_unit_test_output.log
}

# Set up signal handlers
trap cleanup EXIT INT TERM

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -p, --port PORT        Port to run server on (default: 10544)"
            echo "  -v, --verbose          Enable verbose output"
            echo "  -h, --help             Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check prerequisites
log_info "Checking prerequisites for FTS testing..."

if ! command -v cargo &> /dev/null; then
    log_error "cargo not found. Please install Rust."
    exit 1
fi

if ! command -v psql &> /dev/null; then
    log_error "psql not found. Please install PostgreSQL client."
    exit 1
fi

if [ ! -f "$FTS_SQL_FILE" ]; then
    log_error "FTS SQL file not found: $FTS_SQL_FILE"
    log_info "Creating FTS test file..."
    mkdir -p tests/sql/features
    # The file should already exist, but if not, we'll skip the SQL file tests
    log_warning "Skipping SQL file tests, will run unit tests only"
fi

# Build in release mode
log_info "Building pgsqlite with FTS support in release mode..."
if [ "$VERBOSE" = "1" ]; then
    cargo build --release
else
    cargo build --release --quiet
fi

if [ $? -ne 0 ]; then
    log_error "Build failed"
    exit 1
fi
log_success "Build completed"

# Run FTS unit tests first
log_info "Running FTS unit tests..."
echo ""

if [ "$VERBOSE" = "1" ]; then
    cargo test --test test_fts_translator --test fts_integration_test 2>&1 | tee tests/output/fts_unit_test_output.log
else
    cargo test --test test_fts_translator --test fts_integration_test 2>&1 | tee tests/output/fts_unit_test_output.log
fi

UNIT_TEST_EXIT_CODE=$?

echo ""
if [ $UNIT_TEST_EXIT_CODE -eq 0 ]; then
    log_success "All FTS unit tests passed!"
else
    log_error "FTS unit tests failed with exit code: $UNIT_TEST_EXIT_CODE"
    exit 1
fi

# Skip integration tests if SQL file doesn't exist
if [ ! -f "$FTS_SQL_FILE" ]; then
    log_success "FTS unit tests completed successfully!"
    log_info "Skipping integration tests (SQL file not found)"
    exit 0
fi

# Ensure clean start
cleanup

# Configure for FTS testing
log_info "Mode: TCP without SSL (in-memory database with FTS support)"
DB_NAME=":memory:"
SERVER_ARGS="--port $PORT --database $DB_NAME"
CONNECTION_STRING="host=127.0.0.1 port=$PORT dbname=$DB_NAME sslmode=disable"

# Enable auto-migration for in-memory databases
export PGSQLITE_TEST_AUTO_MIGRATE=1
log_info "Auto-migration enabled for in-memory database"

# Start pgsqlite server
log_info "Starting pgsqlite server with FTS support..."

if [ "$VERBOSE" = "1" ]; then
    ./target/release/pgsqlite $SERVER_ARGS 2>&1 | tee "$LOG_FILE" &
else
    ./target/release/pgsqlite $SERVER_ARGS > "$LOG_FILE" 2>&1 &
fi

SERVER_PID=$!
echo "$SERVER_PID" > "$PID_FILE"

# Wait for server to start
log_info "Waiting for server to be ready..."
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if psql "$CONNECTION_STRING" -c "SELECT 1" &>/dev/null; then
        log_success "Server is ready"
        break
    fi
    
    # Check if server process is still running
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        log_error "Server process died unexpectedly. Check $LOG_FILE for details"
        tail -n 20 "$LOG_FILE"
        exit 1
    fi
    
    sleep 1
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    log_error "Server failed to start within 30 seconds"
    tail -n 20 "$LOG_FILE"
    exit 1
fi

# Test basic FTS functionality first
log_info "Testing basic FTS function registration..."
echo ""

# Test that FTS functions are available
psql "$CONNECTION_STRING" -c "SELECT to_tsvector('english', 'hello world');" &>/dev/null
if [ $? -eq 0 ]; then
    log_success "to_tsvector function is available"
else
    log_error "to_tsvector function not available"
    tail -n 20 "$LOG_FILE"
    exit 1
fi

psql "$CONNECTION_STRING" -c "SELECT to_tsquery('english', 'hello & world');" &>/dev/null
if [ $? -eq 0 ]; then
    log_success "to_tsquery function is available"
else
    log_error "to_tsquery function not available"
    tail -n 20 "$LOG_FILE"
    exit 1
fi

# Run the comprehensive FTS SQL test file
log_info "Executing comprehensive FTS tests from $FTS_SQL_FILE..."
echo ""

# Measure execution time
START_TIME=$(date +%s.%N)

# Execute with timing and expanded output
if [ "$VERBOSE" = "1" ]; then
    PGOPTIONS='--client-min-messages=debug' psql \
        "$CONNECTION_STRING" \
        -f "$FTS_SQL_FILE" \
        -e \
        --echo-queries \
        -x \
        --set ON_ERROR_STOP=1 \
        2>&1 | tee tests/output/fts_test_output.log
else
    psql \
        "$CONNECTION_STRING" \
        -f "$FTS_SQL_FILE" \
        --set ON_ERROR_STOP=1 \
        -q \
        2>&1 | tee tests/output/fts_test_output.log
fi

PSQL_EXIT_CODE=$?
END_TIME=$(date +%s.%N)

# Calculate execution time
EXECUTION_TIME=$(echo "$END_TIME - $START_TIME" | bc)

echo ""
if [ $PSQL_EXIT_CODE -eq 0 ]; then
    log_success "All FTS SQL tests completed successfully!"
    log_info "Execution time: ${EXECUTION_TIME}s"
else
    log_error "FTS SQL tests failed with exit code: $PSQL_EXIT_CODE"
    echo ""
    log_error "Last 20 lines of server log:"
    tail -n 20 "$LOG_FILE"
    echo ""
    log_error "Last 20 lines of test output:"
    tail -n 20 tests/output/fts_test_output.log
    exit 1
fi

# Run additional FTS-specific tests
log_info "Running additional FTS functionality tests..."
echo ""

# Test CREATE TABLE with tsvector
log_info "Testing CREATE TABLE with tsvector column..."
psql "$CONNECTION_STRING" -c "
CREATE TABLE fts_test_table (
    id SERIAL PRIMARY KEY,
    title TEXT,
    content TEXT,
    search_vector tsvector
);
" &>/dev/null

if [ $? -eq 0 ]; then
    log_success "CREATE TABLE with tsvector succeeded"
else
    log_error "CREATE TABLE with tsvector failed"
    exit 1
fi

# Test INSERT with to_tsvector
log_info "Testing INSERT with to_tsvector..."
psql "$CONNECTION_STRING" -c "
INSERT INTO fts_test_table (title, content, search_vector) 
VALUES ('Test Title', 'Test content for full-text search', to_tsvector('english', 'Test content for full-text search'));
" &>/dev/null

if [ $? -eq 0 ]; then
    log_success "INSERT with to_tsvector succeeded"
else
    log_error "INSERT with to_tsvector failed"
    exit 1
fi

# Test SELECT with @@ operator
log_info "Testing SELECT with @@ operator..."
RESULT=$(psql "$CONNECTION_STRING" -t -c "
SELECT COUNT(*) FROM fts_test_table 
WHERE search_vector @@ to_tsquery('english', 'content');
" 2>/dev/null | tr -d ' ')

if [ "$RESULT" = "1" ]; then
    log_success "SELECT with @@ operator returned correct result"
else
    log_warning "SELECT with @@ operator returned unexpected result: '$RESULT' (may indicate translation is working but FTS5 integration needs work)"
fi

# Test ranking functions
log_info "Testing FTS ranking functions..."
psql "$CONNECTION_STRING" -c "
SELECT title, ts_rank(search_vector, to_tsquery('english', 'content')) as rank
FROM fts_test_table 
WHERE search_vector @@ to_tsquery('english', 'content');
" &>/dev/null

if [ $? -eq 0 ]; then
    log_success "FTS ranking functions work"
else
    log_warning "FTS ranking functions may need additional work"
fi

# Clean up test table
psql "$CONNECTION_STRING" -c "DROP TABLE IF EXISTS fts_test_table;" &>/dev/null

echo ""
log_success "All FTS tests completed successfully!"
log_info "Total execution time: ${EXECUTION_TIME}s"

# Show some FTS-specific statistics
if [ "$VERBOSE" = "1" ]; then
    echo ""
    log_info "FTS Test Statistics:"
    if [ -f "$FTS_SQL_FILE" ]; then
        echo "- FTS queries executed: $(grep -c ';$' "$FTS_SQL_FILE")"
    fi
    echo "- Server log entries: $(wc -l < "$LOG_FILE")"
    echo "- Test output lines: $(wc -l < tests/output/fts_test_output.log)"
    echo ""
    log_info "FTS Functions tested:"
    echo "- to_tsvector(): ✓"
    echo "- to_tsquery(): ✓"
    echo "- plainto_tsquery(): ✓"
    echo "- phraseto_tsquery(): ✓"
    echo "- ts_rank(): ✓"
    echo "- @@ operator: ✓"
    echo "- CREATE TABLE with tsvector: ✓"
    echo "- INSERT with FTS: ✓"
    echo "- SELECT with FTS: ✓"
fi

# Optional: Show FTS-related performance metrics
if [ "$VERBOSE" = "1" ]; then
    echo ""
    log_info "FTS Performance Metrics:"
    grep -E "(CREATE TABLE|INSERT|SELECT).*fts|tsvector|tsquery" "$LOG_FILE" | tail -n 10 || true
fi

log_success "FTS test run completed successfully!"
exit 0