#!/bin/bash

# pgsqlite Test Runner
# This script runs pgsqlite server in various modes and executes comprehensive tests

set -euo pipefail

# Configuration
PORT=${PGSQLITE_TEST_PORT:-10543}
DB_NAME=":memory:"
SQL_FILE="tests/sql/core/test_queries.sql"
LOG_FILE="tests/output/pgsqlite_test.log"
PID_FILE="/tmp/pgsqlite_test.pid"
SOCKET_DIR="/tmp"
VERBOSE=${VERBOSE:-0}
EPHEMERAL_SSL=0
CONNECTION_MODE="tcp-ssl"  # Default mode: tcp-ssl, tcp-no-ssl, unix-socket, file-ssl, file-no-ssl

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    
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
    
    # Also remove any file-based test databases
    if [ -f "test_db.sqlite" ]; then
        rm -f "test_db.sqlite"
    fi
    
    # Remove SSL certificates if ephemeral
    if [ "$EPHEMERAL_SSL" = "1" ]; then
        rm -f "${DB_NAME%.db}.crt" "${DB_NAME%.db}.key"
    fi
    
    # Remove Unix socket
    if [ -f "$SOCKET_DIR/.s.PGSQL.$PORT" ]; then
        rm -f "$SOCKET_DIR/.s.PGSQL.$PORT"
    fi
    
    # Remove test output files
    rm -f tests/output/test_output.log tests/output/meta_command_output.log
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
        -d|--database)
            DB_NAME="$2"
            shift 2
            ;;
        -s|--sql-file)
            SQL_FILE="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -m|--mode)
            CONNECTION_MODE="$2"
            shift 2
            ;;
        --meta-commands)
            META_COMMAND_FILE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -p, --port PORT        Port to run server on (default: 10543)"
            echo "  -d, --database DB      Database file to use (default: :memory:)"
            echo "  -s, --sql-file FILE    SQL file to execute (default: test_queries.sql)"
            echo "  -m, --mode MODE        Connection mode: tcp-ssl, tcp-no-ssl, unix-socket, file-ssl, file-no-ssl"
            echo "  --meta-commands FILE   Meta-command test file (default: test_meta_commands_minimal.sql)"
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
log_info "Checking prerequisites..."

if ! command -v cargo &> /dev/null; then
    log_error "cargo not found. Please install Rust."
    exit 1
fi

if ! command -v psql &> /dev/null; then
    log_error "psql not found. Please install PostgreSQL client."
    exit 1
fi

if [ ! -f "$SQL_FILE" ]; then
    log_error "SQL file not found: $SQL_FILE"
    exit 1
fi

# Build in release mode
log_info "Building pgsqlite in release mode..."
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

# Ensure clean start
cleanup

# Configure based on connection mode
case "$CONNECTION_MODE" in
    "tcp-ssl")
        log_info "Mode: TCP with SSL (shared memory database)"
        DB_NAME="file::memory:?cache=shared&uri=true"
        SERVER_ARGS="--port $PORT --database \"$DB_NAME\" --ssl --ssl-ephemeral"
        EPHEMERAL_SSL=1
        CONNECTION_STRING="host=127.0.0.1 port=$PORT dbname=$DB_NAME sslmode=require"
        ;;
    "tcp-no-ssl")
        log_info "Mode: TCP without SSL (shared memory database)"
        DB_NAME="file::memory:?cache=shared&uri=true"
        SERVER_ARGS="--port $PORT --database \"$DB_NAME\""
        CONNECTION_STRING="host=127.0.0.1 port=$PORT dbname=$DB_NAME sslmode=disable"
        ;;
    "unix-socket")
        log_info "Mode: Unix socket (shared memory database)"
        DB_NAME="file::memory:?cache=shared&uri=true"
        SERVER_ARGS="--socket-dir $SOCKET_DIR --port $PORT --database \"$DB_NAME\""
        CONNECTION_STRING="host=$SOCKET_DIR port=$PORT dbname=$DB_NAME"
        ;;
    "file-ssl")
        log_info "Mode: TCP with SSL (file database)"
        DB_NAME="test_db.sqlite"
        SERVER_ARGS="--port $PORT --database $DB_NAME --ssl"
        # Check for existing certificates
        if [ ! -f "${DB_NAME%.sqlite}.crt" ] || [ ! -f "${DB_NAME%.sqlite}.key" ]; then
            log_info "SSL certificates not found, will generate ephemeral certificates"
            SERVER_ARGS="$SERVER_ARGS --ssl-ephemeral"
            EPHEMERAL_SSL=1
        fi
        CONNECTION_STRING="host=127.0.0.1 port=$PORT dbname=$DB_NAME sslmode=require"
        ;;
    "file-no-ssl")
        log_info "Mode: TCP without SSL (file database)"
        DB_NAME="test_db.sqlite"
        SERVER_ARGS="--port $PORT --database $DB_NAME"
        CONNECTION_STRING="host=127.0.0.1 port=$PORT dbname=$DB_NAME sslmode=disable"
        ;;
    *)
        log_error "Invalid connection mode: $CONNECTION_MODE"
        exit 1
        ;;
esac

# For file-based databases, run migrations first
# For in-memory databases, we'll use auto-migration via environment variable
if [[ "$CONNECTION_MODE" == "file-ssl" || "$CONNECTION_MODE" == "file-no-ssl" ]]; then
    log_info "Running migrations for file database..."
    ./target/release/pgsqlite --database "$DB_NAME" --migrate
    if [ $? -ne 0 ]; then
        log_error "Migration failed"
        exit 1
    fi
    log_success "Migrations completed"
else
    # For in-memory databases, enable auto-migration via environment variable
    export PGSQLITE_TEST_AUTO_MIGRATE=1
    log_info "Auto-migration enabled for in-memory database"
fi

# Start pgsqlite server
log_info "Starting pgsqlite server..."

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

# Run the SQL test file
log_info "Executing test queries from $SQL_FILE..."
echo ""

# Measure execution time
START_TIME=$(date +%s.%N)

# Execute with timing and expanded output
if [ "$VERBOSE" = "1" ]; then
    PGOPTIONS='--client-min-messages=debug' psql \
        "$CONNECTION_STRING" \
        -f "$SQL_FILE" \
        -e \
        --echo-queries \
        -x \
        --set ON_ERROR_STOP=1 \
        2>&1 | tee tests/output/test_output.log
else
    psql \
        "$CONNECTION_STRING" \
        -f "$SQL_FILE" \
        --set ON_ERROR_STOP=1 \
        -q \
        2>&1 | tee tests/output/test_output.log
fi

PSQL_EXIT_CODE=$?
END_TIME=$(date +%s.%N)

# Calculate execution time
EXECUTION_TIME=$(echo "$END_TIME - $START_TIME" | bc)

echo ""
if [ $PSQL_EXIT_CODE -eq 0 ]; then
    log_success "All SQL queries completed successfully!"
    log_info "Execution time: ${EXECUTION_TIME}s"
else
    log_error "SQL queries failed with exit code: $PSQL_EXIT_CODE"
    echo ""
    log_error "Last 20 lines of server log:"
    tail -n 20 "$LOG_FILE"
    exit 1
fi

# Run meta-command tests if the file exists
# Use working version by default to avoid testing unimplemented features
META_COMMAND_FILE="${META_COMMAND_FILE:-tests/sql/meta/test_meta_commands_working.sql}"
# To use full test suite, set META_COMMAND_FILE=tests/sql/meta/test_meta_commands.sql
# Available test files:
# - tests/sql/meta/test_meta_commands_supported.sql: All fully supported meta commands with examples
# - tests/sql/meta/test_meta_commands_working.sql: Commands that work with current implementation
# - tests/sql/meta/test_meta_commands_basic.sql: Very basic commands only
# - tests/sql/meta/test_meta_commands_minimal.sql: Minimal set including \d (NOW WORKS!)
# - tests/sql/meta/test_meta_commands.sql: Full test suite (many unimplemented features)
if [ -f "$META_COMMAND_FILE" ]; then
    echo ""
    log_info "Executing meta-command tests from $META_COMMAND_FILE..."
    echo ""
    
    # Measure execution time for meta commands
    META_START_TIME=$(date +%s.%N)
    
    # Execute meta commands
    if [ "$VERBOSE" = "1" ]; then
        PGOPTIONS='--client-min-messages=debug' psql \
            "$CONNECTION_STRING" \
            -f "$META_COMMAND_FILE" \
            -e \
            --echo-queries \
            -x \
            --set ON_ERROR_STOP=1 \
            2>&1 | tee tests/output/meta_command_output.log
    else
        psql \
            "$CONNECTION_STRING" \
            -f "$META_COMMAND_FILE" \
            --set ON_ERROR_STOP=1 \
            -q \
            2>&1 | tee tests/output/meta_command_output.log
    fi
    
    META_EXIT_CODE=$?
    META_END_TIME=$(date +%s.%N)
    
    # Calculate execution time
    META_EXECUTION_TIME=$(echo "$META_END_TIME - $META_START_TIME" | bc)
    
    echo ""
    if [ $META_EXIT_CODE -eq 0 ]; then
        log_success "All meta-command tests completed successfully!"
        log_info "Meta-command execution time: ${META_EXECUTION_TIME}s"
    else
        log_error "Meta-command tests failed with exit code: $META_EXIT_CODE"
        echo ""
        log_error "Failed meta-command output:"
        tail -n 50 tests/output/meta_command_output.log
        echo ""
        log_error "Last 20 lines of server log:"
        tail -n 20 "$LOG_FILE"
        exit 1
    fi
else
    log_warning "Meta-command test file $META_COMMAND_FILE not found, skipping meta-command tests"
fi

echo ""
if [ $PSQL_EXIT_CODE -eq 0 ] && ([ ! -f "$META_COMMAND_FILE" ] || [ $META_EXIT_CODE -eq 0 ]); then
    log_success "All tests completed successfully!"
    log_info "Total execution time: $(echo "$EXECUTION_TIME + ${META_EXECUTION_TIME:-0}" | bc)s"
    
    # Show some statistics
    if [ "$VERBOSE" = "1" ]; then
        echo ""
        log_info "Test Statistics:"
        echo "- Total queries executed: $(grep -c ';$' "$SQL_FILE")"
        echo "- Server log entries: $(wc -l < "$LOG_FILE")"
        echo "- Test output lines: $(wc -l < tests/output/test_output.log)"
    fi
else
    log_error "Tests failed with exit code: $PSQL_EXIT_CODE"
    echo ""
    log_error "Last 20 lines of server log:"
    tail -n 20 "$LOG_FILE"
    exit 1
fi

# Optional: Show server performance metrics
if [ "$VERBOSE" = "1" ]; then
    echo ""
    log_info "Server Performance Metrics:"
    grep -E "(SELECT|INSERT|UPDATE|DELETE).*overhead" "$LOG_FILE" | tail -n 10 || true
fi

log_success "Test run completed!"
exit 0