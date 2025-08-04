#!/bin/bash

# SQLAlchemy Integration Test Runner for pgsqlite
# This script builds pgsqlite, starts it in the background, sets up Poetry environment,
# and runs comprehensive SQLAlchemy ORM tests.

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DB="$SCRIPT_DIR/test_sqlalchemy_orm.db"
PORT=15500
PGSQLITE_PID=""
DRIVER="psycopg2"  # Default driver

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
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
    
    # Kill pgsqlite if running
    if [[ -n "$PGSQLITE_PID" && "$PGSQLITE_PID" != "0" ]]; then
        log_info "Stopping pgsqlite (PID: $PGSQLITE_PID)..."
        kill "$PGSQLITE_PID" 2>/dev/null || true
        wait "$PGSQLITE_PID" 2>/dev/null || true
    fi
    
    # Clean up test database
    if [[ -f "$TEST_DB" ]]; then
        rm -f "$TEST_DB"
        log_info "Removed test database"
    fi
    
    # Clean up Unix socket
    if [[ -S "/tmp/.s.PGSQL.$PORT" ]]; then
        rm -f "/tmp/.s.PGSQL.$PORT"
        log_info "Removed Unix socket"
    fi
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Poetry is installed
    if ! command -v poetry &> /dev/null; then
        log_error "Poetry is not installed. Please install it first:"
        log_error "Visit: https://python-poetry.org/docs/#installation"
        exit 1
    fi
    
    # Check Poetry version
    poetry_version=$(poetry --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
    log_info "Found Poetry version: $poetry_version"
    
    # Check if we're in the right directory
    if [[ ! -f "$PROJECT_ROOT/Cargo.toml" ]]; then
        log_error "Not in pgsqlite project root directory"
        exit 1
    fi
    
    # Check if port is available
    if command -v lsof &> /dev/null && lsof -i :$PORT &> /dev/null; then
        log_error "Port $PORT is already in use"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Build pgsqlite
build_pgsqlite() {
    log_info "Building pgsqlite in release mode..."
    
    cd "$PROJECT_ROOT"
    
    # Build pgsqlite
    if ! cargo build --release; then
        log_error "Failed to build pgsqlite"
        exit 1
    fi
    
    # Verify binary exists
    if [[ ! -f "$PROJECT_ROOT/target/release/pgsqlite" ]]; then
        log_error "pgsqlite binary not found after build"
        exit 1
    fi
    
    log_success "pgsqlite built successfully"
}

# Start pgsqlite server
start_pgsqlite() {
    log_info "Starting pgsqlite server on port $PORT..."
    
    # Clean up any existing test database
    rm -f "$TEST_DB"
    
    # Start pgsqlite in background WITHOUT pooling for proper SQLAlchemy transaction isolation
    # SQLAlchemy expects connection-per-session behavior for transaction persistence
    # Use WAL mode with synchronous=FULL for better durability
    cd "$PROJECT_ROOT"
    PGSQLITE_JOURNAL_MODE=WAL PGSQLITE_SYNCHRONOUS=FULL ./target/release/pgsqlite --database "$TEST_DB" --port $PORT > "$SCRIPT_DIR/pgsqlite.log" 2>&1 &
    PGSQLITE_PID=$!
    
    # Wait for server to start
    log_info "Waiting for pgsqlite to start (PID: $PGSQLITE_PID)..."
    sleep 3
    
    # Check if process is still running
    if ! kill -0 "$PGSQLITE_PID" 2>/dev/null; then
        log_error "pgsqlite failed to start. Check log:"
        cat "$SCRIPT_DIR/pgsqlite.log"
        exit 1
    fi
    
    # Test connection
    max_attempts=10
    attempt=1
    while [[ $attempt -le $max_attempts ]]; do
        if timeout 5 bash -c "echo > /dev/tcp/localhost/$PORT" 2>/dev/null; then
            log_success "pgsqlite is running and accepting connections"
            return 0
        fi
        
        log_info "Connection attempt $attempt/$max_attempts failed, retrying..."
        sleep 2
        ((attempt++))
    done
    
    log_error "Failed to connect to pgsqlite after $max_attempts attempts"
    log_error "Server log:"
    cat "$SCRIPT_DIR/pgsqlite.log"
    exit 1
}

# Setup Python environment
setup_python_env() {
    log_info "Setting up Python environment with Poetry..."
    
    cd "$SCRIPT_DIR"
    
    # Configure Poetry to use local .venv in this directory
    poetry config virtualenvs.in-project true --local
    poetry config virtualenvs.prefer-active-python true --local
    
    # Show Poetry configuration
    log_info "Poetry configuration:"
    poetry config --list | grep -E "(virtualenvs|cache)" || true
    
    # Install dependencies
    log_info "Installing dependencies..."
    if ! timeout 300 poetry install --only main; then
        log_error "Failed to install Python dependencies"
        log_error "Check that pyproject.toml is valid"
        exit 1
    fi
    
    # Verify installations
    log_info "Verifying installations..."
    
    if ! poetry run python -c "import sqlalchemy; print(f'✅ SQLAlchemy version: {sqlalchemy.__version__}')"; then
        log_error "SQLAlchemy installation verification failed"
        exit 1
    fi
    
    # Verify driver-specific installations
    case "$DRIVER" in
        psycopg2)
            if ! poetry run python -c "import psycopg2; print('✅ psycopg2 installed successfully')"; then
                log_error "psycopg2 installation verification failed"
                exit 1
            fi
            ;;
        psycopg3-text|psycopg3-binary)
            if ! poetry run python -c "import psycopg; print(f'✅ psycopg3 version: {psycopg.__version__}')"; then
                log_error "psycopg3 installation verification failed"
                exit 1
            fi
            ;;
        *)
            log_error "Unknown driver: $DRIVER"
            exit 1
            ;;
    esac
    
    # Show Python environment info
    poetry run python -c "
import sys
import os
print(f'✅ Python executable: {sys.executable}')
print(f'✅ Python version: {sys.version.split()[0]}')
print(f'✅ Virtual environment: {os.environ.get(\"VIRTUAL_ENV\", \"Not detected\")}')
"
    
    log_success "Python environment set up successfully"
}

# Run SQLAlchemy tests
run_tests() {
    log_info "Running SQLAlchemy ORM integration tests with driver: $DRIVER"
    
    cd "$SCRIPT_DIR"
    
    # Make test script executable
    chmod +x test_sqlalchemy_orm.py
    
    # Run the comprehensive test suite with driver option
    if poetry run python test_sqlalchemy_orm.py --port $PORT --driver $DRIVER; then
        log_success "All SQLAlchemy tests passed!"
        return 0
    else
        log_error "Some SQLAlchemy tests failed"
        return 1
    fi
}

# Show system information
show_system_info() {
    log_info "System Information:"
    echo "  Python version: $(python3 --version 2>/dev/null || echo 'Not found')"
    echo "  Poetry version: $(poetry --version 2>/dev/null || echo 'Not found')"
    echo "  Rust version: $(rustc --version 2>/dev/null || echo 'Not found')"
    echo "  pgsqlite port: $PORT"
    echo "  Test database: $TEST_DB"
    echo "  Project root: $PROJECT_ROOT"
    echo "  Script directory: $SCRIPT_DIR"
    echo ""
}

# Main execution
main() {
    echo "🧪 SQLAlchemy Integration Test Runner for pgsqlite"
    echo "================================================="
    echo ""
    
    show_system_info
    
    # Run all steps
    check_prerequisites
    build_pgsqlite
    start_pgsqlite
    setup_python_env
    
    # Run tests and capture result
    if run_tests; then
        echo ""
        echo "🎉 SUCCESS: All SQLAlchemy integration tests passed!"
        echo "✅ pgsqlite is fully compatible with SQLAlchemy ORM"
        exit 0
    else
        echo ""
        echo "❌ FAILURE: Some SQLAlchemy tests failed"
        echo "📋 Check the output above for details"
        
        # Show server log for debugging
        echo ""
        echo "📄 pgsqlite server log:"
        echo "======================"
        cat "$SCRIPT_DIR/pgsqlite.log" || true
        
        exit 1
    fi
}

# Handle command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Run comprehensive SQLAlchemy ORM integration tests for pgsqlite."
            echo ""
            echo "This script:"
            echo "  1. Builds pgsqlite in release mode"
            echo "  2. Starts pgsqlite server on port $PORT"
            echo "  3. Sets up Poetry environment with SQLAlchemy"
            echo "  4. Runs comprehensive ORM tests"
            echo "  5. Cleans up automatically"
            echo ""
            echo "Options:"
            echo "  --help, -h                Show this help message"
            echo "  --info                    Show system information only"
            echo "  --driver DRIVER           Select driver: psycopg2, psycopg3-text, psycopg3-binary"
            echo "                           (default: psycopg2)"
            echo ""
            echo "Environment variables:"
            echo "  PORT                      Override default port (default: $PORT)"
            echo ""
            echo "Examples:"
            echo "  $0                        # Run with default psycopg2 driver"
            echo "  $0 --driver psycopg3-text # Run with psycopg3 in text mode"
            echo "  $0 --driver psycopg3-binary # Run with psycopg3 in binary mode"
            echo ""
            exit 0
            ;;
        --info)
            show_system_info
            exit 0
            ;;
        --driver)
            shift
            if [[ $# -eq 0 ]]; then
                log_error "--driver requires an argument"
                exit 1
            fi
            DRIVER="$1"
            case "$DRIVER" in
                psycopg2|psycopg3-text|psycopg3-binary)
                    # Valid driver
                    ;;
                *)
                    log_error "Invalid driver: $DRIVER"
                    log_error "Valid options: psycopg2, psycopg3-text, psycopg3-binary"
                    exit 1
                    ;;
            esac
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            log_error "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run main function
main