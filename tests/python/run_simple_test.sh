#!/bin/bash

# Simple SQLAlchemy Integration Test Runner (without Poetry)
# This is a fallback script that uses pip instead of Poetry

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DB="$SCRIPT_DIR/test_sqlalchemy_simple.db"
PORT=15501
PGSQLITE_PID=""

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
    fi
    
    # Clean up Unix socket
    if [[ -S "/tmp/.s.PGSQL.$PORT" ]]; then
        rm -f "/tmp/.s.PGSQL.$PORT"
    fi
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Start pgsqlite server
start_pgsqlite() {
    log_info "Starting pgsqlite server on port $PORT..."
    
    # Clean up any existing test database
    rm -f "$TEST_DB"
    
    # Build pgsqlite first
    cd "$PROJECT_ROOT"
    if ! cargo build --release; then
        log_error "Failed to build pgsqlite"
        exit 1
    fi
    
    # Start pgsqlite in background
    ./target/release/pgsqlite --database "$TEST_DB" --port $PORT > "$SCRIPT_DIR/simple_test.log" 2>&1 &
    PGSQLITE_PID=$!
    
    # Wait for server to start
    log_info "Waiting for pgsqlite to start (PID: $PGSQLITE_PID)..."
    sleep 3
    
    # Check if process is still running
    if ! kill -0 "$PGSQLITE_PID" 2>/dev/null; then
        log_error "pgsqlite failed to start. Check log:"
        cat "$SCRIPT_DIR/simple_test.log"
        exit 1
    fi
    
    # Test connection
    max_attempts=5
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
    exit 1
}

# Create simple Python test
create_simple_test() {
    cat > "$SCRIPT_DIR/simple_sqlalchemy_test.py" << 'EOF'
#!/usr/bin/env python3
import sys
import argparse

def test_basic_connection(port):
    """Test basic SQLAlchemy connection without full ORM."""
    try:
        # Try to import required modules
        from sqlalchemy import create_engine, text
        print("✅ SQLAlchemy imported successfully")
        
        # Create connection
        connection_string = f"postgresql://postgres:postgres@localhost:{port}/main"
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            # Test system functions
            result = conn.execute(text("SELECT version()")).fetchone()
            print(f"✅ version(): {result[0]}")
            
            result = conn.execute(text("SELECT current_database()")).fetchone()
            print(f"✅ current_database(): {result[0]}")
            
            result = conn.execute(text("SELECT current_user()")).fetchone()
            print(f"✅ current_user(): {result[0]}")
            
            # Test basic table creation
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            print("✅ Table created successfully")
            
            # Test insert
            conn.execute(text("""
                INSERT INTO test_table (id, name) 
                VALUES (1, 'Test Record') 
                ON CONFLICT(id) DO NOTHING
            """))
            print("✅ Insert successful")
            
            # Test select
            result = conn.execute(text("SELECT name FROM test_table WHERE id = 1")).fetchone()
            print(f"✅ Select successful: {result[0] if result else 'No data'}")
            
            # Test count
            count = conn.execute(text("SELECT COUNT(*) FROM test_table")).scalar()
            print(f"✅ Count query: {count} records")
            
            conn.commit()
        
        print("🎉 Basic SQLAlchemy test completed successfully!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please install required packages: pip install sqlalchemy psycopg2-binary")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Simple SQLAlchemy test")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_basic_connection(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
EOF
    
    chmod +x "$SCRIPT_DIR/simple_sqlalchemy_test.py"
}

# Run simple test
run_simple_test() {
    log_info "Running simple SQLAlchemy test..."
    
    create_simple_test
    
    cd "$SCRIPT_DIR"
    
    # Check if SQLAlchemy is available
    if ! python3 -c "import sqlalchemy" 2>/dev/null; then
        log_info "SQLAlchemy not found, attempting to install..."
        if ! pip3 install --user sqlalchemy psycopg2-binary; then
            log_error "Failed to install required packages"
            log_error "Please install manually: pip3 install sqlalchemy psycopg2-binary"
            exit 1
        fi
    fi
    
    # Run the test
    if python3 simple_sqlalchemy_test.py --port $PORT; then
        log_success "Simple SQLAlchemy test passed!"
        return 0
    else
        log_error "Simple SQLAlchemy test failed"
        return 1
    fi
}

# Main execution
main() {
    echo "🧪 Simple SQLAlchemy Test for pgsqlite"
    echo "======================================"
    echo ""
    
    log_info "Python version: $(python3 --version)"
    log_info "Test port: $PORT"
    echo ""
    
    start_pgsqlite
    
    if run_simple_test; then
        echo ""
        echo "🎉 SUCCESS: Simple SQLAlchemy test passed!"
        echo "✅ Basic pgsqlite + SQLAlchemy compatibility confirmed"
        exit 0
    else
        echo ""
        echo "❌ FAILURE: Simple SQLAlchemy test failed"
        
        # Show server log for debugging
        echo ""
        echo "📄 pgsqlite server log:"
        echo "======================"
        cat "$SCRIPT_DIR/simple_test.log" || true
        
        exit 1
    fi
}

main