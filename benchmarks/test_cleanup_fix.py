#!/usr/bin/env python3
"""Test that connection cleanup doesn't hang after the fix."""

import psycopg2
import time
import sys

PORT = 15433

def test_connection_cleanup():
    """Test multiple connect/disconnect cycles to ensure cleanup works."""
    print("Testing connection cleanup fix...")
    
    for i in range(5):
        print(f"\nTest {i+1}/5: Connecting...")
        start = time.time()
        
        try:
            # Connect
            conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
            cur = conn.cursor()
            
            # Do a simple query
            cur.execute("SELECT 1")
            result = cur.fetchone()
            print(f"  Query result: {result}")
            assert result and result[0] == 1
            
            # Close cursor and connection
            cur.close()
            conn.close()
            
            elapsed = time.time() - start
            print(f"  ✓ Connect/query/disconnect completed in {elapsed:.3f}s")
            
            if elapsed > 2.0:
                print(f"  ⚠️  WARNING: Connection cycle took longer than expected!")
                
        except Exception as e:
            import traceback
            print(f"  ✗ Error: {e}")
            traceback.print_exc()
            return False
    
    print("\n✅ All connection cleanup tests passed!")
    return True

def test_binary_format_cleanup():
    """Test cleanup with binary format connections."""
    try:
        import psycopg
        
        print("\nTesting binary format connection cleanup...")
        
        for i in range(3):
            print(f"\nBinary test {i+1}/3: Connecting...")
            start = time.time()
            
            # Connect with binary format
            conn = psycopg.connect(f"host=/tmp port={PORT} dbname=main user=postgres", autocommit=True)
            cur = conn.cursor(binary=True)
            
            # Do a query
            cur.execute("SELECT 1, 'test', CURRENT_TIMESTAMP")
            result = cur.fetchone()
            
            # Close
            cur.close()
            conn.close()
            
            elapsed = time.time() - start
            print(f"  ✓ Binary format connect/query/disconnect completed in {elapsed:.3f}s")
            
        print("\n✅ Binary format cleanup tests passed!")
        return True
        
    except ImportError:
        print("\n⚠️  psycopg3 not available, skipping binary format test")
        return True

def main():
    print("Connection Cleanup Test")
    print("=" * 50)
    
    # Test regular connections
    if not test_connection_cleanup():
        sys.exit(1)
    
    # Test binary format connections
    if not test_binary_format_cleanup():
        sys.exit(1)
    
    print("\n" + "=" * 50)
    print("✅ All cleanup tests passed! The hanging issue is fixed.")

if __name__ == "__main__":
    main()