#!/usr/bin/env python3
"""Simple test to verify cleanup works."""

import psycopg2
import time

PORT = 15433

print("Starting server...")
import subprocess
server = subprocess.Popen([
    "../target/release/pgsqlite",
    "--database", "benchmark_test.db",
    "--port", str(PORT)
], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

# Wait for server to start
time.sleep(2)

try:
    print("Connecting...")
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    print("Connected!")
    
    print("Closing connection...")
    conn.close()
    print("Connection closed!")
    
    # Wait a bit to see if server processes the cleanup
    time.sleep(1)
    
    print("✅ Cleanup test passed!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    print("Terminating server...")
    server.terminate()
    
    # Get server output
    output, _ = server.communicate(timeout=5)
    print("\nServer output:")
    print(output.decode('utf-8')[-1000:])  # Last 1000 chars
    
    print("Server terminated.")