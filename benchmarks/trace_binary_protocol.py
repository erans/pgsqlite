#!/usr/bin/env python3
"""Trace the PostgreSQL protocol messages to debug binary format hang"""

import socket
import struct
import sys
import time

def trace_protocol():
    # Start server first
    import subprocess
    proc = subprocess.Popen([
        "/home/eran/work/pgsqlite/target/release/pgsqlite",
        "--database", ":memory:",
        "--port", "5436"
    ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
    time.sleep(2)
    
    try:
        # Connect with raw socket to trace protocol
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 5436))
        
        print("Connected to server")
        
        # Send startup message
        params = b"user\x00dummy\x00database\x00:memory:\x00\x00"
        msg_len = 4 + 4 + len(params)  # length + protocol version + params
        startup_msg = struct.pack('>ii', msg_len, 196608) + params
        
        print(f"Sending startup message (len={msg_len})")
        sock.send(startup_msg)
        
        # Read authentication response
        while True:
            msg_type = sock.recv(1)
            if not msg_type:
                break
                
            print(f"Received message type: {msg_type} ({chr(msg_type[0]) if msg_type[0] > 32 else 'control'})")
            
            # Read message length
            msg_len_bytes = sock.recv(4)
            msg_len = struct.unpack('>i', msg_len_bytes)[0] - 4
            
            # Read message body
            if msg_len > 0:
                body = sock.recv(msg_len)
                print(f"  Length: {msg_len}, Body: {body[:50]}...")
            
            # Check for ReadyForQuery
            if msg_type[0] == ord('Z'):
                print("Server is ready for query")
                break
        
        # Now test binary cursor query
        print("\n--- Testing Binary Format Query ---")
        
        # Send Parse message for "SELECT 1"
        query = b"SELECT 1\x00"
        stmt_name = b"\x00"  # unnamed statement
        param_count = struct.pack('>h', 0)  # no parameters
        
        parse_body = stmt_name + query + param_count
        parse_msg = b'P' + struct.pack('>i', 4 + len(parse_body)) + parse_body
        
        print("Sending Parse message")
        sock.send(parse_msg)
        
        # Send Bind message with binary result format
        portal_name = b"\x00"  # unnamed portal
        stmt_name = b"\x00"     # unnamed statement
        param_format_count = struct.pack('>h', 0)  # no parameters
        param_count = struct.pack('>h', 0)         # no parameters
        result_format_count = struct.pack('>h', 1) # 1 result column
        result_format = struct.pack('>h', 1)       # binary format (1)
        
        bind_body = (portal_name + stmt_name + param_format_count + 
                    param_count + result_format_count + result_format)
        bind_msg = b'B' + struct.pack('>i', 4 + len(bind_body)) + bind_body
        
        print("Sending Bind message with binary result format")
        sock.send(bind_msg)
        
        # Send Execute message
        portal_name = b"\x00"  # unnamed portal
        max_rows = struct.pack('>i', 0)  # no limit
        
        exec_body = portal_name + max_rows
        exec_msg = b'E' + struct.pack('>i', 4 + len(exec_body)) + exec_body
        
        print("Sending Execute message")
        sock.send(exec_msg)
        
        # Send Sync message
        sync_msg = b'S' + struct.pack('>i', 4)
        print("Sending Sync message")
        sock.send(sync_msg)
        
        # Read responses
        print("\n--- Reading responses ---")
        timeout_start = time.time()
        sock.settimeout(5.0)  # 5 second timeout
        
        try:
            while time.time() - timeout_start < 10:
                msg_type = sock.recv(1)
                if not msg_type:
                    print("Connection closed by server")
                    break
                    
                print(f"Received: {msg_type} ({chr(msg_type[0]) if msg_type[0] > 32 else 'control'})")
                
                msg_len_bytes = sock.recv(4)
                msg_len = struct.unpack('>i', msg_len_bytes)[0] - 4
                
                if msg_len > 0:
                    body = sock.recv(msg_len)
                    print(f"  Length: {msg_len}")
                    if msg_type[0] == ord('D'):  # DataRow
                        print(f"  DataRow: {body}")
                    elif msg_type[0] == ord('E'):  # Error
                        print(f"  Error: {body}")
                
                if msg_type[0] == ord('Z'):  # ReadyForQuery
                    print("Query completed successfully!")
                    break
                    
        except socket.timeout:
            print("TIMEOUT: No response from server - this is where it hangs!")
            
    finally:
        sock.close()
        proc.terminate()
        proc.wait()
        print("\nServer stopped")

if __name__ == "__main__":
    trace_protocol()