#!/usr/bin/env python3
"""Debug to see if RowDescription is sent twice"""

import os
import tempfile
import subprocess
import time
import socket
import struct

def read_message(sock):
    """Read a PostgreSQL wire protocol message"""
    # Read message type (1 byte) and length (4 bytes)
    header = sock.recv(5)
    if len(header) < 5:
        return None, None
    
    msg_type = chr(header[0])
    msg_len = struct.unpack('!I', header[1:5])[0] - 4
    
    # Read message body
    body = b''
    while len(body) < msg_len:
        chunk = sock.recv(msg_len - len(body))
        if not chunk:
            break
        body += chunk
    
    return msg_type, body

def send_message(sock, msg_type, body):
    """Send a PostgreSQL wire protocol message"""
    if msg_type:
        msg = msg_type.encode() + struct.pack('!I', len(body) + 4) + body
    else:
        # Startup message has no type byte
        msg = struct.pack('!I', len(body) + 4) + body
    sock.send(msg)

def main():
    # Start pgsqlite
    db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_file.close()
    db_path = db_file.name
    
    port = 15445
    print(f"Starting pgsqlite on port {port}")
    env = os.environ.copy()
    env['RUST_LOG'] = 'info'
    pgsqlite_proc = subprocess.Popen([
        '../../target/release/pgsqlite',
        '--database', db_path,
        '--port', str(port)
    ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    
    time.sleep(1)
    
    try:
        # Connect using raw socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', port))
        
        # Send startup message
        startup = struct.pack('!HH', 3, 0)  # Protocol version 3.0
        startup += b'user\x00postgres\x00'
        startup += b'database\x00main\x00\x00'
        send_message(sock, None, startup)
        
        # Read authentication response
        while True:
            msg_type, body = read_message(sock)
            print(f"Received: {msg_type}")
            if msg_type == 'Z':  # ReadyForQuery
                break
        
        # Create tables
        query = b"CREATE TABLE users (id SERIAL PRIMARY KEY, username VARCHAR(50));\x00"
        send_message(sock, 'Q', query)
        while True:
            msg_type, body = read_message(sock)
            if msg_type == 'Z':
                break
        
        query = b"CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INTEGER, total_amount NUMERIC(12,2));\x00"
        send_message(sock, 'Q', query)
        while True:
            msg_type, body = read_message(sock)
            if msg_type == 'Z':
                break
        
        print("\n=== Testing Extended Protocol ===")
        
        # Parse the exact query that SQLAlchemy uses
        stmt_name = b"stmt1\x00"
        query = b"SELECT orders.id AS orders_id, orders.customer_id AS orders_customer_id, orders.total_amount AS orders_total_amount FROM orders WHERE $1::INTEGER = orders.customer_id\x00"
        param_types = struct.pack('!H', 1) + struct.pack('!I', 23)  # 1 param of type INT4
        
        parse_msg = stmt_name + query + param_types
        send_message(sock, 'P', parse_msg)
        
        # Describe the statement
        describe_msg = b'S' + stmt_name
        send_message(sock, 'D', describe_msg)
        
        # Sync
        send_message(sock, 'S', b'')
        
        print("\n=== Parse/Describe Response ===")
        row_desc_count = 0
        while True:
            msg_type, body = read_message(sock)
            print(f"Received: {msg_type}")
            
            if msg_type == 'T':  # RowDescription
                row_desc_count += 1
                print(f"  RowDescription #{row_desc_count}")
                # Parse field count
                field_count = struct.unpack('!H', body[0:2])[0]
                offset = 2
                for i in range(field_count):
                    # Read field name (null-terminated string)
                    name_end = body.find(b'\x00', offset)
                    name = body[offset:name_end].decode('utf-8')
                    offset = name_end + 1
                    
                    # Skip table OID and column ID
                    offset += 6
                    
                    # Read type OID
                    type_oid = struct.unpack('!I', body[offset:offset+4])[0]
                    offset += 4
                    
                    # Skip rest of field description
                    offset += 8
                    
                    print(f"    Field {i}: {name} - Type OID: {type_oid}")
                    if name == 'orders_total_amount' and type_oid != 1700:
                        print(f"      ^^^ ERROR: Expected NUMERIC (1700) but got {type_oid}")
            
            if msg_type == 'Z':
                break
        
        # Now Bind and Execute
        print("\n=== Bind/Execute ===")
        portal_name = b"portal1\x00"
        bind_msg = portal_name + stmt_name
        bind_msg += struct.pack('!H', 1)  # 1 param format (text)
        bind_msg += struct.pack('!H', 0)  # text format
        bind_msg += struct.pack('!H', 1)  # 1 param value
        bind_msg += struct.pack('!I', 1)  # length of "1"
        bind_msg += b'1'  # value "1"
        bind_msg += struct.pack('!H', 0)  # 0 result formats (all text)
        
        send_message(sock, 'B', bind_msg)
        
        # Execute
        execute_msg = portal_name + struct.pack('!I', 0)  # no row limit
        send_message(sock, 'E', execute_msg)
        
        # Sync
        send_message(sock, 'S', b'')
        
        while True:
            msg_type, body = read_message(sock)
            print(f"Received: {msg_type}")
            
            if msg_type == 'T':  # RowDescription
                row_desc_count += 1
                print(f"  RowDescription #{row_desc_count} (UNEXPECTED during Execute!)")
                # Parse field count
                field_count = struct.unpack('!H', body[0:2])[0]
                offset = 2
                for i in range(field_count):
                    # Read field name (null-terminated string)
                    name_end = body.find(b'\x00', offset)
                    name = body[offset:name_end].decode('utf-8')
                    offset = name_end + 1
                    
                    # Skip table OID and column ID
                    offset += 6
                    
                    # Read type OID
                    type_oid = struct.unpack('!I', body[offset:offset+4])[0]
                    offset += 4
                    
                    # Skip rest of field description
                    offset += 8
                    
                    print(f"    Field {i}: {name} - Type OID: {type_oid}")
                    if name == 'orders_total_amount' and type_oid != 1700:
                        print(f"      ^^^ BUG: Sending TEXT ({type_oid}) instead of NUMERIC (1700)")
            
            if msg_type == 'Z':
                break
        
        print(f"\n=== Total RowDescription messages: {row_desc_count} ===")
        if row_desc_count > 1:
            print("BUG CONFIRMED: RowDescription sent multiple times!")
        
        sock.close()
        
    finally:
        pgsqlite_proc.terminate()
        pgsqlite_proc.wait()
        os.unlink(db_path)

if __name__ == '__main__':
    main()