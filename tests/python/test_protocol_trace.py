import socket
import struct
import hashlib
from datetime import datetime

def send_startup(sock):
    """Send startup message"""
    params = b'user\x00postgres\x00database\x00main\x00\x00'
    msg_len = 4 + 4 + len(params)
    msg = struct.pack('>II', msg_len, 196608) + params
    sock.send(msg)
    
def read_message(sock):
    """Read a backend message"""
    msg_type = sock.recv(1)
    if not msg_type:
        return None, None
    msg_len = struct.unpack('>I', sock.recv(4))[0]
    msg_data = sock.recv(msg_len - 4) if msg_len > 4 else b''
    return msg_type, msg_data

def send_query(sock, query):
    """Send simple query"""
    msg = b'Q' + struct.pack('>I', 4 + len(query) + 1) + query.encode() + b'\x00'
    sock.send(msg)
    
# Connect
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('localhost', 15432))

# Startup
send_startup(sock)

# Read auth response
while True:
    msg_type, data = read_message(sock)
    if msg_type == b'Z':  # ReadyForQuery
        print("Ready for query")
        break
        
# Send simple query
query = "SELECT 123"
print(f"Sending query: {query}")
send_query(sock, query)

# Read response
while True:
    msg_type, data = read_message(sock)
    if not msg_type:
        break
    print(f"Got message type: {msg_type}")
    if msg_type == b'Z':
        break
        
sock.close()
