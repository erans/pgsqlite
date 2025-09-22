#!/usr/bin/env python3
"""
Test script to debug PostgreSQL array binary encoding with NULLs.
Uses psycopg3 to test against pgsqlite server.
"""

import psycopg
import struct
from typing import Optional, List

def decode_binary_array(data: bytes) -> dict:
    """Decode PostgreSQL binary array format and show detailed structure."""

    if len(data) < 12:
        return {"error": f"Data too short: {len(data)} bytes"}

    offset = 0
    result = {}

    # Read header
    ndim = struct.unpack('>I', data[offset:offset+4])[0]
    offset += 4
    result['ndim'] = ndim

    dataoffset = struct.unpack('>I', data[offset:offset+4])[0]
    offset += 4
    result['dataoffset'] = dataoffset

    elemtype = struct.unpack('>I', data[offset:offset+4])[0]
    offset += 4
    result['elemtype'] = elemtype

    # Read dimensions
    dimensions = []
    for i in range(ndim):
        dim_size = struct.unpack('>I', data[offset:offset+4])[0]
        offset += 4
        lower_bound = struct.unpack('>I', data[offset:offset+4])[0]
        offset += 4
        dimensions.append({'size': dim_size, 'lower_bound': lower_bound})
    result['dimensions'] = dimensions

    # Read NULL bitmap if present
    if dataoffset > 0:
        bitmap_start = 20  # After header and 1 dimension
        bitmap_bytes = dataoffset - bitmap_start
        bitmap = data[bitmap_start:bitmap_start+bitmap_bytes]
        result['null_bitmap'] = bitmap.hex()
        result['null_bitmap_binary'] = ' '.join(f'{b:08b}' for b in bitmap)
        offset = dataoffset

    # Read elements
    elements = []
    total_elements = dimensions[0]['size'] if dimensions else 0

    for i in range(total_elements):
        elem_len = struct.unpack('>i', data[offset:offset+4])[0]
        offset += 4

        if elem_len == -1:
            elements.append({'index': i, 'length': -1, 'value': None, 'is_null': True})
        else:
            value_bytes = data[offset:offset+elem_len]
            offset += elem_len

            # For INT4, decode the value
            if elemtype == 23:  # INT4 OID
                value = struct.unpack('>i', value_bytes)[0]
            else:
                value = value_bytes.hex()

            elements.append({'index': i, 'length': elem_len, 'value': value, 'is_null': False})

    result['elements'] = elements
    result['total_bytes'] = len(data)
    result['hex'] = data.hex()

    return result

def test_array_with_nulls():
    """Test array with NULL values using psycopg3 binary protocol."""

    print("=" * 80)
    print("Testing PostgreSQL array binary encoding with NULLs")
    print("=" * 80)

    # Connect to pgsqlite server on port 5433
    conn_str = "host=localhost port=5433 user=postgres dbname=test"

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            # Create test table
            print("\n1. Creating test table...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS array_test (
                    id INTEGER PRIMARY KEY,
                    int_array INTEGER[]
                )
            """)

            # Insert test data - array with NULLs
            print("\n2. Inserting array with NULLs: [1, NULL, 3]")
            cur.execute("""
                INSERT INTO array_test (id, int_array)
                VALUES (1, ARRAY[1, NULL, 3]::INTEGER[])
                ON CONFLICT (id) DO UPDATE SET int_array = excluded.int_array
            """)

            # Query with binary result format
            print("\n3. Querying with binary protocol...")
            cur.execute("""
                SELECT int_array FROM array_test WHERE id = 1
            """, binary=True)

            row = cur.fetchone()

            if row and row[0]:
                # Get the raw binary data
                raw_data = row[0]

                print(f"\n4. Received {len(raw_data)} bytes of binary data")

                # Decode and analyze the binary format
                decoded = decode_binary_array(raw_data)

                print("\n5. Decoded array structure:")
                print(f"   - ndim: {decoded.get('ndim')}")
                print(f"   - dataoffset: {decoded.get('dataoffset')} (0x{decoded.get('dataoffset'):02x})")
                print(f"   - elemtype: {decoded.get('elemtype')} (OID for INT4)")

                if decoded.get('dimensions'):
                    print(f"   - dimensions: {decoded['dimensions']}")

                if decoded.get('null_bitmap'):
                    print(f"   - null_bitmap (hex): {decoded['null_bitmap']}")
                    print(f"   - null_bitmap (binary): {decoded['null_bitmap_binary']}")

                print("\n6. Elements:")
                for elem in decoded.get('elements', []):
                    if elem['is_null']:
                        print(f"   - Element {elem['index']}: NULL (length = -1)")
                    else:
                        print(f"   - Element {elem['index']}: {elem['value']} (length = {elem['length']})")

                print(f"\n7. Full hex dump ({decoded['total_bytes']} bytes):")
                # Print hex in rows of 16 bytes
                hex_str = decoded['hex']
                for i in range(0, len(hex_str), 32):
                    chunk = hex_str[i:i+32]
                    # Format as pairs
                    formatted = ' '.join(chunk[j:j+2] for j in range(0, len(chunk), 2))
                    print(f"   {i//2:04x}: {formatted}")

                # Now try to actually decode as psycopg would
                print("\n8. Attempting psycopg3 decoding...")
                try:
                    # This should work if our encoding is correct
                    from psycopg.types.array import IntArrayLoader
                    loader = IntArrayLoader(23, conn)  # 23 is INT4 OID
                    decoded_array = loader.load(raw_data)
                    print(f"   ✅ Successfully decoded as: {decoded_array}")
                except Exception as e:
                    print(f"   ❌ Failed to decode: {e}")

            else:
                print("No data returned!")

    print("\n" + "=" * 80)

if __name__ == "__main__":
    test_array_with_nulls()