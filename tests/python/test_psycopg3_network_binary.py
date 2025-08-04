#!/usr/bin/env python3
"""
Test psycopg3 network types with binary format.
"""

import psycopg

def test_network_binary():
    """Test network types with binary format."""
    # Connect to pgsqlite
    conn = psycopg.connect("host=localhost port=15500 user=postgres dbname=main")
    
    try:
        with conn.cursor() as cur:
            # Create test table with network columns
            cur.execute("""
                CREATE TABLE IF NOT EXISTS network_test (
                    id INTEGER PRIMARY KEY,
                    cidr_val CIDR,
                    inet_val INET,
                    mac_val MACADDR,
                    mac8_val MACADDR8
                )
            """)
            conn.commit()
            
            # Test data
            test_data = [
                (1, "192.168.1.0/24", "192.168.1.1", "08:00:2b:01:02:03", "08:00:2b:01:02:03:04:05"),
                (2, "10.0.0.0/8", "10.0.0.1/8", "aa:bb:cc:dd:ee:ff", "aa:bb:cc:dd:ee:ff:00:11"),
                (3, "172.16.0.0/16", "172.16.0.1", "00:00:00:00:00:00", "00:00:00:00:00:00:00:00"),
                (4, "2001:db8::/32", "2001:db8::1", "ff:ff:ff:ff:ff:ff", "ff:ff:ff:ff:ff:ff:ff:ff"),
                (5, "fe80::/10", "::1", "12:34:56:78:9a:bc", "12:34:56:78:9a:bc:de:f0"),
            ]
            
            # Insert with binary format
            for test_id, cidr, inet, mac, mac8 in test_data:
                cur.execute(
                    """
                    INSERT INTO network_test (id, cidr_val, inet_val, mac_val, mac8_val)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (test_id, cidr, inet, mac, mac8),
                    binary=True  # Use binary format
                )
            conn.commit()
            
            # Query with binary results
            cur.execute("SELECT * FROM network_test ORDER BY id", binary=True)
            rows = cur.fetchall()
            
            print("✅ Network binary format test successful!")
            for row in rows:
                print(f"  ID: {row[0]}")
                print(f"    CIDR: {row[1]}")
                print(f"    INET: {row[2]}")
                print(f"    MACADDR: {row[3]}")
                print(f"    MACADDR8: {row[4]}")
                print()
            
            # Test specific queries with binary format
            cur.execute(
                "SELECT cidr_val, inet_val FROM network_test WHERE id = %s",
                [1],
                binary=True
            )
            result = cur.fetchone()
            print(f"✅ Single network query: CIDR={result[0]}, INET={result[1]}")
            
            # Test NULL network values
            cur.execute(
                """
                INSERT INTO network_test (id, cidr_val)
                VALUES (%s, %s)
                """,
                (6, None),
                binary=True
            )
            conn.commit()
            
            cur.execute("SELECT cidr_val FROM network_test WHERE id = %s", [6], binary=True)
            result = cur.fetchone()
            print(f"✅ NULL network value: {result[0]}")
            
            # Test network operations
            cur.execute(
                """
                SELECT COUNT(*) FROM network_test 
                WHERE inet_val << %s
                """,
                ["192.168.0.0/16"],
                binary=True
            )
            count = cur.fetchone()[0]
            print(f"\n✅ Networks in 192.168.0.0/16: {count}")
            
            # Test MAC address operations
            cur.execute(
                """
                SELECT mac_val FROM network_test 
                WHERE mac_val = %s
                """,
                ["08:00:2b:01:02:03"],
                binary=True
            )
            mac_result = cur.fetchone()
            print(f"✅ MAC address query: {mac_result[0] if mac_result else 'Not found'}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    test_network_binary()