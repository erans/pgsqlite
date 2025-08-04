#!/usr/bin/env python
"""Test to_regtype function"""
import psycopg
import argparse

def test_to_regtype():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=15502)
    args = parser.parse_args()
    
    conn_str = f"postgresql://postgres@localhost:{args.port}/main"
    
    print(f"Testing to_regtype function on port {args.port}...")
    
    # Test with psycopg3
    with psycopg.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Test query that SQLAlchemy uses
            cur.execute("""
                SELECT
                    typname AS name, oid, typarray AS array_oid,
                    oid::regtype::text AS regtype, typdelim AS delimiter
                FROM pg_type t
                WHERE t.oid = to_regtype(%s)
                ORDER BY t.oid
            """, ('hstore',))
            
            results = cur.fetchall()
            print(f"Query returned {len(results)} rows")
            for row in results:
                print(f"  Row: {row}")
            
            # Expected: 0 rows since hstore doesn't exist
            assert len(results) == 0, f"Expected 0 rows but got {len(results)}"
            
            # Test with a type that exists
            cur.execute("""
                SELECT
                    typname AS name, oid, typarray AS array_oid,
                    oid::regtype::text AS regtype, typdelim AS delimiter
                FROM pg_type t
                WHERE t.oid = to_regtype(%s)
                ORDER BY t.oid
            """, ('integer',))
            
            results = cur.fetchall()
            print(f"\nQuery for 'integer' returned {len(results)} rows")
            for row in results:
                print(f"  Row: {row}")
            
            # Expected: 1 row
            assert len(results) == 1, f"Expected 1 row but got {len(results)}"
            assert results[0][0] == 'int4', f"Expected 'int4' but got '{results[0][0]}'"
            
    print("\n✅ All tests passed!")

if __name__ == "__main__":
    test_to_regtype()