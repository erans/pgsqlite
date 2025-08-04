#!/usr/bin/env python3
"""
Benchmark test comparing text vs binary protocol performance.
"""

import psycopg
import json
import time
import statistics
from decimal import Decimal
import uuid

def benchmark_operation(conn, description, operation_func, iterations=100):
    """Benchmark a specific operation with timing."""
    times = []
    
    for i in range(iterations):
        start_time = time.perf_counter()
        operation_func(conn, i)
        end_time = time.perf_counter()
        times.append(end_time - start_time)
    
    avg_time = statistics.mean(times)
    median_time = statistics.median(times)
    min_time = min(times)
    max_time = max(times)
    
    print(f"  {description}:")
    print(f"    Average: {avg_time*1000:.3f}ms")
    print(f"    Median:  {median_time*1000:.3f}ms")
    print(f"    Min:     {min_time*1000:.3f}ms")
    print(f"    Max:     {max_time*1000:.3f}ms")
    
    return avg_time

def test_binary_protocol_benchmark():
    """Benchmark binary vs text protocol performance."""
    # Connect to pgsqlite
    conn = psycopg.connect("host=localhost port=15500 user=postgres dbname=main")
    
    try:
        with conn.cursor() as cur:
            # Create benchmark table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS benchmark_test (
                    id INTEGER PRIMARY KEY,
                    numeric_val NUMERIC(15, 4),
                    uuid_val UUID,
                    json_val JSONB,
                    timestamp_val TIMESTAMP,
                    int_array INTEGER[],
                    text_val TEXT,
                    money_val MONEY,
                    range_val INT4RANGE,
                    inet_val INET
                )
            """)
            conn.commit()
            
            # Clear any existing data
            cur.execute("DELETE FROM benchmark_test")
            conn.commit()
            
            print("🏁 Starting Binary Protocol Performance Benchmark")
            print("=" * 60)
            
            # Test data for benchmarks
            test_uuid = str(uuid.uuid4())
            test_json = '{"benchmark": true, "iteration": 0, "data": [1, 2, 3, 4, 5]}'
            test_array = json.dumps([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            
            # Benchmark 1: INSERT operations
            print("\n📝 Benchmark 1: INSERT Operations (100 iterations)")
            
            def insert_text_format(conn, iteration):
                with conn.cursor() as c:
                    c.execute("""
                        INSERT INTO benchmark_test 
                        (id, numeric_val, uuid_val, json_val, timestamp_val, int_array, text_val, money_val, range_val, inet_val)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        iteration * 2,
                        Decimal("12345.6789"),
                        test_uuid,
                        test_json,
                        "2024-01-15 14:30:45.123456",
                        test_array,
                        f"Text format iteration {iteration}",
                        "$1234.56",
                        "[1,100)",
                        "192.168.1.1"
                    ))
                    conn.commit()
            
            def insert_binary_format(conn, iteration):
                with conn.cursor() as c:
                    c.execute("""
                        INSERT INTO benchmark_test 
                        (id, numeric_val, uuid_val, json_val, timestamp_val, int_array, text_val, money_val, range_val, inet_val)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        iteration * 2 + 1,
                        Decimal("12345.6789"),
                        test_uuid,
                        test_json,
                        "2024-01-15 14:30:45.123456",
                        test_array,
                        f"Binary format iteration {iteration}",
                        "$1234.56",
                        "[1,100)",
                        "192.168.1.1"
                    ), binary=True)
                    conn.commit()
            
            text_insert_time = benchmark_operation(conn, "Text Format INSERT", insert_text_format)
            binary_insert_time = benchmark_operation(conn, "Binary Format INSERT", insert_binary_format)
            
            insert_speedup = text_insert_time / binary_insert_time
            print(f"  🚀 Binary INSERT is {insert_speedup:.2f}x {'faster' if insert_speedup > 1 else 'slower'} than text")
            
            # Benchmark 2: SELECT operations
            print("\n📖 Benchmark 2: SELECT Operations (100 iterations)")
            
            def select_text_format(conn, iteration):
                with conn.cursor() as c:
                    c.execute("SELECT * FROM benchmark_test WHERE id = %s", [iteration * 2])
                    return c.fetchone()
            
            def select_binary_format(conn, iteration):
                with conn.cursor() as c:
                    c.execute("SELECT * FROM benchmark_test WHERE id = %s", [iteration * 2 + 1], binary=True)
                    return c.fetchone()
            
            text_select_time = benchmark_operation(conn, "Text Format SELECT", select_text_format)
            binary_select_time = benchmark_operation(conn, "Binary Format SELECT", select_binary_format)
            
            select_speedup = text_select_time / binary_select_time
            print(f"  🚀 Binary SELECT is {select_speedup:.2f}x {'faster' if select_speedup > 1 else 'slower'} than text")
            
            # Benchmark 3: Complex queries with aggregations
            print("\n🔢 Benchmark 3: Complex Aggregation Queries (50 iterations)")
            
            def complex_query_text(conn, iteration):
                with conn.cursor() as c:
                    c.execute("""
                        SELECT 
                            COUNT(*) as total,
                            AVG(CAST(SUBSTRING(money_val, 2) AS NUMERIC)) as avg_money,
                            MAX(numeric_val) as max_numeric,
                            MIN(timestamp_val) as min_time
                        FROM benchmark_test 
                        WHERE id % 10 = %s
                    """, [iteration % 10])
                    return c.fetchone()
            
            def complex_query_binary(conn, iteration):
                with conn.cursor() as c:
                    c.execute("""
                        SELECT 
                            COUNT(*) as total,
                            AVG(CAST(SUBSTRING(money_val, 2) AS NUMERIC)) as avg_money,
                            MAX(numeric_val) as max_numeric,
                            MIN(timestamp_val) as min_time
                        FROM benchmark_test 
                        WHERE id % 10 = %s
                    """, [iteration % 10], binary=True)
                    return c.fetchone()
            
            text_complex_time = benchmark_operation(conn, "Text Format Complex Query", complex_query_text, 50)
            binary_complex_time = benchmark_operation(conn, "Binary Format Complex Query", complex_query_binary, 50)
            
            complex_speedup = text_complex_time / binary_complex_time
            print(f"  🚀 Binary Complex Query is {complex_speedup:.2f}x {'faster' if complex_speedup > 1 else 'slower'} than text")
            
            # Benchmark 4: Bulk data operations
            print("\n📦 Benchmark 4: Bulk Data Transfer (10 iterations, 100 rows each)")
            
            def bulk_select_text(conn, iteration):
                with conn.cursor() as c:
                    c.execute("SELECT * FROM benchmark_test LIMIT 100 OFFSET %s", [iteration * 10])
                    return c.fetchall()
            
            def bulk_select_binary(conn, iteration):
                with conn.cursor() as c:
                    c.execute("SELECT * FROM benchmark_test LIMIT 100 OFFSET %s", [iteration * 10], binary=True)
                    return c.fetchall()
            
            text_bulk_time = benchmark_operation(conn, "Text Format Bulk SELECT", bulk_select_text, 10)
            binary_bulk_time = benchmark_operation(conn, "Binary Format Bulk SELECT", bulk_select_binary, 10)
            
            bulk_speedup = text_bulk_time / binary_bulk_time
            print(f"  🚀 Binary Bulk SELECT is {bulk_speedup:.2f}x {'faster' if bulk_speedup > 1 else 'slower'} than text")
            
            # Summary
            print("\n📊 BENCHMARK SUMMARY")
            print("=" * 60)
            print(f"INSERT Operations:     Binary is {insert_speedup:.2f}x {'faster' if insert_speedup > 1 else 'slower'}")
            print(f"SELECT Operations:     Binary is {select_speedup:.2f}x {'faster' if select_speedup > 1 else 'slower'}")
            print(f"Complex Queries:       Binary is {complex_speedup:.2f}x {'faster' if complex_speedup > 1 else 'slower'}")
            print(f"Bulk Data Transfer:    Binary is {bulk_speedup:.2f}x {'faster' if bulk_speedup > 1 else 'slower'}")
            
            overall_speedup = statistics.mean([insert_speedup, select_speedup, complex_speedup, bulk_speedup])
            print(f"\nOverall Average:       Binary is {overall_speedup:.2f}x {'faster' if overall_speedup > 1 else 'slower'}")
            
            # Data volume comparison
            cur.execute("SELECT COUNT(*) FROM benchmark_test")
            total_rows = cur.fetchone()[0]
            print(f"\nData Volume: {total_rows} rows processed during benchmark")
            
            # Type-specific insights
            print("\n🔍 Type-Specific Binary Protocol Benefits:")
            print("  • NUMERIC/DECIMAL: Precise binary representation vs string parsing")
            print("  • UUID: 16-byte binary vs 36-character string (55% size reduction)")
            print("  • TIMESTAMP: 8-byte binary vs string parsing")
            print("  • JSONB: Version header + efficient binary encoding")
            print("  • Arrays: Compact binary format with NULL bitmap")
            print("  • Network Types: Binary address format vs string parsing")
            
            if overall_speedup > 1:
                print(f"\n✅ Binary protocol shows {overall_speedup:.1f}x overall performance improvement!")
            else:
                print(f"\n⚠️  Text protocol performed {1/overall_speedup:.1f}x better overall")
                print("   This could be due to overhead in binary encoding/decoding")
            
    finally:
        conn.close()

if __name__ == "__main__":
    test_binary_protocol_benchmark()