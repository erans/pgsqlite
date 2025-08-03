#!/usr/bin/env python3
"""Trace execution path differences between text and binary formats."""

import psycopg2
import psycopg
import time

PORT = 15432

def trace_simple_insert():
    """Trace a simple INSERT to see execution path differences."""
    
    print("=== TEXT FORMAT TRACE ===")
    conn_text = psycopg2.connect(
        host='/tmp',
        port=PORT,
        dbname='main',
        user='postgres'
    )
    cur_text = conn_text.cursor()
    
    # Create table
    cur_text.execute("DROP TABLE IF EXISTS trace_test")
    cur_text.execute("CREATE TABLE trace_test (id INTEGER, name TEXT)")
    conn_text.commit()
    
    # Single INSERT with timing
    print("Executing INSERT with text format...")
    start = time.perf_counter()
    cur_text.execute("INSERT INTO trace_test (id, name) VALUES (%s, %s)", (1, "test"))
    conn_text.commit()
    text_time = (time.perf_counter() - start) * 1000
    print(f"Text format INSERT: {text_time:.3f}ms")
    
    cur_text.close()
    conn_text.close()
    
    print("\n=== BINARY FORMAT TRACE ===")
    conn_binary = psycopg.connect(
        f"host=/tmp port={PORT} dbname=main user=postgres",
        autocommit=True
    )
    cur_binary = conn_binary.cursor(binary=True)
    
    # Create table
    cur_binary.execute("DROP TABLE IF EXISTS trace_test_binary")
    cur_binary.execute("CREATE TABLE trace_test_binary (id INTEGER, name TEXT)")
    
    # Single INSERT with timing
    print("Executing INSERT with binary format...")
    start = time.perf_counter()
    cur_binary.execute("INSERT INTO trace_test_binary (id, name) VALUES (%s, %s)", (1, "test"))
    binary_time = (time.perf_counter() - start) * 1000
    print(f"Binary format INSERT: {binary_time:.3f}ms")
    print(f"Binary overhead: {binary_time - text_time:.3f}ms ({binary_time/text_time:.1f}x slower)")
    
    cur_binary.close()
    conn_binary.close()

def trace_parameter_conversion():
    """Test different parameter types to identify conversion overhead."""
    
    conn_binary = psycopg.connect(
        f"host=/tmp port={PORT} dbname=main user=postgres",
        autocommit=True
    )
    cur = conn_binary.cursor(binary=True)
    
    # Create table with various types
    cur.execute("DROP TABLE IF EXISTS type_test")
    cur.execute("""
        CREATE TABLE type_test (
            int_col INTEGER,
            text_col TEXT,
            float_col REAL,
            bool_col BOOLEAN,
            date_col DATE,
            timestamp_col TIMESTAMP
        )
    """)
    
    print("\n=== PARAMETER TYPE CONVERSION TIMING ===")
    
    # Test different parameter types
    test_cases = [
        ("Integer", [42, "text", 3.14, True, None, None]),
        ("Text", [None, "test string", None, None, None, None]),
        ("Float", [None, None, 3.14159, None, None, None]),
        ("Boolean", [None, None, None, True, None, None]),
        ("All types", [123, "hello", 2.5, False, "2025-08-03", "2025-08-03 10:30:00"])
    ]
    
    for name, params in test_cases:
        times = []
        for _ in range(10):
            start = time.perf_counter()
            cur.execute(
                "INSERT INTO type_test VALUES (%s, %s, %s, %s, %s, %s)",
                params
            )
            times.append((time.perf_counter() - start) * 1000)
        
        avg_time = sum(times) / len(times)
        print(f"{name}: {avg_time:.3f}ms avg")
    
    cur.close()
    conn_binary.close()

def main():
    trace_simple_insert()
    trace_parameter_conversion()

if __name__ == "__main__":
    main()