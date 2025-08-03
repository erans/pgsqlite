#!/usr/bin/env python3
"""Trace actual benchmark operations to identify the regression."""

import psycopg2
import psycopg
import time
import random

PORT = 15432

def trace_benchmark_style_operations():
    """Simulate the exact operations from the benchmark."""
    
    # Text format test
    print("=== TEXT FORMAT (psycopg2) ===")
    conn_text = psycopg2.connect(
        host='/tmp',
        port=PORT,
        dbname='main',
        user='postgres'
    )
    cur_text = conn_text.cursor()
    
    # Create table exactly like benchmark
    cur_text.execute("DROP TABLE IF EXISTS benchmark_table_pg")
    cur_text.execute("""CREATE TABLE benchmark_table_pg (
        id SERIAL PRIMARY KEY,
        text_col TEXT,
        int_col INTEGER,
        real_col REAL,
        bool_col BOOLEAN
    )""")
    conn_text.commit()
    
    # Simulate mixed operations like the benchmark
    text_times = []
    data_ids = []
    
    for i in range(20):  # Reduced iterations for tracing
        operation = random.choice(["INSERT", "UPDATE", "DELETE", "SELECT"])
        
        if operation == "INSERT" or (operation in ["UPDATE", "DELETE", "SELECT"] and not data_ids):
            # INSERT with RETURNING
            start = time.perf_counter()
            cur_text.execute(
                "INSERT INTO benchmark_table_pg (text_col, int_col, real_col, bool_col) VALUES (%s, %s, %s, %s) RETURNING id",
                (f"text_{i}", i*10, i*1.5, True)
            )
            result_id = cur_text.fetchone()[0]
            conn_text.commit()
            elapsed = (time.perf_counter() - start) * 1000
            text_times.append(elapsed)
            data_ids.append(result_id)
            print(f"  INSERT (text): {elapsed:.3f}ms")
            
    cur_text.close()
    conn_text.close()
    
    # Binary format test
    print("\n=== BINARY FORMAT (psycopg3 + autocommit) ===")
    conn_binary = psycopg.connect(
        f"host=/tmp port={PORT} dbname=main user=postgres",
        autocommit=True
    )
    cur_binary = conn_binary.cursor(binary=True)
    
    # Create table
    cur_binary.execute("DROP TABLE IF EXISTS benchmark_table_pg_binary")
    cur_binary.execute("""CREATE TABLE benchmark_table_pg_binary (
        id SERIAL PRIMARY KEY,
        text_col TEXT,
        int_col INTEGER,
        real_col REAL,
        bool_col BOOLEAN
    )""")
    
    # Same operations
    binary_times = []
    data_ids = []
    
    for i in range(20):
        operation = random.choice(["INSERT", "UPDATE", "DELETE", "SELECT"])
        
        if operation == "INSERT" or (operation in ["UPDATE", "DELETE", "SELECT"] and not data_ids):
            # INSERT with RETURNING
            start = time.perf_counter()
            cur_binary.execute(
                "INSERT INTO benchmark_table_pg_binary (text_col, int_col, real_col, bool_col) VALUES (%s, %s, %s, %s) RETURNING id",
                (f"text_{i}", i*10, i*1.5, True)
            )
            result_id = cur_binary.fetchone()[0]
            elapsed = (time.perf_counter() - start) * 1000
            binary_times.append(elapsed)
            data_ids.append(result_id)
            print(f"  INSERT (binary): {elapsed:.3f}ms")
    
    cur_binary.close()
    conn_binary.close()
    
    # Compare
    avg_text = sum(text_times) / len(text_times)
    avg_binary = sum(binary_times) / len(binary_times)
    print(f"\nAverage INSERT times:")
    print(f"  Text format:   {avg_text:.3f}ms")
    print(f"  Binary format: {avg_binary:.3f}ms")
    print(f"  Binary overhead: {avg_binary/avg_text:.1f}x slower")

def main():
    trace_benchmark_style_operations()

if __name__ == "__main__":
    main()