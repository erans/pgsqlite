#!/usr/bin/env python3
"""Test if autocommit mode is causing the binary format overhead."""

import psycopg
import time
import statistics

PORT = 15432
ITERATIONS = 100

def test_binary_with_autocommit():
    """Test binary format with autocommit (like main benchmark)."""
    conn = psycopg.connect(
        f"host=/tmp port={PORT} dbname=main user=postgres",
        autocommit=True  # AUTOCOMMIT MODE
    )
    cur = conn.cursor(binary=True)
    
    cur.execute("DROP TABLE IF EXISTS autocommit_test")
    cur.execute("CREATE TABLE autocommit_test (id INTEGER, value TEXT)")
    
    times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO autocommit_test VALUES (%s, %s)", (i, f"test_{i}"))
        times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    
    return statistics.mean(times), statistics.median(times)

def test_binary_without_autocommit():
    """Test binary format without autocommit."""
    conn = psycopg.connect(
        f"host=/tmp port={PORT} dbname=main user=postgres",
        autocommit=False  # NO AUTOCOMMIT
    )
    cur = conn.cursor(binary=True)
    
    cur.execute("DROP TABLE IF EXISTS no_autocommit_test")
    cur.execute("CREATE TABLE no_autocommit_test (id INTEGER, value TEXT)")
    conn.commit()
    
    times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO no_autocommit_test VALUES (%s, %s)", (i, f"test_{i}"))
        conn.commit()
        times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    
    return statistics.mean(times), statistics.median(times)

def test_text_with_autocommit():
    """Test text format with autocommit for comparison."""
    conn = psycopg.connect(
        f"host=/tmp port={PORT} dbname=main user=postgres",
        autocommit=True
    )
    cur = conn.cursor()  # Text format
    
    cur.execute("DROP TABLE IF EXISTS text_autocommit_test")
    cur.execute("CREATE TABLE text_autocommit_test (id INTEGER, value TEXT)")
    
    times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO text_autocommit_test VALUES (%s, %s)", (i, f"test_{i}"))
        times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    
    return statistics.mean(times), statistics.median(times)

def main():
    print("Testing autocommit impact on binary format performance...\n")
    
    # Test all combinations
    binary_auto_avg, binary_auto_med = test_binary_with_autocommit()
    print(f"Binary + Autocommit:    avg={binary_auto_avg:.3f}ms, median={binary_auto_med:.3f}ms")
    
    binary_no_auto_avg, binary_no_auto_med = test_binary_without_autocommit()
    print(f"Binary + No Autocommit: avg={binary_no_auto_avg:.3f}ms, median={binary_no_auto_med:.3f}ms")
    
    text_auto_avg, text_auto_med = test_text_with_autocommit()
    print(f"Text + Autocommit:      avg={text_auto_avg:.3f}ms, median={text_auto_med:.3f}ms")
    
    print(f"\nBinary autocommit overhead: {binary_auto_avg / binary_no_auto_avg:.1f}x slower")
    print(f"Binary vs Text (both autocommit): {binary_auto_avg / text_auto_avg:.1f}x slower")

if __name__ == "__main__":
    main()