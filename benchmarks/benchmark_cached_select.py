#!/usr/bin/env python3
"""
Benchmark cached SELECT performance vs raw SQLite
"""

import time
import sqlite3
import psycopg2
from statistics import mean, stdev
from colorama import init, Fore, Style

init(autoreset=True)

def benchmark_sqlite(iterations=1000):
    """Benchmark raw SQLite cached SELECT"""
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    # Create and populate table
    cursor.execute("DROP TABLE IF EXISTS bench_table")
    cursor.execute("""
        CREATE TABLE bench_table (
            id INTEGER PRIMARY KEY,
            value INTEGER,
            name TEXT
        )
    """)
    
    # Insert test data
    for i in range(100):
        cursor.execute("INSERT INTO bench_table (value, name) VALUES (?, ?)", (i, f"name_{i}"))
    conn.commit()
    
    # Warm up
    for _ in range(10):
        cursor.execute("SELECT * FROM bench_table WHERE value = ?", (50,))
        cursor.fetchall()
    
    # Benchmark cached queries
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        cursor.execute("SELECT * FROM bench_table WHERE value = ?", (50,))
        result = cursor.fetchall()
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms
    
    conn.close()
    return times

def benchmark_pgsqlite(port, iterations=1000):
    """Benchmark pgsqlite cached SELECT"""
    conn = psycopg2.connect(
        host='localhost',
        port=port,
        database='/tmp/bench_cached.db',
        user='dummy'
    )
    cursor = conn.cursor()
    
    # Create and populate table
    cursor.execute("DROP TABLE IF EXISTS bench_table")
    cursor.execute("""
        CREATE TABLE bench_table (
            id SERIAL PRIMARY KEY,
            value INTEGER,
            name TEXT
        )
    """)
    
    # Insert test data using prepared statements
    for i in range(100):
        cursor.execute("INSERT INTO bench_table (value, name) VALUES (%s, %s)", (i, f"name_{i}"))
    conn.commit()
    
    # Warm up
    for _ in range(10):
        cursor.execute("SELECT * FROM bench_table WHERE value = %s", (50,))
        cursor.fetchall()
    
    # Benchmark cached queries
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        cursor.execute("SELECT * FROM bench_table WHERE value = %s", (50,))
        result = cursor.fetchall()
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms
    
    conn.close()
    return times

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Benchmark cached SELECT performance')
    parser.add_argument('--iterations', type=int, default=1000, help='Number of iterations')
    parser.add_argument('--port', type=int, default=5434, help='pgsqlite port')
    args = parser.parse_args()
    
    print(f"{Fore.CYAN}{'='*60}")
    print(f"Cached SELECT Performance Benchmark")
    print(f"Iterations: {args.iterations}")
    print(f"{'='*60}{Style.RESET_ALL}")
    
    # SQLite benchmark
    print(f"\n{Fore.YELLOW}Running SQLite benchmark...{Style.RESET_ALL}")
    sqlite_times = benchmark_sqlite(args.iterations)
    sqlite_avg = mean(sqlite_times)
    sqlite_std = stdev(sqlite_times) if len(sqlite_times) > 1 else 0
    
    # pgsqlite benchmark
    print(f"{Fore.YELLOW}Running pgsqlite benchmark...{Style.RESET_ALL}")
    pgsqlite_times = benchmark_pgsqlite(args.port, args.iterations)
    pgsqlite_avg = mean(pgsqlite_times)
    pgsqlite_std = stdev(pgsqlite_times) if len(pgsqlite_times) > 1 else 0
    
    # Results
    overhead_factor = pgsqlite_avg / sqlite_avg
    overhead_percent = ((pgsqlite_avg - sqlite_avg) / sqlite_avg) * 100
    
    print(f"\n{Fore.GREEN}RESULTS:{Style.RESET_ALL}")
    print(f"SQLite:   {sqlite_avg:.4f}ms (±{sqlite_std:.4f}ms)")
    print(f"pgsqlite: {pgsqlite_avg:.4f}ms (±{pgsqlite_std:.4f}ms)")
    print(f"Overhead: {overhead_factor:.1f}x ({overhead_percent:.1f}%)")
    
    # Check against target
    target_overhead = 17.2
    target_time = 0.046
    
    if overhead_factor <= target_overhead:
        print(f"\n{Fore.GREEN}✅ PASS: Overhead {overhead_factor:.1f}x is within target {target_overhead}x{Style.RESET_ALL}")
    else:
        print(f"\n{Fore.RED}❌ FAIL: Overhead {overhead_factor:.1f}x exceeds target {target_overhead}x{Style.RESET_ALL}")
    
    if pgsqlite_avg <= target_time:
        print(f"{Fore.GREEN}✅ PASS: Time {pgsqlite_avg:.4f}ms is within target {target_time}ms{Style.RESET_ALL}")
    else:
        print(f"{Fore.YELLOW}⚠️  WARNING: Time {pgsqlite_avg:.4f}ms exceeds target {target_time}ms{Style.RESET_ALL}")
    
    # Performance analysis
    print(f"\n{Fore.CYAN}ANALYSIS:{Style.RESET_ALL}")
    if overhead_factor > 100:
        print("The overhead is primarily due to:")
        print("1. PostgreSQL wire protocol encoding/decoding")
        print("2. Network/socket communication (even localhost)")
        print("3. Query translation and validation")
        print("4. Connection session management")
    
    # Compare to previous benchmarks
    print(f"\n{Fore.CYAN}COMPARISON TO PREVIOUS:{Style.RESET_ALL}")
    print(f"Current:  {pgsqlite_avg:.4f}ms ({overhead_factor:.1f}x overhead)")
    print(f"Target:   {target_time}ms ({target_overhead}x overhead)")
    print(f"Previous: 0.159ms (31.9x overhead) - from 2025-07-29")

if __name__ == '__main__':
    main()