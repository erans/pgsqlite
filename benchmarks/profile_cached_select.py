#!/usr/bin/env python3
"""
Profile cached SELECT performance to investigate regression
"""

import time
import psycopg2
import sqlite3
import argparse
import cProfile
import pstats
import io
from statistics import mean, stdev
from tabulate import tabulate
from colorama import init, Fore, Style

init(autoreset=True)

def benchmark_sqlite_cached_select(conn, iterations):
    """Benchmark SQLite cached SELECT"""
    cursor = conn.cursor()
    
    # Create and populate table
    cursor.execute("DROP TABLE IF EXISTS cache_test")
    cursor.execute("""
        CREATE TABLE cache_test (
            id INTEGER PRIMARY KEY,
            value INTEGER,
            name TEXT
        )
    """)
    
    # Insert test data
    for i in range(100):
        cursor.execute("INSERT INTO cache_test (value, name) VALUES (?, ?)", (i, f"name_{i}"))
    conn.commit()
    
    # Warm up cache with first query
    cursor.execute("SELECT * FROM cache_test WHERE value = ?", (50,))
    cursor.fetchall()
    
    # Benchmark cached queries
    times = []
    for i in range(iterations):
        # Use the same query to ensure caching
        start = time.perf_counter()
        cursor.execute("SELECT * FROM cache_test WHERE value = ?", (50,))
        result = cursor.fetchall()
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms
    
    return times

def benchmark_pgsqlite_cached_select(conn, iterations):
    """Benchmark pgsqlite cached SELECT"""
    cursor = conn.cursor()
    
    # Create and populate table
    cursor.execute("DROP TABLE IF EXISTS cache_test")
    cursor.execute("""
        CREATE TABLE cache_test (
            id SERIAL PRIMARY KEY,
            value INTEGER,
            name TEXT
        )
    """)
    
    # Insert test data using prepared statements
    for i in range(100):
        cursor.execute("INSERT INTO cache_test (value, name) VALUES (%s, %s)", (i, f"name_{i}"))
    conn.commit()
    
    # Warm up cache with first query
    cursor.execute("SELECT * FROM cache_test WHERE value = %s", (50,))
    cursor.fetchall()
    
    # Benchmark cached queries
    times = []
    for i in range(iterations):
        # Use the same query to ensure caching
        start = time.perf_counter()
        cursor.execute("SELECT * FROM cache_test WHERE value = %s", (50,))
        result = cursor.fetchall()
        end = time.perf_counter()
        times.append((end - start) * 1000)  # Convert to ms
    
    return times

def profile_pgsqlite_operations(conn):
    """Profile pgsqlite operations to find bottlenecks"""
    profiler = cProfile.Profile()
    cursor = conn.cursor()
    
    # Setup
    cursor.execute("DROP TABLE IF EXISTS profile_test")
    cursor.execute("""
        CREATE TABLE profile_test (
            id SERIAL PRIMARY KEY,
            value INTEGER,
            name TEXT
        )
    """)
    
    for i in range(10):
        cursor.execute("INSERT INTO profile_test (value, name) VALUES (%s, %s)", (i, f"name_{i}"))
    conn.commit()
    
    # Profile cached SELECT operations
    profiler.enable()
    for _ in range(100):
        cursor.execute("SELECT * FROM profile_test WHERE value = %s", (5,))
        cursor.fetchall()
    profiler.disable()
    
    # Get profile stats
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
    ps.print_stats(20)
    
    return s.getvalue()

def analyze_cache_behavior(conn, query_variations):
    """Analyze how different query patterns affect caching"""
    cursor = conn.cursor()
    results = []
    
    # Create test table
    cursor.execute("DROP TABLE IF EXISTS cache_analysis")
    cursor.execute("""
        CREATE TABLE cache_analysis (
            id SERIAL PRIMARY KEY,
            a INTEGER,
            b INTEGER,
            c TEXT
        )
    """)
    
    # Insert test data
    for i in range(50):
        cursor.execute("INSERT INTO cache_analysis (a, b, c) VALUES (%s, %s, %s)", 
                      (i, i*2, f"text_{i}"))
    conn.commit()
    
    # Test different query patterns
    patterns = [
        ("Simple WHERE", "SELECT * FROM cache_analysis WHERE a = %s", (25,)),
        ("Multiple WHERE", "SELECT * FROM cache_analysis WHERE a = %s AND b = %s", (25, 50)),
        ("LIKE pattern", "SELECT * FROM cache_analysis WHERE c LIKE %s", ("text_%",)),
        ("ORDER BY", "SELECT * FROM cache_analysis WHERE a > %s ORDER BY b", (20,)),
        ("LIMIT", "SELECT * FROM cache_analysis WHERE a > %s LIMIT 10", (10,)),
        ("Aggregate", "SELECT COUNT(*) FROM cache_analysis WHERE a > %s", (25,)),
    ]
    
    for pattern_name, query, params in patterns:
        # Warm up
        cursor.execute(query, params)
        cursor.fetchall()
        
        # Measure
        times = []
        for _ in range(query_variations):
            start = time.perf_counter()
            cursor.execute(query, params)
            cursor.fetchall()
            end = time.perf_counter()
            times.append((end - start) * 1000)
        
        avg_time = mean(times)
        std_dev = stdev(times) if len(times) > 1 else 0
        results.append((pattern_name, avg_time, std_dev, min(times), max(times)))
    
    return results

def main():
    parser = argparse.ArgumentParser(description='Profile cached SELECT performance')
    parser.add_argument('--iterations', type=int, default=1000, help='Number of iterations')
    parser.add_argument('--port', type=int, default=5434, help='pgsqlite port')
    parser.add_argument('--profile', action='store_true', help='Run profiler')
    args = parser.parse_args()
    
    print(f"{Fore.CYAN}{'='*80}")
    print(f"Cached SELECT Performance Analysis")
    print(f"Iterations: {args.iterations}")
    print(f"{'='*80}{Style.RESET_ALL}")
    
    # SQLite benchmark
    print(f"\n{Fore.YELLOW}Running SQLite benchmark...{Style.RESET_ALL}")
    sqlite_conn = sqlite3.connect(':memory:')
    sqlite_times = benchmark_sqlite_cached_select(sqlite_conn, args.iterations)
    sqlite_avg = mean(sqlite_times)
    sqlite_std = stdev(sqlite_times) if len(sqlite_times) > 1 else 0
    sqlite_conn.close()
    
    # pgsqlite benchmark
    print(f"{Fore.YELLOW}Running pgsqlite benchmark...{Style.RESET_ALL}")
    pg_conn = psycopg2.connect(
        host='localhost',
        port=args.port,
        database=':memory:',
        user='dummy'
    )
    pgsqlite_times = benchmark_pgsqlite_cached_select(pg_conn, args.iterations)
    pgsqlite_avg = mean(pgsqlite_times)
    pgsqlite_std = stdev(pgsqlite_times) if len(pgsqlite_times) > 1 else 0
    
    # Performance comparison
    overhead = ((pgsqlite_avg - sqlite_avg) / sqlite_avg) * 100
    
    print(f"\n{Fore.GREEN}PERFORMANCE COMPARISON:{Style.RESET_ALL}")
    comparison = [
        ["Metric", "SQLite", "pgsqlite", "Difference"],
        ["Average (ms)", f"{sqlite_avg:.4f}", f"{pgsqlite_avg:.4f}", f"{pgsqlite_avg - sqlite_avg:.4f}"],
        ["Std Dev (ms)", f"{sqlite_std:.4f}", f"{pgsqlite_std:.4f}", ""],
        ["Min (ms)", f"{min(sqlite_times):.4f}", f"{min(pgsqlite_times):.4f}", ""],
        ["Max (ms)", f"{max(sqlite_times):.4f}", f"{max(pgsqlite_times):.4f}", ""],
        ["Overhead", "", "", f"{overhead:+.1f}%"],
    ]
    print(tabulate(comparison, headers="firstrow", tablefmt="grid"))
    
    # Analyze cache behavior
    print(f"\n{Fore.YELLOW}Analyzing cache behavior with different query patterns...{Style.RESET_ALL}")
    cache_results = analyze_cache_behavior(pg_conn, 100)
    
    print(f"\n{Fore.GREEN}QUERY PATTERN ANALYSIS:{Style.RESET_ALL}")
    pattern_table = [
        ["Pattern", "Avg (ms)", "Std Dev", "Min (ms)", "Max (ms)"]
    ]
    for pattern, avg, std, min_t, max_t in cache_results:
        pattern_table.append([pattern, f"{avg:.4f}", f"{std:.4f}", f"{min_t:.4f}", f"{max_t:.4f}"])
    print(tabulate(pattern_table, headers="firstrow", tablefmt="grid"))
    
    # Profiling (optional)
    if args.profile:
        print(f"\n{Fore.YELLOW}Running profiler on pgsqlite...{Style.RESET_ALL}")
        profile_output = profile_pgsqlite_operations(pg_conn)
        print(f"\n{Fore.GREEN}PROFILE OUTPUT (Top 20 functions):{Style.RESET_ALL}")
        print(profile_output)
    
    # Distribution analysis
    print(f"\n{Fore.GREEN}LATENCY DISTRIBUTION:{Style.RESET_ALL}")
    
    # SQLite distribution
    percentiles = [50, 75, 90, 95, 99]
    sqlite_sorted = sorted(sqlite_times)
    pgsqlite_sorted = sorted(pgsqlite_times)
    
    dist_table = [["Percentile", "SQLite (ms)", "pgsqlite (ms)", "Difference"]]
    for p in percentiles:
        idx = int(len(sqlite_sorted) * p / 100)
        sqlite_p = sqlite_sorted[min(idx, len(sqlite_sorted)-1)]
        pgsqlite_p = pgsqlite_sorted[min(idx, len(pgsqlite_sorted)-1)]
        dist_table.append([f"p{p}", f"{sqlite_p:.4f}", f"{pgsqlite_p:.4f}", f"{pgsqlite_p - sqlite_p:.4f}"])
    
    print(tabulate(dist_table, headers="firstrow", tablefmt="grid"))
    
    # Identify bottlenecks
    print(f"\n{Fore.CYAN}ANALYSIS SUMMARY:{Style.RESET_ALL}")
    if overhead > 100:
        print(f"{Fore.RED}⚠️  High overhead detected: {overhead:.1f}%{Style.RESET_ALL}")
        print("Possible causes:")
        print("  - Protocol overhead for small result sets")
        print("  - Query translation overhead in unified processor")
        print("  - Connection management overhead")
    elif overhead > 50:
        print(f"{Fore.YELLOW}⚠️  Moderate overhead: {overhead:.1f}%{Style.RESET_ALL}")
        print("Consider optimizing:")
        print("  - Query caching strategy")
        print("  - Pattern matching in unified processor")
    else:
        print(f"{Fore.GREEN}✅ Acceptable overhead: {overhead:.1f}%{Style.RESET_ALL}")
    
    # Variance analysis
    if pgsqlite_std > sqlite_std * 2:
        print(f"\n{Fore.YELLOW}⚠️  High variance detected in pgsqlite times{Style.RESET_ALL}")
        print("This suggests inconsistent performance, possibly due to:")
        print("  - Cache misses")
        print("  - GC or memory allocation issues")
        print("  - Lock contention")
    
    pg_conn.close()

if __name__ == '__main__':
    main()