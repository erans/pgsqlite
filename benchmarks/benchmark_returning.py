#!/usr/bin/env python3
"""
Benchmark specifically for INSERT with RETURNING operations
"""

import time
import psycopg2
import sqlite3
import argparse
from tabulate import tabulate
from colorama import init, Fore, Style

init(autoreset=True)

def benchmark_sqlite_insert_returning(conn, iterations):
    """Benchmark SQLite INSERT with RETURNING (simulated)"""
    cursor = conn.cursor()
    
    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_returning (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            value INTEGER
        )
    """)
    conn.commit()
    
    # Benchmark INSERT operations
    insert_times = []
    for i in range(iterations):
        start = time.perf_counter()
        cursor.execute(
            "INSERT INTO test_returning (name, value) VALUES (?, ?)",
            (f"test_{i}", i)
        )
        # Simulate RETURNING by fetching lastrowid
        row_id = cursor.lastrowid
        end = time.perf_counter()
        insert_times.append((end - start) * 1000)  # Convert to ms
    
    conn.commit()
    return insert_times

def benchmark_pgsqlite_insert_returning(conn, iterations):
    """Benchmark pgsqlite INSERT with RETURNING"""
    cursor = conn.cursor()
    
    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_returning (
            id SERIAL PRIMARY KEY,
            name TEXT,
            value INTEGER
        )
    """)
    conn.commit()
    
    # Benchmark INSERT with RETURNING operations
    insert_times = []
    for i in range(iterations):
        start = time.perf_counter()
        cursor.execute(
            "INSERT INTO test_returning (name, value) VALUES (%s, %s) RETURNING id",
            (f"test_{i}", i)
        )
        row_id = cursor.fetchone()[0]
        end = time.perf_counter()
        insert_times.append((end - start) * 1000)  # Convert to ms
    
    conn.commit()
    return insert_times

def main():
    parser = argparse.ArgumentParser(description='Benchmark INSERT with RETURNING')
    parser.add_argument('--iterations', type=int, default=1000, help='Number of iterations')
    parser.add_argument('--port', type=int, default=5432, help='pgsqlite port')
    args = parser.parse_args()
    
    print(f"{Fore.CYAN}{'='*80}")
    print(f"INSERT WITH RETURNING BENCHMARK")
    print(f"Iterations: {args.iterations}")
    print(f"{'='*80}{Style.RESET_ALL}")
    
    # SQLite benchmark
    print(f"\n{Fore.YELLOW}Running SQLite benchmark...{Style.RESET_ALL}")
    sqlite_conn = sqlite3.connect(':memory:')
    sqlite_times = benchmark_sqlite_insert_returning(sqlite_conn, args.iterations)
    sqlite_avg = sum(sqlite_times) / len(sqlite_times)
    sqlite_min = min(sqlite_times)
    sqlite_max = max(sqlite_times)
    sqlite_conn.close()
    
    # pgsqlite benchmark
    print(f"{Fore.YELLOW}Running pgsqlite benchmark...{Style.RESET_ALL}")
    pg_conn = psycopg2.connect(
        host='localhost',
        port=args.port,
        database=':memory:',
        user='dummy'
    )
    pgsqlite_times = benchmark_pgsqlite_insert_returning(pg_conn, args.iterations)
    pgsqlite_avg = sum(pgsqlite_times) / len(pgsqlite_times)
    pgsqlite_min = min(pgsqlite_times)
    pgsqlite_max = max(pgsqlite_times)
    pg_conn.close()
    
    # Calculate overhead
    overhead = ((pgsqlite_avg - sqlite_avg) / sqlite_avg) * 100
    
    # Results table
    results = [
        ["Metric", "SQLite", "pgsqlite", "Difference", "Overhead"],
        ["Average (ms)", f"{sqlite_avg:.4f}", f"{pgsqlite_avg:.4f}", 
         f"{pgsqlite_avg - sqlite_avg:.4f}", f"{overhead:+.1f}%"],
        ["Min (ms)", f"{sqlite_min:.4f}", f"{pgsqlite_min:.4f}", 
         f"{pgsqlite_min - sqlite_min:.4f}", ""],
        ["Max (ms)", f"{sqlite_max:.4f}", f"{pgsqlite_max:.4f}", 
         f"{pgsqlite_max - sqlite_max:.4f}", ""],
        ["Total (s)", f"{sum(sqlite_times)/1000:.3f}", f"{sum(pgsqlite_times)/1000:.3f}",
         f"{(sum(pgsqlite_times) - sum(sqlite_times))/1000:.3f}", ""],
    ]
    
    print(f"\n{Fore.GREEN}RESULTS:{Style.RESET_ALL}")
    print(tabulate(results, headers="firstrow", tablefmt="grid"))
    
    # Performance verdict
    print(f"\n{Fore.CYAN}PERFORMANCE VERDICT:{Style.RESET_ALL}")
    if overhead < 100:
        print(f"{Fore.GREEN}✅ Excellent: pgsqlite overhead is under 100%{Style.RESET_ALL}")
    elif overhead < 500:
        print(f"{Fore.YELLOW}⚠️  Good: pgsqlite overhead is {overhead:.1f}%{Style.RESET_ALL}")
    elif overhead < 1000:
        print(f"{Fore.YELLOW}⚠️  Acceptable: pgsqlite overhead is {overhead:.1f}%{Style.RESET_ALL}")
    else:
        print(f"{Fore.RED}❌ Poor: pgsqlite overhead is {overhead:.1f}%{Style.RESET_ALL}")
    
    print(f"\n{Fore.CYAN}Per-operation overhead: {(pgsqlite_avg - sqlite_avg):.4f}ms{Style.RESET_ALL}")

if __name__ == '__main__':
    main()