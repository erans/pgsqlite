#!/usr/bin/env python3
"""
Benchmark script comparing SQLite direct access vs PostgreSQL client via pgsqlite.
Supports both psycopg2 and psycopg3 drivers.
"""

import sqlite3
import time
import random
import string
import statistics
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple, Optional
from tabulate import tabulate
from colorama import init, Fore, Style
import os
import sys

# Initialize colorama
init()

@dataclass
class BenchmarkResult:
    operation: str
    sqlite_time: float
    pgsqlite_time: float
    count: int

class BenchmarkRunner:
    def __init__(self, iterations: int = 1000, batch_size: int = 100, in_memory: bool = False, 
                 port: int = 5432, socket_dir: str = None, sqlite_only: bool = False, 
                 pgsqlite_only: bool = False, driver: str = "psycopg2"):
        self.iterations = iterations
        self.batch_size = batch_size
        self.in_memory = in_memory
        self.sqlite_file = ":memory:" if in_memory else "benchmark_test.db"
        self.socket_dir = socket_dir
        self.sqlite_only = sqlite_only
        self.pgsqlite_only = pgsqlite_only
        self.driver = driver
        
        if socket_dir:
            # Use Unix socket
            self.pg_host = socket_dir
        else:
            # Use TCP
            self.pg_host = "localhost"
        self.pg_port = port
        self.pg_dbname = self.sqlite_file
        
        # Import the appropriate driver
        if driver == "psycopg2":
            import psycopg2
            self.psycopg = psycopg2
        elif driver == "psycopg3-text":
            import psycopg
            self.psycopg = psycopg
        else:
            raise ValueError(f"Unknown driver: {driver}")
        
        # Timing storage
        self.sqlite_times: Dict[str, List[float]] = {
            "CREATE": [], "INSERT": [], "UPDATE": [], "DELETE": [], "SELECT": [], "SELECT (cached)": []
        }
        self.pgsqlite_times: Dict[str, List[float]] = {
            "CREATE": [], "INSERT": [], "UPDATE": [], "DELETE": [], "SELECT": [], "SELECT (cached)": []
        }
        
    def setup(self):
        """Remove existing database file if it exists"""
        if not self.in_memory and os.path.exists(self.sqlite_file):
            os.remove(self.sqlite_file)
    
    def random_string(self, length: int) -> str:
        """Generate random string for testing"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    def random_data(self) -> Tuple[str, int, float, bool]:
        """Generate random test data"""
        return (
            self.random_string(20),
            random.randint(1, 10000),
            random.uniform(0.0, 1000.0),
            random.choice([True, False])
        )
    
    def measure_time(self, func, *args, **kwargs) -> float:
        """Measure execution time of a function"""
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        return end - start, result
    
    def run_sqlite_benchmarks(self):
        """Run benchmarks using direct SQLite access"""
        print(f"{Fore.CYAN}Running SQLite benchmarks...{Style.RESET_ALL}")
        
        conn = sqlite3.connect(self.sqlite_file)
        cursor = conn.cursor()
        
        # CREATE TABLE
        elapsed, _ = self.measure_time(
            cursor.execute,
            """CREATE TABLE IF NOT EXISTS benchmark_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text_col TEXT,
                int_col INTEGER,
                real_col REAL,
                bool_col BOOLEAN
            )"""
        )
        self.sqlite_times["CREATE"].append(elapsed)
        conn.commit()
        
        # Mixed operations with timing
        data_ids = []
        
        for i in range(self.iterations):
            operation = random.choice(["INSERT", "UPDATE", "DELETE", "SELECT"])
            
            if operation == "INSERT" or (operation in ["UPDATE", "DELETE", "SELECT"] and not data_ids):
                # INSERT
                data = self.random_data()
                elapsed, _ = self.measure_time(
                    cursor.execute,
                    "INSERT INTO benchmark_table (text_col, int_col, real_col, bool_col) VALUES (?, ?, ?, ?)",
                    data
                )
                self.sqlite_times["INSERT"].append(elapsed)
                data_ids.append(cursor.lastrowid)
                
            elif operation == "UPDATE" and data_ids:
                # UPDATE
                id_to_update = random.choice(data_ids)
                new_text = self.random_string(20)
                elapsed, _ = self.measure_time(
                    cursor.execute,
                    "UPDATE benchmark_table SET text_col = ? WHERE id = ?",
                    (new_text, id_to_update)
                )
                self.sqlite_times["UPDATE"].append(elapsed)
                
            elif operation == "DELETE" and data_ids:
                # DELETE
                id_to_delete = random.choice(data_ids)
                elapsed, _ = self.measure_time(
                    cursor.execute,
                    "DELETE FROM benchmark_table WHERE id = ?",
                    (id_to_delete,)
                )
                self.sqlite_times["DELETE"].append(elapsed)
                data_ids.remove(id_to_delete)
                
            elif operation == "SELECT" and data_ids:
                # SELECT
                elapsed, _ = self.measure_time(
                    cursor.execute,
                    "SELECT * FROM benchmark_table WHERE int_col > ?",
                    (random.randint(1, 5000),)
                )
                cursor.fetchall()  # Ensure we fetch results
                self.sqlite_times["SELECT"].append(elapsed)
            
            # Commit periodically
            if i % self.batch_size == 0:
                conn.commit()
        
        conn.commit()
        
        # Run cached query benchmarks
        print(f"{Fore.CYAN}Running SQLite cached query benchmarks...{Style.RESET_ALL}")
        
        # Define a set of queries to repeat
        cached_queries = [
            ("SELECT * FROM benchmark_table WHERE int_col > ?", (2500,)),
            ("SELECT text_col, real_col FROM benchmark_table WHERE bool_col = ?", (1,)),
            ("SELECT COUNT(*) FROM benchmark_table WHERE text_col LIKE ?", ("A%",)),
            ("SELECT AVG(real_col) FROM benchmark_table WHERE int_col BETWEEN ? AND ?", (1000, 5000)),
            ("SELECT * FROM benchmark_table ORDER BY int_col DESC LIMIT ?", (10,))
        ]
        
        # Run each query multiple times to test caching
        for _ in range(100):
            query, params = random.choice(cached_queries)
            elapsed, _ = self.measure_time(cursor.execute, query, params)
            cursor.fetchall()
            self.sqlite_times["SELECT (cached)"].append(elapsed)
        
        conn.close()
    
    def run_pgsqlite_benchmarks(self):
        """Run benchmarks using PostgreSQL client via pgsqlite"""
        driver_name = self.driver
        print(f"{Fore.CYAN}Running pgsqlite benchmarks with {driver_name}...{Style.RESET_ALL}")
        if self.socket_dir:
            print(f"Connecting to pgsqlite via Unix socket: {self.socket_dir}/.s.PGSQL.{self.pg_port}")
        else:
            print(f"Connecting to pgsqlite via TCP on port {self.pg_port}")
        
        # Connect using appropriate driver
        if self.driver == "psycopg2":
            conn = self.psycopg.connect(
                host=self.pg_host,
                port=self.pg_port,
                dbname=self.pg_dbname,
                user="dummy",  # pgsqlite doesn't use auth
                password="dummy",
                sslmode="disable"  # pgsqlite doesn't support SSL
            )
        elif self.driver == "psycopg3-text":
            # psycopg3 connection string
            if self.socket_dir:
                conninfo = f"host={self.socket_dir} port={self.pg_port} dbname={self.pg_dbname} user=dummy password=dummy sslmode=disable"
            else:
                conninfo = f"host={self.pg_host} port={self.pg_port} dbname={self.pg_dbname} user=dummy password=dummy sslmode=disable"
            conn = self.psycopg.connect(conninfo)
            # Force text mode for psycopg3
            if hasattr(conn, 'prepare_threshold'):
                conn.prepare_threshold = None  # Disable prepared statements
        
        cursor = conn.cursor()
        
        # CREATE TABLE
        elapsed, _ = self.measure_time(
            cursor.execute,
            """CREATE TABLE IF NOT EXISTS benchmark_table_pg (
                id SERIAL PRIMARY KEY,
                text_col TEXT,
                int_col INTEGER,
                real_col REAL,
                bool_col BOOLEAN
            )"""
        )
        self.pgsqlite_times["CREATE"].append(elapsed)
        conn.commit()
        
        # Mixed operations with timing
        data_ids = []
        
        for i in range(self.iterations):
            operation = random.choice(["INSERT", "UPDATE", "DELETE", "SELECT"])
            
            if operation == "INSERT" or (operation in ["UPDATE", "DELETE", "SELECT"] and not data_ids):
                # INSERT
                data = self.random_data()
                elapsed, _ = self.measure_time(
                    cursor.execute,
                    "INSERT INTO benchmark_table_pg (text_col, int_col, real_col, bool_col) VALUES (%s, %s, %s, %s) RETURNING id",
                    data
                )
                self.pgsqlite_times["INSERT"].append(elapsed)
                data_ids.append(cursor.fetchone()[0])
                
            elif operation == "UPDATE" and data_ids:
                # UPDATE
                id_to_update = random.choice(data_ids)
                new_text = self.random_string(20)
                elapsed, _ = self.measure_time(
                    cursor.execute,
                    "UPDATE benchmark_table_pg SET text_col = %s WHERE id = %s",
                    (new_text, id_to_update)
                )
                self.pgsqlite_times["UPDATE"].append(elapsed)
                
            elif operation == "DELETE" and data_ids:
                # DELETE
                id_to_delete = random.choice(data_ids)
                elapsed, _ = self.measure_time(
                    cursor.execute,
                    "DELETE FROM benchmark_table_pg WHERE id = %s",
                    (id_to_delete,)
                )
                self.pgsqlite_times["DELETE"].append(elapsed)
                data_ids.remove(id_to_delete)
                
            elif operation == "SELECT" and data_ids:
                # SELECT
                elapsed, _ = self.measure_time(
                    cursor.execute,
                    "SELECT * FROM benchmark_table_pg WHERE int_col > %s",
                    (random.randint(1, 5000),)
                )
                cursor.fetchall()  # Ensure we fetch results
                self.pgsqlite_times["SELECT"].append(elapsed)
            
            # Commit periodically
            if i % self.batch_size == 0:
                conn.commit()
        
        conn.commit()
        
        # Run cached query benchmarks
        print(f"{Fore.CYAN}Running pgsqlite cached query benchmarks with {driver_name}...{Style.RESET_ALL}")
        
        # Define a set of queries to repeat (same as SQLite but with pgsqlite table)
        cached_queries = [
            ("SELECT * FROM benchmark_table_pg WHERE int_col > %s", (2500,)),
            ("SELECT text_col, real_col FROM benchmark_table_pg WHERE bool_col = %s", (True,)),
            ("SELECT COUNT(*) FROM benchmark_table_pg WHERE text_col LIKE %s", ("A%",)),
            ("SELECT AVG(real_col) FROM benchmark_table_pg WHERE int_col BETWEEN %s AND %s", (1000, 5000)),
            ("SELECT * FROM benchmark_table_pg ORDER BY int_col DESC LIMIT %s", (10,))
        ]
        
        # Run each query multiple times to test caching
        for _ in range(100):
            query, params = random.choice(cached_queries)
            elapsed, _ = self.measure_time(cursor.execute, query, params)
            cursor.fetchall()
            self.pgsqlite_times["SELECT (cached)"].append(elapsed)
        
        cursor.close()
        conn.close()
        
    def calculate_stats(self, times: List[float]) -> Dict[str, float]:
        """Calculate statistics for a list of times"""
        if not times:
            return {"avg": 0, "min": 0, "max": 0, "median": 0, "total": 0}
        
        return {
            "avg": statistics.mean(times),
            "min": min(times),
            "max": max(times),
            "median": statistics.median(times),
            "total": sum(times)
        }
    
    def print_results(self):
        """Print benchmark results"""
        print(f"\n{Fore.GREEN}{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}BENCHMARK RESULTS{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{'='*80}{Style.RESET_ALL}")
        
        if self.driver != "psycopg2":
            print(f"{Fore.CYAN}Driver: {self.driver}{Style.RESET_ALL}")
        
        # Summary table
        summary_data = []
        
        if self.sqlite_only:
            # SQLite-only table
            for operation in ["CREATE", "INSERT", "UPDATE", "DELETE", "SELECT", "SELECT (cached)"]:
                sqlite_stats = self.calculate_stats(self.sqlite_times[operation])
                if len(self.sqlite_times[operation]) > 0:
                    summary_data.append([
                        operation,
                        len(self.sqlite_times[operation]),
                        f"{sqlite_stats['avg']*1000:.3f}",
                        f"{sqlite_stats['min']*1000:.3f}",
                        f"{sqlite_stats['max']*1000:.3f}",
                        f"{sqlite_stats['median']*1000:.3f}",
                        f"{sqlite_stats['total']:.3f}"
                    ])
            headers = ["Operation", "Count", "Avg (ms)", "Min (ms)", "Max (ms)", "Median (ms)", "Total (s)"]
        
        elif self.pgsqlite_only:
            # pgSQLite-only table
            for operation in ["CREATE", "INSERT", "UPDATE", "DELETE", "SELECT", "SELECT (cached)"]:
                pgsqlite_stats = self.calculate_stats(self.pgsqlite_times[operation])
                if len(self.pgsqlite_times[operation]) > 0:
                    summary_data.append([
                        operation,
                        len(self.pgsqlite_times[operation]),
                        f"{pgsqlite_stats['avg']*1000:.3f}",
                        f"{pgsqlite_stats['min']*1000:.3f}",
                        f"{pgsqlite_stats['max']*1000:.3f}",
                        f"{pgsqlite_stats['median']*1000:.3f}",
                        f"{pgsqlite_stats['total']:.3f}"
                    ])
            headers = ["Operation", "Count", "Avg (ms)", "Min (ms)", "Max (ms)", "Median (ms)", "Total (s)"]
        
        else:
            # Full comparison table
            for operation in ["CREATE", "INSERT", "UPDATE", "DELETE", "SELECT", "SELECT (cached)"]:
                sqlite_stats = self.calculate_stats(self.sqlite_times[operation])
                pgsqlite_stats = self.calculate_stats(self.pgsqlite_times[operation])
                
                if sqlite_stats["avg"] > 0:
                    overhead = ((pgsqlite_stats["avg"] - sqlite_stats["avg"]) / sqlite_stats["avg"]) * 100
                else:
                    overhead = 0
                
                diff_ms = (pgsqlite_stats['avg'] - sqlite_stats['avg']) * 1000
                
                summary_data.append([
                    operation,
                    len(self.sqlite_times[operation]),
                    f"{sqlite_stats['avg']*1000:.3f}",
                    f"{pgsqlite_stats['avg']*1000:.3f}",
                    f"{diff_ms:+.3f}",
                    f"{overhead:+.1f}%",
                    f"{sqlite_stats['total']:.3f}",
                    f"{pgsqlite_stats['total']:.3f}"
                ])
            
            headers = ["Operation", "Count", "SQLite Avg (ms)", "pgsqlite Avg (ms)", 
                       "Diff (ms)", "Overhead", "SQLite Total (s)", "pgsqlite Total (s)"]
        
        print(tabulate(summary_data, headers=headers, tablefmt="grid"))
        
        # Per-operation difference summary (only for full comparison)
        if not self.sqlite_only and not self.pgsqlite_only:
            print(f"\n{Fore.CYAN}Per-Operation Time Differences:{Style.RESET_ALL}")
            for operation in ["CREATE", "INSERT", "UPDATE", "DELETE", "SELECT", "SELECT (cached)"]:
                sqlite_stats = self.calculate_stats(self.sqlite_times[operation])
                pgsqlite_stats = self.calculate_stats(self.pgsqlite_times[operation])
                if len(self.sqlite_times[operation]) > 0:
                    diff_ms = (pgsqlite_stats['avg'] - sqlite_stats['avg']) * 1000
                    print(f"{operation}: {diff_ms:+.3f}ms ({Fore.GREEN if diff_ms < 0 else Fore.RED}{diff_ms:+.3f}ms{Style.RESET_ALL} avg difference per call)")
        
        # Overall statistics
        print(f"\n{Fore.CYAN}Overall Statistics:{Style.RESET_ALL}")
        
        if self.sqlite_only:
            all_sqlite_times = sum(self.sqlite_times.values(), [])
            total_sqlite = sum(all_sqlite_times)
            print(f"Total operations: {len(all_sqlite_times)}")
            print(f"Total SQLite time: {total_sqlite:.3f}s")
            
        elif self.pgsqlite_only:
            all_pgsqlite_times = sum(self.pgsqlite_times.values(), [])
            total_pgsqlite = sum(all_pgsqlite_times)
            print(f"Total operations: {len(all_pgsqlite_times)}")
            print(f"Total pgSQLite time: {total_pgsqlite:.3f}s")
            
        else:
            all_sqlite_times = sum(self.sqlite_times.values(), [])
            all_pgsqlite_times = sum(self.pgsqlite_times.values(), [])
            total_sqlite = sum(all_sqlite_times)
            total_pgsqlite = sum(all_pgsqlite_times)
            print(f"Total operations: {len(all_sqlite_times)}")
            print(f"Total SQLite time: {total_sqlite:.3f}s")
            print(f"Total pgsqlite time: {total_pgsqlite:.3f}s")
            if total_sqlite > 0:
                print(f"Overall overhead: {((total_pgsqlite - total_sqlite) / total_sqlite * 100):+.1f}%")
            
            # Cache effectiveness analysis
            if len(self.sqlite_times["SELECT"]) > 0 and len(self.sqlite_times["SELECT (cached)"]) > 0:
                print(f"\n{Fore.CYAN}Cache Effectiveness Analysis:{Style.RESET_ALL}")
                
                # SQLite cached performance
                sqlite_uncached = self.calculate_stats(self.sqlite_times["SELECT"])
                sqlite_cached = self.calculate_stats(self.sqlite_times["SELECT (cached)"])
                sqlite_cache_speedup = sqlite_uncached['avg'] / sqlite_cached['avg'] if sqlite_cached['avg'] > 0 else 1
                
                # pgsqlite cached performance
                pgsqlite_uncached = self.calculate_stats(self.pgsqlite_times["SELECT"])
                pgsqlite_cached = self.calculate_stats(self.pgsqlite_times["SELECT (cached)"])
                pgsqlite_cache_speedup = pgsqlite_uncached['avg'] / pgsqlite_cached['avg'] if pgsqlite_cached['avg'] > 0 else 1
                
                print(f"SQLite cache speedup: {sqlite_cache_speedup:.1f}x")
                print(f"pgsqlite cache speedup: {pgsqlite_cache_speedup:.1f}x")
        
    def run(self):
        """Run the complete benchmark suite"""
        self.setup()
        
        try:
            if not self.pgsqlite_only:
                self.run_sqlite_benchmarks()
            if not self.sqlite_only:
                self.run_pgsqlite_benchmarks()
            self.print_results()
        except Exception as e:
            print(f"{Fore.RED}Error during benchmark: {e}{Style.RESET_ALL}")
            raise

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark SQLite vs pgsqlite performance")
    parser.add_argument("-i", "--iterations", type=int, default=1000,
                        help="Number of operations to perform (default: 1000)")
    parser.add_argument("-b", "--batch-size", type=int, default=100,
                        help="Batch size for commits (default: 100)")
    parser.add_argument("--file-based", action="store_true",
                        help="Use file-based database instead of in-memory (default: in-memory)")
    parser.add_argument("--port", type=int, default=5432,
                        help="PostgreSQL port to connect to (default: 5432)")
    parser.add_argument("--socket-dir", type=str, default=None,
                        help="Use Unix socket in specified directory instead of TCP")
    parser.add_argument("--sqlite-only", action="store_true",
                        help="Run only SQLite benchmarks")
    parser.add_argument("--pgsqlite-only", action="store_true",
                        help="Run only pgSQLite benchmarks")
    parser.add_argument("--driver", type=str, default="psycopg2",
                        choices=["psycopg2", "psycopg3-text"],
                        help="PostgreSQL driver to use (default: psycopg2)")
    
    args = parser.parse_args()
    
    # Validate mutually exclusive options
    if args.sqlite_only and args.pgsqlite_only:
        parser.error("Cannot specify both --sqlite-only and --pgsqlite-only")
    
    # Default to in-memory mode unless --file-based is specified
    in_memory = not args.file_based
    
    runner = BenchmarkRunner(iterations=args.iterations, batch_size=args.batch_size, 
                           in_memory=in_memory, port=args.port, socket_dir=args.socket_dir,
                           sqlite_only=args.sqlite_only, pgsqlite_only=args.pgsqlite_only,
                           driver=args.driver)
    runner.run()

if __name__ == "__main__":
    main()