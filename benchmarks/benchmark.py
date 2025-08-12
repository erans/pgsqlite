#!/usr/bin/env python3
"""
Benchmark script comparing SQLite direct access vs PostgreSQL client via pgsqlite.
Supports psycopg2, psycopg3-text, and psycopg3-binary drivers.
"""

import sqlite3
import time
import random
import string
import statistics
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple
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
        
        # Timing storage
        self.sqlite_times: Dict[str, List[float]] = {
            "CREATE": [], "INSERT": [], "UPDATE": [], "DELETE": [], "SELECT": [], "SELECT (cached)": []
        }
        self.pgsqlite_times: Dict[str, List[float]] = {
            "CREATE": [], "INSERT": [], "UPDATE": [], "DELETE": [], "SELECT": [], "SELECT (cached)": []
        }
        
        # Import and setup the appropriate driver
        self._setup_driver()
        
    def _setup_driver(self):
        """Import and configure the appropriate PostgreSQL driver"""
        if self.driver == "psycopg2":
            import psycopg2
            self.psycopg_module = psycopg2
            self.binary_format = False
            print(f"{Fore.GREEN}Using psycopg2 driver (text protocol){Style.RESET_ALL}")
        elif self.driver == "psycopg3-text":
            import psycopg
            self.psycopg_module = psycopg
            self.binary_format = False
            print(f"{Fore.GREEN}Using psycopg3 driver (text protocol){Style.RESET_ALL}")
        elif self.driver == "psycopg3-binary":
            import psycopg
            self.psycopg_module = psycopg
            self.binary_format = True
            print(f"{Fore.GREEN}Using psycopg3 driver (binary protocol){Style.RESET_ALL}")
        else:
            raise ValueError(f"Unknown driver: {self.driver}")
    
    def _get_connection(self):
        """Get PostgreSQL connection using the configured driver"""
        if self.driver == "psycopg2":
            return self.psycopg_module.connect(
                host=self.pg_host,
                port=self.pg_port,
                dbname=self.pg_dbname,
                user="dummy",  # pgsqlite doesn't use auth
                password="dummy",
                sslmode="disable"  # pgsqlite doesn't support SSL
            )
        else:  # psycopg3
            conn_str = f"host={self.pg_host} port={self.pg_port} dbname={self.pg_dbname} user=dummy password=dummy sslmode=disable"
            conn = self.psycopg_module.connect(conn_str)
            if self.binary_format:
                # Enable binary format for psycopg3
                conn.execute("SET client_encoding = 'UTF8'")
                # Note: psycopg3 automatically uses binary format when available
                # We'll use the binary parameter in cursor operations
            return conn
        
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
    
    def execute_query(self, cursor, query, params=None):
        """Execute a query with optional binary mode for psycopg3."""
        if self.driver == "psycopg3-binary" and params is not None:
            # For psycopg3 binary mode, pass binary=True to execute
            return cursor.execute(query, params, binary=True)
        elif params is not None:
            return cursor.execute(query, params)
        else:
            return cursor.execute(query)
    
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
            ("SELECT text_col, real_col FROM benchmark_table WHERE bool_col = ?", (True,)),
            ("SELECT COUNT(*) FROM benchmark_table WHERE text_col LIKE ?", ("A%",)),
            ("SELECT AVG(real_col) FROM benchmark_table WHERE int_col BETWEEN ? AND ?", (1000, 5000)),
            ("SELECT * FROM benchmark_table ORDER BY int_col DESC LIMIT ?", (10,))
        ]
        
        # Run each query multiple times to test caching
        for _ in range(20):
            for query, params in cached_queries:
                elapsed, _ = self.measure_time(cursor.execute, query, params)
                cursor.fetchall()  # Ensure we fetch all results
                self.sqlite_times["SELECT (cached)"].append(elapsed)
        
        conn.close()
        print(f"{Fore.GREEN}SQLite benchmarks completed{Style.RESET_ALL}")
    
    def run_pgsqlite_benchmarks(self):
        """Run benchmarks using PostgreSQL client via pgsqlite"""
        print(f"{Fore.CYAN}Running pgsqlite benchmarks with {self.driver}...{Style.RESET_ALL}")
        if self.socket_dir:
            print(f"Connecting to pgsqlite via Unix socket: {self.socket_dir}/.s.PGSQL.{self.pg_port}")
        else:
            print(f"Connecting to pgsqlite via TCP on port {self.pg_port}")
        
        # Connect using configured driver
        conn = self._get_connection()
        
        cursor = conn.cursor()  # Same for all drivers
        
        # CREATE TABLE
        elapsed, _ = self.measure_time(
            self.execute_query,
            cursor,
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
                    self.execute_query,
                    cursor,
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
                    self.execute_query,
                    cursor,
                    "UPDATE benchmark_table_pg SET text_col = %s WHERE id = %s",
                    (new_text, id_to_update)
                )
                self.pgsqlite_times["UPDATE"].append(elapsed)
                
            elif operation == "DELETE" and data_ids:
                # DELETE
                id_to_delete = random.choice(data_ids)
                elapsed, _ = self.measure_time(
                    self.execute_query,
                    cursor,
                    "DELETE FROM benchmark_table_pg WHERE id = %s",
                    (id_to_delete,)
                )
                self.pgsqlite_times["DELETE"].append(elapsed)
                data_ids.remove(id_to_delete)
                
            elif operation == "SELECT" and data_ids:
                # SELECT
                elapsed, _ = self.measure_time(
                    self.execute_query,
                    cursor,
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
        print(f"{Fore.CYAN}Running pgsqlite cached query benchmarks...{Style.RESET_ALL}")
        # Continue using the same connection
        
        # Define a set of queries to repeat (same as SQLite but with pgsqlite table)
        cached_queries = [
            ("SELECT * FROM benchmark_table_pg WHERE int_col > %s", (2500,)),
            ("SELECT text_col, real_col FROM benchmark_table_pg WHERE bool_col = %s", (True,)),
            ("SELECT COUNT(*) FROM benchmark_table_pg WHERE text_col LIKE %s", ("A%",)),
            ("SELECT AVG(real_col) FROM benchmark_table_pg WHERE int_col BETWEEN %s AND %s", (1000, 5000)),
            ("SELECT * FROM benchmark_table_pg ORDER BY int_col DESC LIMIT %s", (10,))
        ]
        
        # Run each query multiple times to test caching
        for _ in range(20):
            for query, params in cached_queries:
                elapsed, _ = self.measure_time(self.execute_query, cursor, query, params)
                cursor.fetchall()  # Ensure we fetch all results
                self.pgsqlite_times["SELECT (cached)"].append(elapsed)
        
        conn.close()
        print(f"{Fore.GREEN}pgsqlite benchmarks completed{Style.RESET_ALL}")
    
    def print_results(self):
        """Print benchmark results in a nice table format"""
        print("\n" + "=" * 80)
        print("BENCHMARK RESULTS")
        
        # Determine mode
        if self.sqlite_only:
            mode = "SQLite Only"
        elif self.pgsqlite_only:
            mode = f"pgsqlite Only ({self.driver})"
        else:
            mode = f"Full Comparison ({self.driver})"
        
        # Connection type
        conn_type = "Unix Socket" if self.socket_dir else "TCP"
        
        print(f"Mode: {mode}")
        print(f"Connection: {conn_type}")
        print(f"Database: {'In-Memory' if self.in_memory else 'File-Based'}")
        print("=" * 80 + "\n")
        
        results = []
        headers = []
        
        if not self.pgsqlite_only:
            headers = ["Operation", "Count", "SQLite Avg (ms)", "pgsqlite Avg (ms)", "Diff (ms)", "Overhead", "SQLite Total (s)", "pgsqlite Total (s)"]
        else:
            headers = ["Operation", "Count", "pgsqlite Avg (ms)", "pgsqlite Total (s)"]
        
        total_sqlite_time = 0
        total_pgsqlite_time = 0
        total_operations = 0
        
        for operation in ["CREATE", "INSERT", "UPDATE", "DELETE", "SELECT", "SELECT (cached)"]:
            sqlite_times = self.sqlite_times.get(operation, [])
            pgsqlite_times = self.pgsqlite_times.get(operation, [])
            
            if not self.pgsqlite_only and sqlite_times:
                sqlite_avg = statistics.mean(sqlite_times) * 1000  # Convert to ms
                sqlite_total = sum(sqlite_times)
                total_sqlite_time += sqlite_total
            else:
                sqlite_avg = 0
                sqlite_total = 0
            
            if not self.sqlite_only and pgsqlite_times:
                pgsqlite_avg = statistics.mean(pgsqlite_times) * 1000  # Convert to ms
                pgsqlite_total = sum(pgsqlite_times)
                total_pgsqlite_time += pgsqlite_total
                count = len(pgsqlite_times)
                total_operations += count
            else:
                pgsqlite_avg = 0
                pgsqlite_total = 0
                count = len(sqlite_times)
                total_operations += count
            
            if not self.pgsqlite_only:
                if sqlite_avg > 0:
                    diff = pgsqlite_avg - sqlite_avg
                    overhead = ((pgsqlite_avg / sqlite_avg - 1) * 100) if sqlite_avg > 0 else 0
                    overhead_str = f"+{overhead:.1f}%" if overhead > 0 else f"{overhead:.1f}%"
                else:
                    diff = pgsqlite_avg
                    overhead_str = "N/A"
                
                results.append([
                    operation,
                    count,
                    f"{sqlite_avg:.3f}",
                    f"{pgsqlite_avg:.3f}",
                    f"{diff:.3f}",
                    overhead_str,
                    f"{sqlite_total:.3f}",
                    f"{pgsqlite_total:.3f}"
                ])
            else:
                results.append([
                    operation,
                    count,
                    f"{pgsqlite_avg:.3f}",
                    f"{pgsqlite_total:.3f}"
                ])
        
        print(tabulate(results, headers=headers, tablefmt="grid"))
        
        if not self.sqlite_only and not self.pgsqlite_only:
            # Print additional analysis
            print("\nPer-Operation Time Differences:")
            for operation in ["CREATE", "INSERT", "UPDATE", "DELETE", "SELECT", "SELECT (cached)"]:
                sqlite_times = self.sqlite_times.get(operation, [])
                pgsqlite_times = self.pgsqlite_times.get(operation, [])
                
                if sqlite_times and pgsqlite_times:
                    sqlite_avg = statistics.mean(sqlite_times) * 1000
                    pgsqlite_avg = statistics.mean(pgsqlite_times) * 1000
                    diff = pgsqlite_avg - sqlite_avg
                    print(f"{operation}: {'+' if diff > 0 else ''}{diff:.3f}ms (+{diff:.3f}ms avg difference per call)")
            
            print(f"\nOverall Statistics:")
            print(f"Total operations: {total_operations}")
            print(f"Total SQLite time: {total_sqlite_time:.3f}s")
            print(f"Total pgsqlite time: {total_pgsqlite_time:.3f}s")
            
            if total_sqlite_time > 0:
                overall_overhead = ((total_pgsqlite_time / total_sqlite_time - 1) * 100)
                print(f"Overall overhead: {'+' if overall_overhead > 0 else ''}{overall_overhead:.1f}%")
            
            # Cache effectiveness analysis
            if self.sqlite_times["SELECT"] and self.sqlite_times["SELECT (cached)"]:
                sqlite_uncached_avg = statistics.mean(self.sqlite_times["SELECT"]) * 1000
                sqlite_cached_avg = statistics.mean(self.sqlite_times["SELECT (cached)"]) * 1000
                pgsqlite_uncached_avg = statistics.mean(self.pgsqlite_times["SELECT"]) * 1000
                pgsqlite_cached_avg = statistics.mean(self.pgsqlite_times["SELECT (cached)"]) * 1000
                
                print(f"\nCache Effectiveness Analysis:")
                print(f"SQLite - Uncached SELECT: {sqlite_uncached_avg:.3f}ms, Cached: {sqlite_cached_avg:.3f}ms (Speedup: {sqlite_uncached_avg/sqlite_cached_avg:.1f}x)")
                print(f"pgsqlite - Uncached SELECT: {pgsqlite_uncached_avg:.3f}ms, Cached: {pgsqlite_cached_avg:.3f}ms (Speedup: {pgsqlite_uncached_avg/pgsqlite_cached_avg:.1f}x)")
                
                if sqlite_cached_avg > 0:
                    cache_overhead = ((pgsqlite_cached_avg / sqlite_cached_avg - 1) * 100)
                    print(f"\nCached query overhead: {'+' if cache_overhead > 0 else ''}{cache_overhead:.1f}% (pgsqlite vs SQLite)")
                    print(f"Cache improvement: {pgsqlite_uncached_avg/pgsqlite_cached_avg:.1f}x speedup for pgsqlite cached queries")
    
    def run(self):
        """Run the complete benchmark suite"""
        print(f"{Fore.YELLOW}Starting pgsqlite benchmarks...{Style.RESET_ALL}")
        print(f"Iterations: {self.iterations}")
        print(f"Batch size: {self.batch_size}")
        print(f"Database mode: {'In-memory' if self.in_memory else 'File-based'}")
        print(f"Driver: {self.driver}")
        print()
        
        self.setup()
        
        if not self.pgsqlite_only:
            self.run_sqlite_benchmarks()
        
        if not self.sqlite_only:
            self.run_pgsqlite_benchmarks()
        
        self.print_results()
        
        print(f"\n{Fore.GREEN}pgsqlite benchmarks completed.{Style.RESET_ALL}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark pgsqlite performance vs direct SQLite access")
    parser.add_argument("--iterations", type=int, default=1000, help="Number of iterations (default: 1000)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for commits (default: 100)")
    parser.add_argument("--file-based", action="store_true", help="Use file-based database instead of in-memory")
    parser.add_argument("--port", type=int, default=5432, help="pgsqlite port (default: 5432)")
    parser.add_argument("--socket-dir", type=str, help="Unix socket directory (enables socket mode)")
    parser.add_argument("--sqlite-only", action="store_true", help="Run only SQLite benchmarks")
    parser.add_argument("--pgsqlite-only", action="store_true", help="Run only pgsqlite benchmarks")
    parser.add_argument("--driver", type=str, default="psycopg2", 
                        choices=["psycopg2", "psycopg3-text", "psycopg3-binary"],
                        help="PostgreSQL driver to use (default: psycopg2)")
    
    args = parser.parse_args()
    
    runner = BenchmarkRunner(
        iterations=args.iterations,
        batch_size=args.batch_size,
        in_memory=not args.file_based,
        port=args.port,
        socket_dir=args.socket_dir,
        sqlite_only=args.sqlite_only,
        pgsqlite_only=args.pgsqlite_only,
        driver=args.driver
    )
    
    runner.run()

if __name__ == "__main__":
    main()