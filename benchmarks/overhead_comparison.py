#!/usr/bin/env python3
"""
Comprehensive overhead comparison between pure SQLite, pgsqlite text mode, and pgsqlite binary mode.
This benchmark measures the exact overhead introduced by the PostgreSQL protocol layer.
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
import subprocess
import signal
import threading

# Initialize colorama
init()

@dataclass
class OverheadResult:
    operation: str
    sqlite_time: float
    pgsqlite_text_time: float
    pgsqlite_binary_time: float
    count: int

class OverheadBenchmark:
    def __init__(self, iterations: int = 500, batch_size: int = 50, port: int = 44000):
        self.iterations = iterations
        self.batch_size = batch_size
        self.port = port
        self.sqlite_file = "overhead_benchmark.db"
        self.pgsqlite_process = None

        # Import drivers
        try:
            import psycopg
            self.psycopg = psycopg
            self.has_psycopg3 = True
        except ImportError:
            print(f"{Fore.RED}Error: psycopg3 not available. Install with: pip install psycopg[binary]{Style.RESET_ALL}")
            sys.exit(1)

        # Timing storage
        self.sqlite_times: Dict[str, List[float]] = {
            "CREATE": [], "INSERT": [], "UPDATE": [], "DELETE": [], "SELECT": [], "SELECT (cached)": []
        }
        self.pgsqlite_text_times: Dict[str, List[float]] = {
            "CREATE": [], "INSERT": [], "UPDATE": [], "DELETE": [], "SELECT": [], "SELECT (cached)": []
        }
        self.pgsqlite_binary_times: Dict[str, List[float]] = {
            "CREATE": [], "INSERT": [], "UPDATE": [], "DELETE": [], "SELECT": [], "SELECT (cached)": []
        }

    def setup(self):
        """Setup benchmark environment"""
        # Clean up any existing files
        if os.path.exists(self.sqlite_file):
            os.remove(self.sqlite_file)

        # Kill any existing pgsqlite processes
        os.system(f"pkill -f 'pgsqlite.*{self.port}' 2>/dev/null")
        time.sleep(1)

    def start_pgsqlite_server(self):
        """Start pgsqlite server"""
        cmd = [
            "./target/release/pgsqlite",
            "--database", self.sqlite_file,
            "--port", str(self.port)
        ]

        print(f"{Fore.CYAN}Starting pgsqlite server on port {self.port}...{Style.RESET_ALL}")

        # Start server in background
        self.pgsqlite_process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd="/home/eran/work/pgsqlite"
        )

        # Wait for server to start
        time.sleep(3)

        # Verify server is running
        if self.pgsqlite_process.poll() is not None:
            print(f"{Fore.RED}Error: pgsqlite server failed to start{Style.RESET_ALL}")
            sys.exit(1)

        print(f"{Fore.GREEN}pgsqlite server started successfully{Style.RESET_ALL}")

    def stop_pgsqlite_server(self):
        """Stop pgsqlite server"""
        if self.pgsqlite_process:
            self.pgsqlite_process.terminate()
            self.pgsqlite_process.wait()
            print(f"{Fore.CYAN}pgsqlite server stopped{Style.RESET_ALL}")

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
        print(f"{Fore.CYAN}🗂️  Running pure SQLite benchmarks...{Style.RESET_ALL}")

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
        print(f"{Fore.CYAN}🗂️  Running SQLite cached query benchmarks...{Style.RESET_ALL}")

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

    def run_pgsqlite_benchmarks(self, driver_mode: str, times_dict: Dict[str, List[float]]):
        """Run benchmarks using pgsqlite with specified driver mode"""
        mode_name = "text" if driver_mode == "text" else "binary"
        print(f"{Fore.CYAN}🚀 Running pgsqlite benchmarks in {mode_name} mode...{Style.RESET_ALL}")

        # Connect using psycopg3
        conninfo = f"host=localhost port={self.port} dbname={self.sqlite_file} user=dummy password=dummy sslmode=disable"

        if driver_mode == "binary":
            # psycopg3 binary mode
            conn = self.psycopg.connect(conninfo)
        else:
            # psycopg3 text mode
            conn = self.psycopg.connect(conninfo)
            # Force text mode by disabling prepared statements
            if hasattr(conn, 'prepare_threshold'):
                conn.prepare_threshold = None

        cursor = conn.cursor()

        # CREATE TABLE (only if not exists)
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
        times_dict["CREATE"].append(elapsed)
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
                times_dict["INSERT"].append(elapsed)
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
                times_dict["UPDATE"].append(elapsed)

            elif operation == "DELETE" and data_ids:
                # DELETE
                id_to_delete = random.choice(data_ids)
                elapsed, _ = self.measure_time(
                    cursor.execute,
                    "DELETE FROM benchmark_table_pg WHERE id = %s",
                    (id_to_delete,)
                )
                times_dict["DELETE"].append(elapsed)
                data_ids.remove(id_to_delete)

            elif operation == "SELECT" and data_ids:
                # SELECT
                elapsed, _ = self.measure_time(
                    cursor.execute,
                    "SELECT * FROM benchmark_table_pg WHERE int_col > %s",
                    (random.randint(1, 5000),)
                )
                cursor.fetchall()  # Ensure we fetch results
                times_dict["SELECT"].append(elapsed)

            # Commit periodically
            if i % self.batch_size == 0:
                conn.commit()

        conn.commit()

        # Run cached query benchmarks
        print(f"{Fore.CYAN}🚀 Running pgsqlite cached query benchmarks in {mode_name} mode...{Style.RESET_ALL}")

        # Define a set of queries to repeat (PostgreSQL syntax)
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
            times_dict["SELECT (cached)"].append(elapsed)

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

    def print_overhead_results(self):
        """Print comprehensive overhead analysis"""
        print(f"\n{Fore.GREEN}{'='*100}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}PGSQLITE OVERHEAD ANALYSIS RESULTS{Style.RESET_ALL}")
        print(f"{Fore.GREEN}{'='*100}{Style.RESET_ALL}")

        # Detailed comparison table
        comparison_data = []

        for operation in ["CREATE", "INSERT", "UPDATE", "DELETE", "SELECT", "SELECT (cached)"]:
            sqlite_stats = self.calculate_stats(self.sqlite_times[operation])
            text_stats = self.calculate_stats(self.pgsqlite_text_times[operation])
            binary_stats = self.calculate_stats(self.pgsqlite_binary_times[operation])

            if sqlite_stats["avg"] > 0:
                text_overhead = ((text_stats["avg"] - sqlite_stats["avg"]) / sqlite_stats["avg"]) * 100
                binary_overhead = ((binary_stats["avg"] - sqlite_stats["avg"]) / sqlite_stats["avg"]) * 100
                text_multiplier = text_stats["avg"] / sqlite_stats["avg"] if sqlite_stats["avg"] > 0 else 0
                binary_multiplier = binary_stats["avg"] / sqlite_stats["avg"] if sqlite_stats["avg"] > 0 else 0
            else:
                text_overhead = 0
                binary_overhead = 0
                text_multiplier = 0
                binary_multiplier = 0

            comparison_data.append([
                operation,
                len(self.sqlite_times[operation]),
                f"{sqlite_stats['avg']*1000:.3f}",
                f"{text_stats['avg']*1000:.3f}",
                f"{binary_stats['avg']*1000:.3f}",
                f"{text_multiplier:.1f}x",
                f"{binary_multiplier:.1f}x",
                f"{text_overhead:+.1f}%",
                f"{binary_overhead:+.1f}%"
            ])

        headers = [
            "Operation", "Count",
            "SQLite (ms)", "pgsqlite Text (ms)", "pgsqlite Binary (ms)",
            "Text Overhead", "Binary Overhead",
            "Text %", "Binary %"
        ]

        print(tabulate(comparison_data, headers=headers, tablefmt="grid"))

        # Summary statistics
        print(f"\n{Fore.CYAN}📊 OVERHEAD SUMMARY:{Style.RESET_ALL}")

        total_sqlite = sum(sum(times) for times in self.sqlite_times.values())
        total_text = sum(sum(times) for times in self.pgsqlite_text_times.values())
        total_binary = sum(sum(times) for times in self.pgsqlite_binary_times.values())

        if total_sqlite > 0:
            overall_text_overhead = ((total_text - total_sqlite) / total_sqlite) * 100
            overall_binary_overhead = ((total_binary - total_sqlite) / total_sqlite) * 100

            print(f"📈 Overall Text Mode Overhead: {overall_text_overhead:+.1f}% ({total_text/total_sqlite:.1f}x)")
            print(f"📈 Overall Binary Mode Overhead: {overall_binary_overhead:+.1f}% ({total_binary/total_sqlite:.1f}x)")

            # Performance comparison between modes
            if total_text > 0:
                binary_vs_text = ((total_binary - total_text) / total_text) * 100
                print(f"⚡ Binary vs Text Performance: {binary_vs_text:+.1f}% ({'faster' if binary_vs_text < 0 else 'slower'})")

        print(f"\n{Fore.CYAN}🎯 OPERATIONAL BREAKDOWN:{Style.RESET_ALL}")

        for operation in ["INSERT", "UPDATE", "DELETE", "SELECT", "SELECT (cached)"]:
            sqlite_avg = self.calculate_stats(self.sqlite_times[operation])["avg"]
            text_avg = self.calculate_stats(self.pgsqlite_text_times[operation])["avg"]
            binary_avg = self.calculate_stats(self.pgsqlite_binary_times[operation])["avg"]

            if sqlite_avg > 0:
                text_mult = text_avg / sqlite_avg
                binary_mult = binary_avg / sqlite_avg

                text_color = Fore.GREEN if text_mult < 2 else Fore.YELLOW if text_mult < 5 else Fore.RED
                binary_color = Fore.GREEN if binary_mult < 2 else Fore.YELLOW if binary_mult < 5 else Fore.RED

                print(f"{operation:15}: Text {text_color}{text_mult:.1f}x{Style.RESET_ALL}, Binary {binary_color}{binary_mult:.1f}x{Style.RESET_ALL}")

    def run(self):
        """Run the complete overhead benchmark suite"""
        print(f"{Fore.YELLOW}🚀 Starting pgsqlite Overhead Analysis Benchmark{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Iterations: {self.iterations}, Batch size: {self.batch_size}{Style.RESET_ALL}")

        try:
            self.setup()

            # Build pgsqlite first
            print(f"{Fore.CYAN}Building pgsqlite in release mode...{Style.RESET_ALL}")
            build_result = subprocess.run(
                ["cargo", "build", "--release", "--bin", "pgsqlite"],
                cwd="/home/eran/work/pgsqlite",
                capture_output=True,
                text=True
            )

            if build_result.returncode != 0:
                print(f"{Fore.RED}Failed to build pgsqlite{Style.RESET_ALL}")
                return

            # Run pure SQLite benchmarks
            self.run_sqlite_benchmarks()

            # Start pgsqlite server
            self.start_pgsqlite_server()

            # Run pgsqlite text mode benchmarks
            self.run_pgsqlite_benchmarks("text", self.pgsqlite_text_times)

            # Run pgsqlite binary mode benchmarks
            self.run_pgsqlite_benchmarks("binary", self.pgsqlite_binary_times)

            # Print results
            self.print_overhead_results()

        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Benchmark interrupted by user{Style.RESET_ALL}")
        except Exception as e:
            print(f"{Fore.RED}Error during benchmark: {e}{Style.RESET_ALL}")
            raise
        finally:
            # Clean up
            self.stop_pgsqlite_server()
            if os.path.exists(self.sqlite_file):
                os.remove(self.sqlite_file)

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Analyze pgsqlite overhead vs pure SQLite")
    parser.add_argument("-i", "--iterations", type=int, default=500,
                        help="Number of operations to perform (default: 500)")
    parser.add_argument("-b", "--batch-size", type=int, default=50,
                        help="Batch size for commits (default: 50)")
    parser.add_argument("--port", type=int, default=44000,
                        help="PostgreSQL port to use (default: 44000)")

    args = parser.parse_args()

    benchmark = OverheadBenchmark(
        iterations=args.iterations,
        batch_size=args.batch_size,
        port=args.port
    )
    benchmark.run()

if __name__ == "__main__":
    main()