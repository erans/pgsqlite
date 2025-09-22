#!/usr/bin/env python3
"""
Benchmark array operations with psycopg3 binary protocol vs SQLite direct access.
Tests the performance impact of array binary encoding/decoding.
"""

import sqlite3
import time
import random
import json
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
class ArrayBenchmarkResult:
    operation: str
    data_type: str
    array_size: int
    sqlite_time: float
    pgsqlite_time: float
    overhead_factor: float

class ArrayBenchmarkRunner:
    def __init__(self, iterations: int = 100, port: int = 15500):
        self.iterations = iterations
        self.port = port
        self.sqlite_file = "array_benchmark_test.db"

        # Import psycopg3
        try:
            import psycopg
            self.psycopg = psycopg
        except ImportError:
            print("❌ psycopg3 not available. Install with: pip install psycopg[binary]")
            sys.exit(1)

        self.results: List[ArrayBenchmarkResult] = []

    def setup(self):
        """Remove existing database file if it exists"""
        if os.path.exists(self.sqlite_file):
            os.remove(self.sqlite_file)

    def cleanup(self):
        """Clean up test database"""
        if os.path.exists(self.sqlite_file):
            os.remove(self.sqlite_file)

    def generate_test_arrays(self, size: int) -> Dict[str, Any]:
        """Generate test arrays of different types and sizes"""
        return {
            "int_array": [random.randint(1, 1000) for _ in range(size)],
            "bigint_array": [random.randint(1000000000000, 9999999999999) for _ in range(size)],
            "text_array": [f"text_{i}_{random.randint(1000, 9999)}" for i in range(size)],
            "float_array": [round(random.uniform(0.0, 1000.0), 3) for _ in range(size)],
            "bool_array": [random.choice([True, False]) for _ in range(size)]
        }

    def measure_time(self, func, *args, **kwargs) -> float:
        """Measure execution time of a function"""
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        return end - start, result

    def benchmark_sqlite_arrays(self, array_size: int) -> Dict[str, float]:
        """Benchmark array operations using direct SQLite access"""
        conn = sqlite3.connect(self.sqlite_file)
        cursor = conn.cursor()

        times = {}

        # Drop and create table for clean state
        try:
            cursor.execute("DROP TABLE IF EXISTS array_bench_sqlite")
        except:
            pass

        create_time, _ = self.measure_time(cursor.execute, """
            CREATE TABLE array_bench_sqlite (
                id INTEGER PRIMARY KEY,
                int_array TEXT,
                bigint_array TEXT,
                text_array TEXT,
                float_array TEXT,
                bool_array TEXT
            )
        """)
        times['CREATE'] = create_time

        # Generate test data
        test_data = self.generate_test_arrays(array_size)

        # Benchmark INSERT
        insert_times = []
        for i in range(self.iterations):
            data = self.generate_test_arrays(array_size)
            insert_time, _ = self.measure_time(cursor.execute,
                """INSERT INTO array_bench_sqlite
                   (id, int_array, bigint_array, text_array, float_array, bool_array)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (i, json.dumps(data["int_array"]), json.dumps(data["bigint_array"]),
                 json.dumps(data["text_array"]), json.dumps(data["float_array"]),
                 json.dumps(data["bool_array"]))
            )
            insert_times.append(insert_time)

        times['INSERT'] = statistics.mean(insert_times)
        conn.commit()

        # Benchmark SELECT
        select_times = []
        for i in range(min(50, self.iterations)):  # Fewer iterations for SELECT
            select_time, _ = self.measure_time(cursor.execute,
                "SELECT int_array, text_array, float_array FROM array_bench_sqlite WHERE id = ?", (i,))
            result = cursor.fetchone()
            select_times.append(select_time)

        times['SELECT'] = statistics.mean(select_times)

        # Benchmark UPDATE
        update_times = []
        for i in range(min(50, self.iterations)):
            new_data = self.generate_test_arrays(array_size)
            update_time, _ = self.measure_time(cursor.execute,
                "UPDATE array_bench_sqlite SET int_array = ? WHERE id = ?",
                (json.dumps(new_data["int_array"]), i))
            update_times.append(update_time)

        times['UPDATE'] = statistics.mean(update_times)
        conn.commit()

        conn.close()
        return times

    def benchmark_pgsqlite_arrays(self, array_size: int) -> Dict[str, float]:
        """Benchmark array operations using pgsqlite with psycopg3 binary"""
        conn = self.psycopg.connect(f"host=localhost port={self.port} user=postgres dbname={self.sqlite_file}")

        times = {}

        try:
            with conn.cursor() as cur:
                # Drop and create table for clean state
                try:
                    cur.execute("DROP TABLE IF EXISTS array_bench_pgsqlite")
                    conn.commit()
                except:
                    pass

                # Create table
                create_time, _ = self.measure_time(cur.execute, """
                    CREATE TABLE array_bench_pgsqlite (
                        id INTEGER PRIMARY KEY,
                        int_array INTEGER[],
                        bigint_array BIGINT[],
                        text_array TEXT[],
                        float_array DOUBLE PRECISION[],
                        bool_array BOOLEAN[]
                    )
                """)
                times['CREATE'] = create_time
                conn.commit()

                # Benchmark INSERT with binary protocol
                insert_times = []
                for i in range(self.iterations):
                    data = self.generate_test_arrays(array_size)
                    insert_time, _ = self.measure_time(cur.execute,
                        """INSERT INTO array_bench_pgsqlite
                           (id, int_array, bigint_array, text_array, float_array, bool_array)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (i, json.dumps(data["int_array"]), json.dumps(data["bigint_array"]),
                         json.dumps(data["text_array"]), json.dumps(data["float_array"]),
                         json.dumps(data["bool_array"])),
                        binary=True  # Enable binary protocol
                    )
                    insert_times.append(insert_time)

                times['INSERT'] = statistics.mean(insert_times)
                conn.commit()

                # Benchmark SELECT with binary results
                select_times = []
                for i in range(min(50, self.iterations)):
                    select_time, _ = self.measure_time(cur.execute,
                        "SELECT int_array, text_array, float_array FROM array_bench_pgsqlite WHERE id = %s",
                        [i], binary=True)
                    result = cur.fetchone()
                    select_times.append(select_time)

                times['SELECT'] = statistics.mean(select_times)

                # Benchmark UPDATE with binary protocol
                update_times = []
                for i in range(min(50, self.iterations)):
                    new_data = self.generate_test_arrays(array_size)
                    update_time, _ = self.measure_time(cur.execute,
                        "UPDATE array_bench_pgsqlite SET int_array = %s WHERE id = %s",
                        (json.dumps(new_data["int_array"]), i),
                        binary=True)
                    update_times.append(update_time)

                times['UPDATE'] = statistics.mean(update_times)
                conn.commit()

        finally:
            conn.close()

        return times

    def run_array_size_benchmark(self, array_size: int):
        """Run benchmark for a specific array size"""
        print(f"{Fore.CYAN}Benchmarking arrays of size {array_size}...{Style.RESET_ALL}")

        # Setup fresh database
        self.setup()

        # Run SQLite benchmarks
        print(f"  Running SQLite benchmarks...")
        sqlite_times = self.benchmark_sqlite_arrays(array_size)

        # Run pgsqlite benchmarks
        print(f"  Running pgsqlite benchmarks...")
        pgsqlite_times = self.benchmark_pgsqlite_arrays(array_size)

        # Calculate results
        for operation in ["CREATE", "INSERT", "SELECT", "UPDATE"]:
            sqlite_time = sqlite_times.get(operation, 0)
            pgsqlite_time = pgsqlite_times.get(operation, 0)
            overhead = pgsqlite_time / sqlite_time if sqlite_time > 0 else 0

            result = ArrayBenchmarkResult(
                operation=operation,
                data_type="mixed_arrays",
                array_size=array_size,
                sqlite_time=sqlite_time * 1000,  # Convert to ms
                pgsqlite_time=pgsqlite_time * 1000,  # Convert to ms
                overhead_factor=overhead
            )
            self.results.append(result)

        self.cleanup()

    def run_benchmarks(self):
        """Run benchmarks for different array sizes"""
        print(f"{Fore.GREEN}🚀 Array Binary Protocol Benchmark{Style.RESET_ALL}")
        print(f"Driver: psycopg3-binary")
        print(f"Iterations: {self.iterations}")
        print(f"Port: {self.port}")
        print()

        # Test different array sizes
        array_sizes = [5, 10, 50, 100, 500]

        for i, size in enumerate(array_sizes):
            # Use unique database for each array size
            original_db = self.sqlite_file
            self.sqlite_file = f"array_benchmark_test_{i}_{size}.db"
            self.run_array_size_benchmark(size)
            self.sqlite_file = original_db

        self.print_results()

    def print_results(self):
        """Print benchmark results in a formatted table"""
        print(f"\n{Fore.GREEN}📊 Array Binary Protocol Performance Results{Style.RESET_ALL}")
        print("=" * 80)

        # Group results by array size
        size_groups = {}
        for result in self.results:
            if result.array_size not in size_groups:
                size_groups[result.array_size] = []
            size_groups[result.array_size].append(result)

        for array_size in sorted(size_groups.keys()):
            print(f"\n{Fore.YELLOW}Array Size: {array_size} elements{Style.RESET_ALL}")

            table_data = []
            for result in size_groups[array_size]:
                overhead_color = ""
                if result.overhead_factor < 100:
                    overhead_color = Fore.GREEN
                elif result.overhead_factor < 500:
                    overhead_color = Fore.YELLOW
                else:
                    overhead_color = Fore.RED

                table_data.append([
                    result.operation,
                    f"{result.sqlite_time:.3f}",
                    f"{result.pgsqlite_time:.3f}",
                    f"{overhead_color}{result.overhead_factor:.1f}x{Style.RESET_ALL}"
                ])

            headers = ["Operation", "SQLite (ms)", "pgsqlite (ms)", "Overhead"]
            print(tabulate(table_data, headers=headers, tablefmt="grid"))

        # Summary
        print(f"\n{Fore.CYAN}📈 Summary{Style.RESET_ALL}")
        avg_overhead_by_op = {}
        for result in self.results:
            if result.operation not in avg_overhead_by_op:
                avg_overhead_by_op[result.operation] = []
            avg_overhead_by_op[result.operation].append(result.overhead_factor)

        summary_data = []
        for operation, overheads in avg_overhead_by_op.items():
            avg_overhead = statistics.mean(overheads)
            color = Fore.GREEN if avg_overhead < 100 else Fore.YELLOW if avg_overhead < 500 else Fore.RED
            summary_data.append([
                operation,
                f"{color}{avg_overhead:.1f}x{Style.RESET_ALL}"
            ])

        print(tabulate(summary_data, headers=["Operation", "Avg Overhead"], tablefmt="grid"))

        print(f"\n{Fore.GREEN}✅ Array binary protocol benchmarking complete!{Style.RESET_ALL}")

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Benchmark array operations with binary protocol")
    parser.add_argument("--iterations", "-i", type=int, default=100,
                       help="Number of iterations for each test")
    parser.add_argument("--port", "-p", type=int, default=15500,
                       help="pgsqlite server port")

    args = parser.parse_args()

    runner = ArrayBenchmarkRunner(iterations=args.iterations, port=args.port)
    runner.run_benchmarks()

if __name__ == "__main__":
    main()