#!/usr/bin/env python3
"""Simple test to verify array benchmark functionality without full server setup."""

import tempfile
import os
import json
import time
import statistics
from benchmark_array_binary import ArrayBenchmarkRunner

def test_sqlite_only():
    """Test just the SQLite portion of the benchmark."""
    print("🧪 Testing SQLite array benchmark functionality...")

    runner = ArrayBenchmarkRunner(iterations=3, port=15505)

    # Create temporary database
    temp_db = tempfile.mktemp(suffix=".db")
    runner.sqlite_file = temp_db

    try:
        # Test SQLite benchmark
        sqlite_times = runner.benchmark_sqlite_arrays(array_size=5)

        print("✅ SQLite benchmark results:")
        for operation, time_ms in sqlite_times.items():
            print(f"  {operation}: {time_ms*1000:.3f}ms")

        print(f"\n✅ SQLite benchmark test passed!")
        return True

    except Exception as e:
        print(f"❌ SQLite benchmark test failed: {e}")
        return False

    finally:
        # Cleanup
        if os.path.exists(temp_db):
            os.remove(temp_db)

def test_array_generation():
    """Test array data generation."""
    print("\n🧪 Testing array data generation...")

    runner = ArrayBenchmarkRunner(iterations=3, port=15505)

    try:
        # Test different array sizes
        for size in [5, 10, 20]:
            arrays = runner.generate_test_arrays(size)

            # Verify all array types are present
            expected_keys = {"int_array", "bigint_array", "text_array", "float_array", "bool_array"}
            if not expected_keys.issubset(arrays.keys()):
                raise ValueError(f"Missing array types: {expected_keys - arrays.keys()}")

            # Verify array sizes
            for key, array in arrays.items():
                if len(array) != size:
                    raise ValueError(f"{key} has wrong size: {len(array)} != {size}")

            # Verify JSON serialization works
            for key, array in arrays.items():
                json_str = json.dumps(array)
                restored = json.loads(json_str)
                if restored != array:
                    raise ValueError(f"{key} JSON serialization failed")

        print("✅ Array generation test passed!")
        return True

    except Exception as e:
        print(f"❌ Array generation test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Array Binary Protocol Benchmark - Component Tests")
    print("=" * 60)

    all_passed = True

    # Test array generation
    all_passed &= test_array_generation()

    # Test SQLite benchmarking
    all_passed &= test_sqlite_only()

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 All component tests passed!")
        print("\nTo run the full benchmark with pgsqlite server:")
        print("  ./run_array_benchmark.sh --iterations 10")
    else:
        print("❌ Some tests failed!")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())