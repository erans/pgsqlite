#!/usr/bin/env python3
"""Compare text vs binary benchmark results"""

# Text format results (from first run)
text_results = {
    "CREATE": {"count": 1, "sqlite_avg": 0.142, "pgsqlite_avg": 8.798, "overhead": 6080.6},
    "INSERT": {"count": 265, "sqlite_avg": 0.002, "pgsqlite_avg": 0.055, "overhead": 3231.1},
    "UPDATE": {"count": 233, "sqlite_avg": 0.001, "pgsqlite_avg": 0.061, "overhead": 5309.5},
    "DELETE": {"count": 248, "sqlite_avg": 0.001, "pgsqlite_avg": 0.031, "overhead": 3238.3},
    "SELECT": {"count": 254, "sqlite_avg": 0.001, "pgsqlite_avg": 0.616, "overhead": 61014.5},
    "SELECT (cached)": {"count": 100, "sqlite_avg": 0.003, "pgsqlite_avg": 0.065, "overhead": 1863.7},
}

# Binary format results (from second run)
binary_results = {
    "CREATE": {"count": 1, "sqlite_avg": 0.001, "pgsqlite_avg": 11.151, "overhead": 1115000.0},
    "INSERT": {"count": 248, "sqlite_avg": 0.002, "pgsqlite_avg": 0.045, "overhead": 2563.5},
    "UPDATE": {"count": 245, "sqlite_avg": 0.001, "pgsqlite_avg": 0.054, "overhead": 4658.4},
    "DELETE": {"count": 272, "sqlite_avg": 0.001, "pgsqlite_avg": 0.027, "overhead": 2629.7},
    "SELECT": {"count": 235, "sqlite_avg": 0.001, "pgsqlite_avg": 0.662, "overhead": 65549.5},
    "SELECT (cached)": {"count": 100, "sqlite_avg": 0.001, "pgsqlite_avg": 0.094, "overhead": 8350.8},
}

print("=== pgsqlite Text vs Binary Format Comparison ===\n")

print("Average Response Times (ms):")
print("+" + "-"*70 + "+")
print("| Operation       | Text Format | Binary Format | Difference | Change % |")
print("+" + "="*70 + "+")

for op in ["CREATE", "INSERT", "UPDATE", "DELETE", "SELECT", "SELECT (cached)"]:
    text_avg = text_results[op]["pgsqlite_avg"]
    binary_avg = binary_results[op]["pgsqlite_avg"]
    diff = binary_avg - text_avg
    pct_change = ((binary_avg - text_avg) / text_avg) * 100 if text_avg > 0 else 0
    
    print(f"| {op:15} | {text_avg:11.3f} | {binary_avg:13.3f} | {diff:10.3f} | {pct_change:+7.1f}% |")

print("+" + "-"*70 + "+")

print("\nOverhead Comparison (vs SQLite):")
print("+" + "-"*50 + "+")
print("| Operation       | Text Overhead | Binary Overhead |")
print("+" + "="*50 + "+")

for op in ["INSERT", "UPDATE", "DELETE", "SELECT", "SELECT (cached)"]:
    text_oh = text_results[op]["overhead"]
    binary_oh = binary_results[op]["overhead"]
    print(f"| {op:15} | {text_oh:11.1f}% | {binary_oh:14.1f}% |")

print("+" + "-"*50 + "+")

print("\nKey Findings:")
print("- INSERT: Binary is 18.2% faster (0.055ms → 0.045ms)")
print("- UPDATE: Binary is 11.5% faster (0.061ms → 0.054ms)")
print("- DELETE: Binary is 12.9% faster (0.031ms → 0.027ms)")
print("- SELECT: Binary is 7.5% slower (0.616ms → 0.662ms)")
print("- SELECT (cached): Binary is 44.6% slower (0.065ms → 0.094ms)")

print("\nSummary:")
print("- Binary format improves DML operations (INSERT/UPDATE/DELETE) by 11-18%")
print("- Binary format has worse performance for SELECT operations")
print("- The overhead is likely due to binary encoding/decoding for result sets")