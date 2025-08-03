# Unix Socket Performance: Text vs Binary Format Comparison

## Executive Summary

Binary format shows **significantly worse performance** than text format for all operations when using Unix sockets, contrary to expectations. The regression is particularly severe for DML operations.

## Performance Comparison

### Overall Overhead (vs SQLite)
- **Text Format**: +2,516.6% overhead
- **Binary Format**: +7,197.4% overhead
- **Binary is 2.9x SLOWER than text format overall**

### Per-Operation Analysis

| Operation | Text Format | Binary Format | Binary vs Text |
|-----------|-------------|---------------|----------------|
| CREATE | +90.7% | +86.6% | ✅ 4% better |
| INSERT | +1,643.6% | +20,858.5% | ❌ 12.7x worse |
| UPDATE | +4,285.7% | +12,798.4% | ❌ 3.0x worse |
| DELETE | +1,633.3% | +10,190.0% | ❌ 6.2x worse |
| SELECT | +46,404.4% | +82,911.2% | ❌ 1.8x worse |
| SELECT (cached) | +777.3% | +8,356.4% | ❌ 10.8x worse |

### Absolute Time Differences

| Operation | Text (ms) | Binary (ms) | Difference |
|-----------|-----------|-------------|------------|
| INSERT | 0.070 | 0.870 | +0.800ms per op |
| UPDATE | 0.075 | 0.218 | +0.143ms per op |
| DELETE | 0.040 | 0.200 | +0.160ms per op |
| SELECT | 0.699 | 1.189 | +0.490ms per op |
| SELECT (cached) | 0.085 | 0.778 | +0.693ms per op |

## Key Findings

1. **Binary format regression confirmed**: Binary format is significantly slower than text format for all operations except CREATE TABLE.

2. **DML operations severely impacted**: INSERT operations are 12.7x slower with binary format, suggesting a critical issue in the binary encoding path.

3. **Cached queries also affected**: Even cached SELECT queries are 10.8x slower with binary format, indicating overhead in the binary result encoding.

4. **The fix for the hanging issue works**: Both benchmarks completed successfully, confirming the server no longer hangs after binary operations.

## Root Cause Analysis

The performance regression appears to be in the binary format implementation:
- Fast path optimization may not be working correctly for binary format
- Binary encoding/decoding adds significant overhead
- The issue affects both request processing (DML) and response encoding (SELECT)

## Recommendations

1. **Use text format for production**: Until the binary format regression is fixed, text format provides significantly better performance.

2. **Investigate binary encoding path**: The 12.7x regression for INSERT operations suggests a critical issue in the binary parameter handling.

3. **Profile binary format code**: Focus on:
   - Parameter decoding in extended.rs
   - Binary result encoding in fast path
   - Connection/session handling differences between formats

4. **Fix the cleanup_connection issue properly**: The current workaround (commenting out the cleanup) should be replaced with a proper fix.