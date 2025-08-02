# Binary Format Performance Analysis

## Current Status (2025-08-02)
- SELECT: 10.5% FASTER with binary format ✅
- SELECT (cached): 2.3x slower (needs binary result caching)
- INSERT: 8.7x SLOWER ❌ (regression under investigation)
- UPDATE: 1.9x SLOWER ❌ (regression under investigation)  
- DELETE: 3.4x SLOWER ❌ (regression under investigation)

Phase 1 optimization successfully improved SELECT performance, but DML operations have significant regressions.

## Root Cause Analysis

### 1. Excessive Memory Allocations
```rust
// Current approach - allocates for EVERY value
pub fn encode_int4(value: i32) -> Vec<u8> {
    value.to_be_bytes().to_vec()  // New heap allocation!
}
```

For a query returning 1000 rows × 5 columns = 5000 allocations!

### 2. Unused Zero-Copy Infrastructure
The codebase already has `ZeroCopyBinaryEncoder` using `BytesMut` but it's not being used:
```rust
pub struct ZeroCopyBinaryEncoder<'a> {
    buffer: &'a mut BytesMut,
}
```

### 3. Row-by-Row Encoding
Current flow:
1. Get row from SQLite
2. Encode each value separately
3. Create DataRow message
4. Send to client
5. Repeat for next row

This has poor cache locality and repeated overhead.

## Optimization Implementation Plan

### Phase 1: Use Zero-Copy Encoder (Quick Win) - COMPLETED ✅
**Actual improvement: 10.5% for all SELECTs**

1. ✅ Replaced current encoding with `BinaryResultEncoder` using `BytesMut`
2. ✅ Pre-allocated single buffer for entire result set
3. ✅ Encode directly into buffer without intermediate allocations
4. ✅ Fixed type detection for REAL columns (FLOAT8 instead of NUMERIC)
5. ✅ Fixed binary format detection for single-format result requests

### Phase 2: Binary Result Caching
**Expected improvement: 50-70% for cached SELECTs**

1. Cache encoded binary results alongside query results
2. For cached queries, skip encoding entirely
3. Send pre-encoded binary data directly

### Phase 3: Columnar Batch Encoding
**Expected improvement: 10-15% additional for large result sets**

1. Collect values by column type
2. Use SIMD operations for integer/float encoding
3. Batch encode entire columns at once

### Phase 4: Direct SQLite Binary Access
**Expected improvement: 5-10% by avoiding text parsing**

1. Investigate SQLite's internal binary representation
2. For numeric types, get raw bytes directly
3. Avoid text→parse→binary conversion

## Implementation Priority

1. **High Priority**: Phase 1 (Zero-Copy) - Easy to implement, big impact
2. **High Priority**: Phase 2 (Caching) - Fixes the 44.6% cached query regression
3. **Medium Priority**: Phase 3 (Columnar) - Benefits large result sets
4. **Low Priority**: Phase 4 (Direct Access) - Complex, smaller benefit

## Benchmarking Plan

Create specific benchmarks to measure:
1. Small result sets (1-10 rows)
2. Medium result sets (100-1000 rows)
3. Large result sets (10,000+ rows)
4. Cached vs uncached performance
5. Different column types (integers, floats, text, mixed)

## Expected Results

With all optimizations:
- Non-cached SELECT: From 7.5% slower to 5-10% faster than text ✅ ACHIEVED
- Cached SELECT: From 44.6% slower to 20-30% faster than text (pending Phase 2)
- DML operations: Need investigation to fix regressions and achieve parity with text format

## DML Performance Regression Analysis

The DML operations (INSERT/UPDATE/DELETE) are significantly slower with binary format. Potential causes:

1. **Binary Parameter Decoding Overhead**: Converting binary parameters to text for SQLite
2. **No Fast Path for DML**: DML operations don't use the optimized fast path
3. **Double Encoding**: RETURNING clauses may cause values to be encoded twice
4. **Extended Protocol Overhead**: Additional round trips for Parse/Bind/Execute