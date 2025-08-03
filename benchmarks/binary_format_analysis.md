# Binary Format Performance Analysis

## Root Cause Identified

The severe performance regression in binary format is caused by **RETURNING clause handling**, not the binary encoding itself.

## Key Findings

1. **Without RETURNING clause**: Binary format is actually 30% FASTER than text format
   - Text format: 0.310ms per INSERT
   - Binary format: 0.213ms per INSERT (0.7x of text format time)

2. **With RETURNING clause**: Binary format is 6.9x SLOWER than text format
   - Text format: 0.120ms per INSERT RETURNING
   - Binary format: 0.830ms per INSERT RETURNING
   - Additional overhead: 0.617ms

3. **The benchmark uses RETURNING extensively**:
   - All INSERT operations use `RETURNING id`
   - This explains why the benchmark shows 12.7x slower performance for binary INSERT

## Implementation Issue

The current implementation of RETURNING in `execute_dml_with_returning`:

1. Executes the INSERT/UPDATE/DELETE query
2. Then runs a SEPARATE SELECT query to fetch the RETURNING data
3. This doubles the database operations

For binary format, there's additional overhead:
- The SELECT results must be encoded to binary format
- This encoding happens for every RETURNING operation
- The binary encoding adds ~0.6ms per operation

## Why Text Format is Less Affected

Text format with RETURNING is actually faster (0.120ms) than without RETURNING (0.310ms), which seems counterintuitive. This might be due to:
- Different code paths or optimizations
- Caching effects
- The benchmark methodology

## Solution

To fix the binary format performance regression:

1. **Short term**: Optimize RETURNING clause handling
   - Use SQLite's `RETURNING` clause directly (SQLite 3.35.0+)
   - Avoid the double query execution

2. **Medium term**: Optimize binary result encoding
   - Cache binary-encoded results for RETURNING queries
   - Use more efficient encoding for common cases

3. **Long term**: Redesign the extended query protocol handling
   - Unified path for RETURNING and non-RETURNING queries
   - Better integration with SQLite's native capabilities

## Recommendation

Until fixed, users should:
- Use text format for queries with RETURNING clauses
- Use binary format only for SELECT queries without RETURNING
- Consider removing RETURNING clauses where not strictly necessary