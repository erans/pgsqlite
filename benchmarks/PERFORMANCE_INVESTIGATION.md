# Performance Investigation: RETURNING Optimization Regression

## Issue
After implementing RETURNING clause support in the fast path, INSERT performance regressed by 3.3x (from 0.177ms to 0.587ms).

## Key Findings

### 1. Fast Path IS Being Used
- Logging confirms INSERT queries with RETURNING are using the fast path
- Overhead of `is_simple_returning_clause()` is minimal (~400-600ns)
- Total fast path check takes ~9-11µs

### 2. Logging Overhead Eliminated
- Removed info! logging didn't significantly improve performance
- INSERT still at 0.587ms after removing all logging

### 3. Identified Performance Issues

#### Issue 1: Duplicate RETURNING Searches
```rust
// Current inefficient code
if memchr::memmem::find(query_bytes, b"RETURNING").is_some() ||
   memchr::memmem::find(query_bytes, b"returning").is_some() {
```
This searches the entire query twice for case variations.

#### Issue 2: String Allocation in Hot Path
```rust
let query_upper = query.to_uppercase();  // Allocates new string!
```
This allocates a new string on EVERY query, even simple ones.

#### Issue 3: Multiple Pattern Searches  
The fast path checker does many sequential searches:
- Check for "::"
- Check for "RETURNING"
- Check for "returning"  
- Check for "AT TIME ZONE"
- Check for "NOW()"
- ... and many more

Each search scans the entire query string.

## Root Cause
The regression isn't from the RETURNING logic itself, but from inefficiencies in how we check for patterns:
1. **String allocation** (to_uppercase) on every query
2. **Duplicate searches** for case variations
3. **Sequential pattern matching** instead of optimized approach

## Solution
1. Eliminate the to_uppercase() allocation - use case-insensitive comparisons
2. Combine duplicate searches into single passes
3. Consider more efficient pattern matching strategies

The RETURNING optimization logic is correct, but the implementation has performance issues that compound over thousands of queries.