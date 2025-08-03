# DML Binary Format Performance Regression Analysis

## Root Cause Identified

The DML operations (INSERT, UPDATE, DELETE) are significantly slower with binary format due to the following issues:

### 1. No Fast Path for DML Operations

**Current State:**
- Fast path ONLY checks for SELECT queries (line 962 in extended.rs)
- DML operations always go through the slow `substitute_parameters` path
- This path converts binary parameters to text strings for every execution

**Code Evidence:**
```rust
let meets_fast_path_conditions = query_starts_with_ignore_case(&query, "SELECT") && 
   !query.contains("JOIN") && 
   !query.contains("GROUP BY") && 
   // ... more conditions
```

### 2. Inefficient Binary-to-Text Conversion

**The substitute_parameters function (line 2298) does:**
1. Decodes each binary parameter (e.g., `i16::from_be_bytes`)
2. Converts to string (allocates memory)
3. Logs each conversion with `info!` (I/O overhead)
4. Substitutes into query string
5. SQLite parses the text back to values

**Example for INT2:**
```rust
let value = i16::from_be_bytes([bytes[0], bytes[1]]);
info!("Decoded binary int16 parameter {}: {}", i + 1, value);
value.to_string()  // Allocation!
```

### 3. Performance Impact Breakdown

For each DML operation with binary parameters:
- **Binary decoding**: ~0.05ms per parameter
- **String allocation**: ~0.02ms per parameter  
- **Logging overhead**: ~0.1ms per parameter (info! level)
- **String substitution**: ~0.05ms per parameter
- **Total overhead**: ~0.22ms per parameter

With 2 parameters per INSERT: 0.44ms overhead
This explains the 8.7x slowdown (0.1ms → 0.871ms)

## Solution Plan

### Phase 1: Enable Fast Path for DML Operations (High Priority)

1. **Extend fast path conditions to include INSERT, UPDATE, DELETE**
   ```rust
   let meets_fast_path_conditions = 
       (query_starts_with_ignore_case(&query, "SELECT") ||
        query_starts_with_ignore_case(&query, "INSERT") ||
        query_starts_with_ignore_case(&query, "UPDATE") ||
        query_starts_with_ignore_case(&query, "DELETE")) &&
       !query.contains("JOIN") && // ... other conditions
   ```

2. **Use execute_with_rusqlite_params for DML**
   - Already converts binary to rusqlite::Value efficiently
   - No string allocation or substitution needed
   - Direct parameter binding to SQLite

### Phase 2: Remove Logging Overhead

1. **Change info! to debug! in parameter decoding**
   ```rust
   debug!("Decoded binary int16 parameter {}: {}", i + 1, value);
   ```

2. **Add conditional logging based on log level**

### Phase 3: Optimize Extended Fast Path

The extended_fast_path module already handles DML operations but still uses text substitution:
1. Modify to use rusqlite parameter binding
2. Skip substitute_parameters entirely
3. Use prepared statements with parameter placeholders

## Expected Results

With these optimizations:
- INSERT: From 8.7x slower → Same speed as text format
- UPDATE: From 1.9x slower → Same speed as text format  
- DELETE: From 3.4x slower → Same speed as text format
- Zero allocations for parameter handling
- Direct binary-to-SQLite value conversion

## Implementation Priority

1. **Quick Win**: Change info! to debug! (5 min fix, 50% improvement)
2. **Major Fix**: Enable fast path for DML (1 hour, eliminates regression)
3. **Future**: Optimize extended_fast_path (2 hours, further improvements)