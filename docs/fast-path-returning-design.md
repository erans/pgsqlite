# Fast Path for DML with RETURNING Design

## Problem
The benchmark shows INSERT operations with `RETURNING id` have 16,984% overhead compared to SQLite. This is because any query with RETURNING clause currently bypasses the fast path and goes through the expensive LazyQueryProcessor.

## Solution
Allow simple DML queries with basic RETURNING clauses to use the fast path.

## Criteria for Simple RETURNING

### Allowed (Fast Path Eligible):
1. `RETURNING id` - single column name
2. `RETURNING id, name, email` - multiple column names  
3. `RETURNING *` - all columns
4. Column names can contain underscores: `RETURNING user_id, created_at`

### NOT Allowed (Must Use Slow Path):
1. `RETURNING id::text` - type casts
2. `RETURNING upper(name)` - function calls
3. `RETURNING price * quantity` - expressions
4. `RETURNING now()` - function calls
5. `RETURNING CASE WHEN ... END` - complex expressions
6. `RETURNING (SELECT ...)` - subqueries
7. `RETURNING "quoted column"` - quoted identifiers (may need special handling)

## Implementation Approach

1. Add a helper function to check if RETURNING clause is simple
2. Update `is_fast_path_simple_query` to allow DML with simple RETURNING
3. Use byte-level pattern matching for performance
4. Ensure the rest of the query is still simple (no other complex features)

## Expected Impact
- INSERT with RETURNING id: Should see significant improvement
- UPDATE/DELETE with RETURNING: Similar improvements
- Complex RETURNING queries: Continue using slow path for correctness