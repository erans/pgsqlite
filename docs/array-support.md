# Array Support in pgsqlite

pgsqlite provides comprehensive support for PostgreSQL array types, allowing you to store and query array data in SQLite databases using familiar PostgreSQL syntax.

## Overview

PostgreSQL arrays are fully supported and stored as JSON in SQLite. Key features include:

- **All base type arrays**: Support for INTEGER[], TEXT[], BOOLEAN[], REAL[], and 30+ other array types
- **Multi-dimensional arrays**: Support for nested arrays like INTEGER[][] or TEXT[][][]
- **Automatic validation**: Array columns include JSON validation constraints
- **PostgreSQL syntax**: Both ARRAY[1,2,3] constructor and '{1,2,3}' literal formats
- **Wire protocol support**: Proper array type OIDs transmitted to clients

## Supported Array Types

pgsqlite supports arrays for all major PostgreSQL types:

### Numeric Arrays
- `SMALLINT[]` / `INT2[]` - Small integer arrays
- `INTEGER[]` / `INT4[]` - Integer arrays  
- `BIGINT[]` / `INT8[]` - Big integer arrays
- `REAL[]` / `FLOAT4[]` - Single precision float arrays
- `DOUBLE PRECISION[]` / `FLOAT8[]` - Double precision float arrays
- `NUMERIC[]` / `DECIMAL[]` - Arbitrary precision number arrays

### Text Arrays
- `TEXT[]` - Variable length text arrays
- `VARCHAR[]` - Variable length character arrays
- `CHAR[]` - Fixed length character arrays
- `NAME[]` - Name type arrays (63 byte strings)

### Boolean Arrays
- `BOOLEAN[]` / `BOOL[]` - Boolean value arrays

### Date/Time Arrays
- `DATE[]` - Date arrays
- `TIME[]` - Time without timezone arrays
- `TIMESTAMP[]` - Timestamp without timezone arrays
- `TIMESTAMPTZ[]` - Timestamp with timezone arrays
- `INTERVAL[]` - Time interval arrays

### Binary Arrays
- `BYTEA[]` - Binary data arrays

### Other Type Arrays
- `UUID[]` - UUID arrays
- `JSON[]` - JSON arrays
- `JSONB[]` - JSONB arrays
- `INET[]` - IPv4/IPv6 address arrays
- `CIDR[]` - Network address arrays
- `MACADDR[]` - MAC address arrays

## Creating Tables with Arrays

```sql
CREATE TABLE example (
    id SERIAL PRIMARY KEY,
    tags TEXT[],
    scores INTEGER[],
    matrix INTEGER[][],
    active_days BOOLEAN[7]
);
```

## Inserting Array Data

### Using PostgreSQL Array Literals

```sql
-- Single-dimensional arrays
INSERT INTO example (tags, scores) VALUES 
    ('{"red", "blue", "green"}', '{95, 87, 92}'),
    ('{"urgent", "bug"}', '{100}');

-- Empty arrays
INSERT INTO example (tags, scores) VALUES 
    ('{}', '{}');

-- Arrays with NULL values
INSERT INTO example (tags, scores) VALUES 
    ('{"first", NULL, "third"}', '{1, NULL, 3}');

-- Multi-dimensional arrays
INSERT INTO example (matrix) VALUES 
    ('{{1,2,3}, {4,5,6}}'),
    ('{{{1,2}, {3,4}}, {{5,6}, {7,8}}}');
```

### Using ARRAY Constructor (Limited Support)

```sql
-- Note: ARRAY constructor is converted to JSON internally
INSERT INTO example (scores) VALUES 
    (ARRAY[10, 20, 30]);
```

## Querying Array Data

### Basic Queries

```sql
-- Select all rows with non-empty arrays
SELECT * FROM example WHERE tags != '{}';

-- Select rows with NULL arrays
SELECT * FROM example WHERE scores IS NULL;

-- Array equality
SELECT * FROM example WHERE tags = '{"urgent", "bug"}';
```

### Using JSON Functions for Array Operations

Since arrays are stored as JSON, you can use SQLite's JSON functions:

```sql
-- Get array length
SELECT id, json_array_length(scores) as num_scores 
FROM example;

-- Extract array element (0-based index)
SELECT id, json_extract(tags, '$[0]') as first_tag 
FROM example;

-- Extract multiple elements
SELECT id, 
    json_extract(scores, '$[0]') as first_score,
    json_extract(scores, '$[1]') as second_score
FROM example;

-- Check if array contains a value (using JSON)
SELECT * FROM example 
WHERE json_extract(tags, '$') LIKE '%urgent%';
```

## Multi-Dimensional Arrays

```sql
-- Create table with 2D array
CREATE TABLE matrices (
    id INTEGER PRIMARY KEY,
    data INTEGER[][]
);

-- Insert 2D array
INSERT INTO matrices (id, data) VALUES 
    (1, '{{1,2,3}, {4,5,6}, {7,8,9}}');

-- Access nested elements
SELECT json_extract(data, '$[0][0]') as top_left,
       json_extract(data, '$[1][1]') as center
FROM matrices;
```

## Array Storage Details

Arrays are stored as JSON TEXT in SQLite with automatic validation:

1. **Storage format**: JSON arrays preserve PostgreSQL array structure
2. **Type preservation**: Numbers stay numbers, strings stay strings
3. **NULL handling**: JSON null represents SQL NULL in arrays
4. **Validation**: CHECK constraint ensures valid JSON using `json_valid()`

Example of how arrays are stored:
- PostgreSQL: `'{1,2,3}'` → SQLite: `'[1,2,3]'`
- PostgreSQL: `'{"a","b","c"}'` → SQLite: `'["a","b","c"]'`
- PostgreSQL: `'{{1,2},{3,4}}'` → SQLite: `'[[1,2],[3,4]]'`

## Array Operators and Functions

pgsqlite supports most PostgreSQL array operators and functions:

### Array Operators

- **`@>` (contains)**: Check if array contains another array
  ```sql
  SELECT * FROM products WHERE tags @> '["electronics"]';
  ```

- **`<@` (is contained by)**: Check if array is contained by another
  ```sql
  SELECT * FROM products WHERE '["laptop"]' <@ tags;
  ```

- **`&&` (overlap)**: Check if arrays have any elements in common
  ```sql
  SELECT * FROM products WHERE tags && '["electronics", "books"]';
  ```

- **`||` (concatenation)**: Concatenate arrays
  ```sql
  SELECT tags || '["new-tag"]' FROM products;
  ```

### Array Functions

- **`array_length(array, dimension)`**: Get array length for specified dimension
  ```sql
  SELECT array_length(tags, 1) FROM products;
  ```

- **`array_upper(array, dimension)` / `array_lower(array, dimension)`**: Get array bounds (always 1-based)
  ```sql
  SELECT array_upper(scores, 1), array_lower(scores, 1) FROM example;
  ```

- **`array_append(array, element)` / `array_prepend(element, array)`**: Add elements
  ```sql
  SELECT array_append(tags, 'new-tag') FROM products;
  SELECT array_prepend('first', tags) FROM products;
  ```

- **`array_cat(array1, array2)`**: Concatenate arrays (same as || operator)
  ```sql
  SELECT array_cat(tags, '["extra", "tags"]') FROM products;
  ```

- **`array_remove(array, element)`**: Remove all occurrences of an element
  ```sql
  -- Note: element must be JSON-encoded for now
  SELECT array_remove(tags, '"electronics"') FROM products;
  ```

- **`array_replace(array, search, replace)`**: Replace all occurrences
  ```sql
  SELECT array_replace(tags, '"old"', '"new"') FROM products;
  ```

- **`array_position(array, element)` / `array_positions(array, element)`**: Find element positions (1-based)
  ```sql
  SELECT array_position(tags, '"electronics"') FROM products;
  ```

- **`array_slice(array, start, end)`**: Extract array slice
  ```sql
  SELECT array_slice(tags, 2, 4) FROM products;
  ```

- **`array_agg(expression)`**: Aggregate values into an array
  ```sql
  SELECT category, array_agg(name) FROM products GROUP BY category;
  ```

### Array Access

- **Subscript access**: Access individual elements (1-based indexing)
  ```sql
  SELECT tags[1] FROM products;  -- First element
  ```

- **Array slicing**: Extract a range of elements
  ```sql
  SELECT tags[2:4] FROM products;  -- Elements 2 through 4
  ```

### ANY/ALL Operators

- **`value = ANY(array)`**: Check if value equals any array element
  ```sql
  SELECT * FROM products WHERE 'electronics' = ANY(tags);
  ```

- **`value = ALL(array)`**: Check if value satisfies condition for all elements
  ```sql
  SELECT * FROM orders WHERE 100 < ALL(quantities);
  ```

## Current Limitations

The following features are not yet supported:

1. **Advanced Array Functions**:
   - `unnest(array)` - Set-returning function (requires table function support)
   - `array_agg` with ORDER BY or DISTINCT

2. **Binary Protocol**:
   - Arrays are returned as JSON strings, not in PostgreSQL binary array format
   - Clients expecting binary array encoding may have issues

3. **Array Constructors**:
   - Limited ARRAY[...] constructor support (converted to JSON internally)
   - Array input/output functions

## Workarounds

### Unnest Alternative

Use SQLite's `json_each()` function:

```sql
-- PostgreSQL: SELECT unnest(tags) FROM example
-- pgsqlite workaround:
SELECT value FROM example, json_each(tags);
```

### Array Contains Check

```sql
-- Check if array contains a specific value
SELECT * FROM example 
WHERE EXISTS (
    SELECT 1 FROM json_each(tags) 
    WHERE value = 'urgent'
);
```

### Array Aggregation Alternative

Use SQLite's `json_group_array()`:

```sql
-- PostgreSQL: SELECT array_agg(name) FROM users
-- pgsqlite workaround:
SELECT json_group_array(name) as names FROM users;
```

## Performance Considerations

1. **JSON Validation**: Happens during INSERT/UPDATE operations
2. **No Indexing**: Cannot create indexes on array elements
3. **Full Table Scans**: Array content searches require scanning all rows
4. **Large Arrays**: Very large arrays may impact performance

## Migration from PostgreSQL

When migrating from PostgreSQL:

1. **Table definitions** work without changes - array types are recognized
2. **INSERT statements** work with PostgreSQL array literal syntax
3. **Simple queries** comparing entire arrays work as expected
4. **Complex array operations** need to be rewritten using JSON functions
5. **Consider performance** implications for large arrays or complex queries

## Best Practices

1. **Use appropriate array types** for your data (INTEGER[] for numbers, TEXT[] for strings)
2. **Keep arrays reasonably sized** - very large arrays impact performance
3. **Consider normalization** for frequently queried array elements
4. **Use JSON functions** for array manipulation rather than string operations
5. **Test with your data** to ensure performance meets requirements

## Example: Tags System

```sql
-- Create articles with tags
CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    tags TEXT[]
);

-- Insert articles with tags
INSERT INTO articles (title, tags) VALUES
    ('PostgreSQL Arrays', '{"database", "postgresql", "arrays"}'),
    ('SQLite Tips', '{"database", "sqlite", "performance"}'),
    ('pgsqlite Guide', '{"database", "postgresql", "sqlite"}');

-- Find articles with specific tag
SELECT id, title 
FROM articles 
WHERE EXISTS (
    SELECT 1 FROM json_each(tags) 
    WHERE value = 'postgresql'
);

-- Count tags per article
SELECT title, json_array_length(tags) as tag_count 
FROM articles;

-- Get all unique tags (using json_each)
SELECT DISTINCT value as tag 
FROM articles, json_each(tags) 
ORDER BY tag;
```

## Integration with CI/CD

Array support is fully tested in pgsqlite's CI/CD pipeline:

- Integration tests in `test_queries.sql`
- Rust unit tests in `array_types_test.rs`
- Tested across all connection modes (TCP, Unix socket, with/without SSL)
- Automatic migration creates necessary metadata tables