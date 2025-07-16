# JSON/JSONB Support in pgsqlite

pgsqlite provides comprehensive support for PostgreSQL's JSON and JSONB data types, allowing you to store, query, and manipulate JSON data in SQLite databases using familiar PostgreSQL syntax.

## Overview

Both `JSON` and `JSONB` types are supported and stored as TEXT in SQLite. The key features include:

- **Automatic validation**: JSON columns include validation constraints to ensure data integrity
- **PostgreSQL operators**: All major JSON operators (`->`, `->>`, `@>`, etc.) are translated to SQLite-compatible functions
- **Rich function library**: Core PostgreSQL JSON functions are implemented as SQLite custom functions
- **Seamless integration**: Works with both simple and extended query protocols

## Data Types

### JSON vs JSONB

In pgsqlite, both `JSON` and `JSONB` types are stored identically as TEXT in SQLite:

```sql
CREATE TABLE example (
    data JSON,      -- Stored as TEXT with JSON validation
    config JSONB    -- Also stored as TEXT with JSON validation
);
```

While PostgreSQL stores JSONB in a binary format for faster processing, pgsqlite's implementation provides the same functionality with TEXT storage, relying on SQLite's efficient JSON functions.

## JSON Operators

pgsqlite translates PostgreSQL JSON operators to SQLite-compatible functions automatically:

### Field Extraction Operators

| Operator | Description | Example | Result |
|----------|-------------|---------|--------|
| `->` | Extract JSON field as JSON | `'{"a": {"b": 2}}'::json -> 'a'` | `{"b": 2}` |
| `->>` | Extract JSON field as text | `'{"a": 1}'::json ->> 'a'` | `1` |

```sql
-- Extract nested JSON
SELECT data->'address'->'city' FROM users;

-- Extract as text
SELECT data->>'email' FROM users;

-- Array element access
SELECT data->'items'->0 FROM orders;
SELECT data->'skills'->>1 FROM profiles;
```

### Path Extraction Operators

| Operator | Description | Example | Result |
|----------|-------------|---------|--------|
| `#>` | Extract at path as JSON | `'{"a": {"b": {"c": 1}}}'::json #> '{a,b}'` | `{"c": 1}` |
| `#>>` | Extract at path as text | `'{"a": {"b": 2}}'::json #>> '{a,b}'` | `2` |

```sql
-- Path extraction
SELECT data#>'{address,coordinates,lat}' FROM locations;
SELECT data#>>'{user,profile,name}' FROM accounts;
```

### Containment Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `@>` | Does JSON contain | `'{"a": 1, "b": 2}'::jsonb @> '{"a": 1}'` |
| `<@` | Is JSON contained by | `'{"a": 1}'::jsonb <@ '{"a": 1, "b": 2}'` |

```sql
-- Find records containing specific JSON
SELECT * FROM products WHERE specs @> '{"color": "red"}';

-- Check if value is contained
SELECT * FROM orders WHERE '{"status": "pending"}' <@ metadata;
```

### Existence Operators

The following operators check for key existence in JSON objects:

| Operator | Description | Example |
|----------|-------------|---------|
| `?` | Does key exist | `'{"a": 1, "b": 2}'::jsonb ? 'a'` |
| `?|` | Do any of the keys exist | `'{"a": 1, "b": 2}'::jsonb ?| ARRAY['a', 'c']` |
| `?&` | Do all of the keys exist | `'{"a": 1, "b": 2}'::jsonb ?& ARRAY['a', 'b']` |

```sql
-- Check if key exists
SELECT * FROM users WHERE profile ? 'email';

-- Check if any of several keys exist
SELECT * FROM products WHERE specs ?| ARRAY['color', 'size', 'weight'];

-- Check if all required keys exist
SELECT * FROM orders WHERE metadata ?& ARRAY['customer_id', 'order_date'];
```

## JSON Functions

### Validation and Type Functions

#### json_valid(json)
Validates if a string is valid JSON.

```sql
SELECT json_valid('{"valid": true}');  -- Returns 1
SELECT json_valid('{invalid}');        -- Returns 0
```

#### json_typeof(json) / jsonb_typeof(jsonb)
Returns the type of the JSON value.

```sql
SELECT json_typeof('123');           -- 'number'
SELECT json_typeof('"text"');        -- 'string'
SELECT json_typeof('{"a": 1}');      -- 'object'
SELECT json_typeof('[1,2,3]');       -- 'array'
SELECT json_typeof('true');          -- 'boolean'
SELECT json_typeof('null');          -- 'null'
```

### Array Functions

#### json_array_length(json) / jsonb_array_length(jsonb)
Returns the length of a JSON array.

```sql
SELECT json_array_length('[1, 2, 3, 4, 5]');  -- Returns 5
SELECT json_array_length('{"a": 1}');         -- Returns NULL (not an array)
```

#### json_array_elements(json) / jsonb_array_elements(jsonb)
Expands a JSON array to a set of JSON values (currently returns comma-separated string).

```sql
SELECT json_array_elements('[1, 2, 3]');
-- Returns: "1,2,3"
```

#### json_array_elements_text(json)
Expands a JSON array to a set of text values.

```sql
SELECT json_array_elements_text('["a", "b", "c"]');
-- Returns: "a,b,c"
```

### Object Functions

#### jsonb_object_keys(jsonb)
Returns the keys of a JSON object.

```sql
SELECT jsonb_object_keys('{"name": "John", "age": 30, "city": "NYC"}');
-- Returns: "name,age,city"
```

### Conversion Functions

#### to_json(anyelement) / to_jsonb(anyelement)
Converts any value to JSON.

```sql
SELECT to_json('hello world');    -- Returns: "hello world"
SELECT to_json(123);              -- Returns: 123
SELECT to_json(NULL);             -- Returns: null
```

#### json_build_object(variadic)
Builds a JSON object from key-value pairs.

```sql
SELECT json_build_object('name', 'John', 'age', 30);
-- Returns: {"name": "John", "age": 30}
```

### Manipulation Functions

#### json_strip_nulls(json) / jsonb_strip_nulls(jsonb)
Removes all null values from JSON.

```sql
SELECT json_strip_nulls('{"a": 1, "b": null, "c": {"d": null, "e": 2}}');
-- Returns: {"a": 1, "c": {"e": 2}}
```

#### jsonb_set(jsonb, text[], jsonb)
Sets a value at the specified path.

```sql
SELECT jsonb_set('{"a": 1, "b": 2}', '{b}', '99');
-- Returns: {"a": 1, "b": 99}

SELECT jsonb_set('{"a": {"b": 1}}', '{a,c}', '"new"');
-- Returns: {"a": {"b": 1, "c": "new"}}
```

### Path Functions

#### json_extract_path(json, variadic text)
Extracts a value at the specified path.

```sql
SELECT json_extract_path('{"a": {"b": {"c": 42}}}', 'a', 'b', 'c');
-- Returns: 42
```

#### json_extract_path_text(json, variadic text)
Extracts a value at the specified path as text.

```sql
SELECT json_extract_path_text('{"name": "John", "age": 30}', 'name');
-- Returns: John
```

### Containment Functions

#### jsonb_contains(jsonb, jsonb)
Checks if the first JSON contains the second.

```sql
SELECT jsonb_contains('{"a": 1, "b": 2}', '{"a": 1}');  -- Returns 1
SELECT jsonb_contains('{"a": 1}', '{"b": 2}');          -- Returns 0
```

#### jsonb_contained(jsonb, jsonb)
Checks if the first JSON is contained by the second.

```sql
SELECT jsonb_contained('{"a": 1}', '{"a": 1, "b": 2}');  -- Returns 1
```

### Record Conversion Functions

#### json_populate_record(base_record, json_object)
Populates a record from JSON object data. In pgsqlite, this returns a formatted string representation acknowledging the operation.

```sql
SELECT json_populate_record('null', '{"name": "Alice", "age": 30}');
-- Returns: json_populate_record: base=null, json={"name": "Alice", "age": 30}
```

#### json_to_record(json_object)
Converts a JSON object to a record-like string representation with key:value pairs.

```sql
SELECT json_to_record('{"id": 1, "name": "Bob", "active": true}');
-- Returns: (id:1,name:Bob,active:true)

SELECT json_to_record('{"user": "Charlie", "score": 95, "verified": false}');
-- Returns: (user:Charlie,score:95,verified:false)

-- Handles edge cases
SELECT json_to_record('{}');  -- Returns: ()
SELECT json_to_record('[{"invalid": "array"}]');  -- Returns error message
```

**Note**: These functions provide simplified implementations of PostgreSQL's record conversion capabilities. Full RECORD type support would require significant infrastructure changes in SQLite.

## Practical Examples

### Creating Tables with JSON

```sql
-- User profiles with flexible attributes
CREATE TABLE user_profiles (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    profile JSON,
    settings JSONB
);

-- Product catalog with specifications
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    specs JSONB,
    metadata JSON
);
```

### Inserting JSON Data

```sql
-- Insert user profile
INSERT INTO user_profiles (username, profile, settings) VALUES 
('john_doe', 
 '{"name": "John Doe", "age": 30, "interests": ["coding", "music"]}',
 '{"theme": "dark", "notifications": {"email": true, "sms": false}}');

-- Insert product with specifications
INSERT INTO products (name, specs, metadata) VALUES
('Laptop Pro', 
 '{"cpu": "Intel i7", "ram": "16GB", "storage": {"type": "SSD", "size": "512GB"}}',
 '{"created": "2024-01-15", "tags": ["electronics", "computers"]}');
```

### Querying JSON Data

```sql
-- Find users interested in coding
SELECT username, profile->>'name' as full_name
FROM user_profiles
WHERE profile @> '{"interests": ["coding"]}';

-- Get products with specific CPU
SELECT name, specs->>'cpu' as processor
FROM products
WHERE specs->>'cpu' LIKE 'Intel%';

-- Extract nested values
SELECT 
    name,
    specs#>>'{storage,size}' as storage_size,
    specs#>>'{storage,type}' as storage_type
FROM products;

-- Complex filtering with multiple operators
SELECT 
    username,
    profile->>'age' as age,
    settings#>>'{notifications,email}' as email_notifications
FROM user_profiles
WHERE 
    profile->>'age' > '25' 
    AND settings @> '{"theme": "dark"}';
```

### Updating JSON Data

```sql
-- Update a specific field in JSON
UPDATE user_profiles
SET settings = jsonb_set(settings, '{theme}', '"light"')
WHERE username = 'john_doe';

-- Add a new field to JSON
UPDATE products
SET specs = jsonb_set(specs, '{warranty}', '"2 years"')
WHERE name = 'Laptop Pro';
```

## Performance Considerations

1. **Indexing**: While PostgreSQL supports GIN indexes on JSONB columns, SQLite doesn't have equivalent functionality. Consider extracting frequently queried JSON fields into separate columns for better performance.

2. **Validation**: JSON validation happens during INSERT/UPDATE operations. For bulk operations, this may add overhead.

3. **Operator Translation**: JSON operators are translated to SQLite functions at query time. Complex queries with many operators may have additional translation overhead.

4. **Storage**: JSON is stored as TEXT in SQLite, which means no binary optimization like PostgreSQL's JSONB. However, SQLite's JSON functions are highly optimized.

## Limitations and Differences from PostgreSQL

1. **Storage Format**: Both JSON and JSONB are stored as TEXT (no binary format)
2. **Indexing**: No GIN or GiST index support for JSON columns
3. **Operators**: Existence operators (`?`, `?|`, `?&`) are not yet implemented
4. **Functions**: Some advanced functions are not yet implemented:
   - `json_each()` / `jsonb_each()`
   - `json_populate_record()`
   - `json_agg()` / `jsonb_agg()`
   - `row_to_json()`
   - JSON path expressions (jsonpath)

## Best Practices

1. **Validate JSON before insertion** when possible to avoid runtime errors
2. **Use JSONB type** for consistency with PostgreSQL applications
3. **Extract frequently queried fields** into separate columns for better performance
4. **Keep JSON documents reasonably sized** as they're stored and processed as TEXT
5. **Use batch operations** when inserting multiple JSON records for better performance

## Migration from PostgreSQL

When migrating from PostgreSQL to pgsqlite:

1. **Table definitions** work without changes - both JSON and JSONB are supported
2. **Queries using operators** are automatically translated
3. **Most common functions** work identically
4. **Consider performance implications** for large JSON documents or complex queries
5. **Test existence operators** (`?`, `?|`, `?&`) as they're not yet supported

## Future Enhancements

Planned improvements for JSON support include:

- Implementation of existence operators (`?`, `?|`, `?&`)
- Table-valued functions like `json_each()`, `json_array_elements()` returning proper result sets
- Aggregate functions (`json_agg()`, `json_object_agg()`)
- JSON path expression support
- Performance optimizations for large JSON documents