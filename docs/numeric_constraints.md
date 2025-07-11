# NUMERIC/DECIMAL Constraints Implementation

## Overview

pgsqlite now supports PostgreSQL-compatible NUMERIC and DECIMAL type constraints, including precision (total digits) and scale (decimal places) validation. This ensures data integrity by rejecting values that exceed the specified constraints, matching PostgreSQL's behavior.

## Features

### 1. Precision and Scale Parsing
- Extracts precision and scale from `NUMERIC(p,s)` and `DECIMAL(p,s)` type definitions
- Stores constraints using PostgreSQL's internal encoding format: `((precision << 16) | scale) + 4`
- Supports all valid PostgreSQL numeric constraints (precision up to 1000, scale -1000 to 1000)

### 2. Runtime Validation
- Validates INSERT and UPDATE operations against defined constraints
- Uses application-layer validation for reliability and flexibility
- Returns PostgreSQL-compatible error messages (error code 22003: numeric_value_out_of_range)
- Supports multi-row INSERT statements

### 3. Output Formatting
- Formats numeric values to the specified scale on retrieval
- Ensures consistent decimal places in query results
- Handles NULL values appropriately

## Architecture

### Storage Schema

```sql
-- Migration v7: Numeric constraints table
CREATE TABLE __pgsqlite_numeric_constraints (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    precision INTEGER NOT NULL,
    scale INTEGER NOT NULL,
    PRIMARY KEY (table_name, column_name)
);

CREATE INDEX idx_numeric_constraints_table 
ON __pgsqlite_numeric_constraints(table_name);
```

### Components

1. **CreateTableTranslator** (`src/translator/create_table_translator.rs`)
   - Enhanced `extract_type_modifier()` to handle NUMERIC(p,s) parsing
   - Encodes precision and scale using PostgreSQL's format

2. **NumericValidator** (`src/validator/numeric_validator.rs`)
   - Manages constraint caching for performance
   - Validates numeric values against precision/scale limits
   - Intercepts INSERT/UPDATE statements for validation
   - Handles multi-row INSERT VALUES syntax
   - Uses string-based validation for large precision values

3. **NumericFormatTranslator** (`src/translator/numeric_format_translator.rs`)
   - Replaces ::text casts with numeric_format() function calls
   - Ensures proper decimal formatting according to scale
   - Integrates with both simple and extended protocols

4. **NumericCastTranslator** (`src/translator/numeric_cast_translator.rs`)
   - Translates CAST(expr AS NUMERIC(p,s)) to numeric_cast() function
   - Validates and formats values during CAST operations
   - Supports scientific notation and edge cases

5. **Query Executor** (`src/query/executor.rs`)
   - Stores numeric constraints during CREATE TABLE
   - Integrates NumericValidator for runtime validation
   - Applies NumericFormatTranslator for proper output formatting

6. **Decimal Functions** (`src/functions/decimal_functions.rs`)
   - Implements numeric_format() SQLite function
   - Implements numeric_cast() SQLite function for CAST validation
   - Handles edge cases like values exceeding rust_decimal range
   - Provides string-based formatting for large numbers
   - Supports scientific notation (e.g., 1.23e2)

## Usage Examples

### Creating Tables with Numeric Constraints

```sql
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    price NUMERIC(10,2),      -- Up to 10 digits total, 2 decimal places
    weight DECIMAL(6,3),      -- Up to 6 digits total, 3 decimal places
    quantity NUMERIC(5,0),    -- Up to 5 digits, no decimals (integer)
    tax_rate NUMERIC(5,4)     -- Up to 5 digits, 4 decimal places
);
```

### Valid Operations

```sql
-- Valid inserts
INSERT INTO products (price, weight, quantity, tax_rate) 
VALUES (99999999.99, 999.999, 99999, 0.1875);

INSERT INTO products (price, weight, quantity, tax_rate) 
VALUES (0.01, 0.001, 0, 0.0001);

-- NULL values are allowed
INSERT INTO products (price, weight, quantity, tax_rate) 
VALUES (NULL, NULL, NULL, NULL);
```

### Constraint Violations

```sql
-- Too many total digits (11 > 10)
INSERT INTO products (price) VALUES (99999999.999);
-- ERROR: numeric field overflow

-- Too many decimal places (3 > 2)
INSERT INTO products (price) VALUES (100.999);
-- ERROR: numeric field overflow

-- Integer overflow
INSERT INTO products (quantity) VALUES (100000);
-- ERROR: numeric field overflow
```

### Output Formatting

```sql
-- Values are formatted to their defined scale
INSERT INTO products (price, weight) VALUES (123.4, 45);

SELECT price, weight FROM products;
-- Returns:
-- price  | weight
-- -------+--------
-- 123.40 | 45.000
```

## Implementation Details

### Type Modifier Encoding

The precision and scale are encoded in the `type_modifier` column of `__pgsqlite_schema`:

```
type_modifier = ((precision << 16) | (scale & 0xFFFF)) + 4
```

For example, NUMERIC(10,2):
- Precision: 10
- Scale: 2
- type_modifier: ((10 << 16) | 2) + 4 = 655366

### Application-Layer Validation

The NumericValidator intercepts INSERT and UPDATE statements to validate values before they reach SQLite:

```rust
// Example validation flow:
// 1. Parse INSERT/UPDATE statement to extract values
// 2. Check each value against its column's constraints
// 3. Return error if constraints are violated
// 4. Allow statement to proceed if all values are valid
```

This approach provides several advantages:
- Works reliably with all SQLite versions
- Supports complex validation logic (e.g., string-based for large numbers)
- Better error messages with column context
- Easier to maintain and debug
- Handles multi-row INSERT statements efficiently

### Performance Considerations

1. **Constraint Caching**: The system caches numeric constraints in memory to avoid repeated database queries

2. **Fast Path Detection**: Simple queries without numeric columns bypass validation entirely

3. **Lazy Loading**: Constraints are loaded on first use per table

4. **Numeric Formatting**: The numeric_format() function efficiently formats values during SELECT

## PostgreSQL Compatibility

### Supported Features
- NUMERIC(precision, scale) syntax
- DECIMAL as an alias for NUMERIC
- Precision up to 1000 digits
- Scale from -1000 to 1000
- Error code 22003 for constraint violations
- Proper NULL handling
- Exact decimal arithmetic

### Differences from PostgreSQL
- SQLite stores decimals as DECIMAL type (text-based for exact precision)
- Very large precision values (>28 digits) use string-based validation
- No support for special values like 'NaN' or 'Infinity'
- No automatic rounding (PostgreSQL rounds, pgsqlite rejects)
- Maximum practical precision limited by rust_decimal (28 significant digits)
- Computed values and expressions in INSERT...SELECT are not validated
- CAST operations format values but don't enforce constraints

## Migration from Existing Systems

### For New Databases
The numeric constraints table is created automatically when the database is initialized.

### For Existing Databases
Run pgsqlite with the `--migrate` flag to create the necessary tables:

```bash
pgsqlite --database mydb.db --migrate
```

### Backward Compatibility
- Tables without numeric constraints continue to work as before
- The system gracefully handles databases without the constraints table
- Type modifiers are stored but not enforced if constraints table is missing

## Testing

Comprehensive tests are available in multiple test files:

```bash
cargo test numeric_constraints    # Basic constraint tests
cargo test numeric_type          # Type-specific tests
```

Tests cover:
- Precision and scale validation
- Output formatting with ::text casts
- NULL value handling
- Negative numbers
- Multi-row INSERT statements
- Extended protocol support
- Edge cases (NUMERIC(38,10))
- Error messages and codes

## Future Enhancements

Potential improvements for future versions:

1. **Automatic Rounding**: Option to round values instead of rejecting them
2. **Custom Error Messages**: Table-specific constraint violation messages
3. **Performance Optimizations**: Compiled validation functions
4. **Extended Numeric Types**: Support for MONEY type constraints
5. **Constraint Modification**: ALTER TABLE support for changing constraints