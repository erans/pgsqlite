# psql \d Command Requirements

This document captures the exact SQL queries that psql sends when executing the `\d` command, based on PostgreSQL source code and testing.

## Overview

The psql `\d` command is used to:
- `\d` - List all tables, views, sequences, etc.
- `\d table_name` - Describe a specific table's structure

## Required pg_catalog Tables

### Currently Implemented
- `pg_class` - Table/relation information
- `pg_namespace` - Schema information  
- `pg_attribute` - Column information
- `pg_type` - Data type information
- `pg_enum` - Enum type values

### Missing Tables
1. **pg_constraint** - Constraint definitions (PRIMARY KEY, FOREIGN KEY, CHECK, etc.)
2. **pg_attrdef** - Column default values
3. **pg_collation** - Collation information
4. **pg_index** - Index information
5. **pg_description** - Comments on database objects

## Required System Functions

### Currently Implemented
- `pg_table_is_visible(oid)` - Check if table is in search path
- `format_type(oid, integer)` - Format type name with modifiers
- `pg_get_constraintdef(oid, boolean)` - Get constraint definition
- `pg_get_userbyid(oid)` - Get username from user ID
- `pg_get_expr(pg_node_tree, oid)` - Deparse expression trees
- `pg_get_indexdef(oid, integer, boolean)` - Get index definition

### Missing/Incomplete Functions
- Full support for `format_type` with type modifiers (e.g., varchar(255))
- `pg_get_expr` for complex default expressions
- `array_to_string` for array handling
- `substring` function for string manipulation

## Required Type Support

### Missing Type Features
1. **regclass** type casting - `'table_name'::regclass` converts table name to OID
2. **pg_node_tree** type - For storing parsed expression trees
3. **oid** type - Proper OID type handling (currently returned as text)

## Actual psql Queries

### 1. List Tables Query (\d without arguments)

```sql
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind 
    WHEN 'r' THEN 'table' 
    WHEN 'v' THEN 'view' 
    WHEN 'm' THEN 'materialized view' 
    WHEN 'i' THEN 'index'
    WHEN 'S' THEN 'sequence' 
    WHEN 's' THEN 'special' 
    WHEN 'f' THEN 'foreign table' 
    WHEN 'p' THEN 'partitioned table'
    WHEN 'I' THEN 'partitioned index' 
  END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','v','m','S','f','p')
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;
```

### 2. Describe Table Query (\d table_name)

```sql
-- Get column information
SELECT a.attname,
  pg_catalog.format_type(a.atttypid, a.atttypmod),
  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
   FROM pg_catalog.pg_attrdef d
   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
  a.attnotnull,
  a.attnum,
  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
  a.attidentity,
  a.attgenerated
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = 'table_name'::regclass AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum;

-- Get constraints
SELECT conname,
  pg_catalog.pg_get_constraintdef(r.oid, true) as condef
FROM pg_catalog.pg_constraint r
WHERE r.conrelid = 'table_name'::regclass ORDER BY 1;

-- Get indexes
SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, 
  pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
  pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred,
  i.indisreplident, c2.reltablespace
FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
WHERE c.oid = 'table_name'::regclass AND c.oid = i.indrelid AND i.indexrelid = c2.oid
ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
```

## Implementation Priority

1. **High Priority** (Required for basic \d functionality)
   - JOIN support between pg_class and pg_namespace
   - pg_constraint table with basic constraint information
   - regclass type casting support
   - Additional pg_attribute columns (atthasdef, attidentity, attgenerated)

2. **Medium Priority** (Improves \d output)
   - pg_attrdef table for column defaults
   - Better format_type implementation with type modifiers
   - pg_index table for index information

3. **Low Priority** (Nice to have)
   - pg_collation support
   - pg_description for comments
   - Full pg_get_expr implementation

## Current Limitations

1. **JOIN Queries**: Currently only pg_type JOIN queries are specially handled. Need generic JOIN support for catalog tables.

2. **Type System**: OID values are returned as text, which causes type mismatches with some PostgreSQL clients.

3. **Missing Columns**: pg_attribute is missing several columns that psql expects (atthasdef, attidentity, attgenerated).

4. **Schema Filtering**: The queries filter by schema (pg_catalog, information_schema, pg_toast) which we don't fully support.

## Testing Approach

To test psql compatibility:
1. Start pgsqlite with debug logging: `RUST_LOG=debug cargo run -- --in-memory`
2. Connect with psql: `psql -h localhost -p 5432 -U postgres test`
3. Create test tables and run `\d` commands
4. Capture the exact SQL queries from debug logs
5. Implement missing features based on the queries