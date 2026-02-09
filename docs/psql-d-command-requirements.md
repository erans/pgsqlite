# psql \d Command Requirements

This document captures the SQL queries that psql sends when executing the `\d` command, and notes what pgsqlite needs to support those queries.

## Overview

The psql `\d` command is used to:
- `\d` - List all tables, views, sequences, etc.
- `\d table_name` - Describe a specific table's structure

## Required pg_catalog Tables

pgsqlite implements a number of `pg_catalog` relations that psql introspection relies on. The exact completeness depends on the specific query shape psql issues.

Notable catalog relations used by `\d` and related commands:

- `pg_class`, `pg_namespace`, `pg_attribute`, `pg_type`, `pg_enum`
- `pg_constraint`, `pg_attrdef`, `pg_index`, `pg_description`
- `pg_proc` (used by `\\df` and some `\d` output)

## Required System Functions

psql uses several `pg_catalog.*` helper functions in its queries. pgsqlite includes compatibility implementations for a subset of these.

Commonly observed:

- `pg_table_is_visible(oid)`
- `format_type(oid, integer)`
- `pg_get_constraintdef(oid, boolean)`
- `pg_get_userbyid(oid)`
- `pg_get_expr(pg_node_tree, oid)`
- `pg_get_indexdef(oid, integer, boolean)`

Note: function behavior may be simplified compared to PostgreSQL, but is sufficient for common introspection patterns.

## Required Type Support

psql commonly uses regclass casts like `'table_name'::regclass` inside catalog queries. pgsqlite includes special handling for these patterns in its catalog query interception layer.

Note: some catalog OID values are represented as TEXT internally for convenience, which can affect edge-case query shapes.

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

`\\d`-family commands are sensitive to subtle catalog-query differences. Even with many catalog relations present, some `\\d table_name` queries may not fully work yet depending on the join shape, regclass casting, and the specific columns requested.

For a practical view of what currently works well, see the SQL scripts under `tests/sql/meta/` (they group psql meta-commands into "supported" / "working" subsets).

## Testing Approach

To test psql compatibility:
1. Start pgsqlite with debug logging: `cargo run -- --in-memory --log-level debug`
2. Connect with psql: `psql -h localhost -p 5432 -U postgres test`
3. Create test tables and run `\d` commands
4. Capture the exact SQL queries from debug logs
5. Implement missing features based on the queries
