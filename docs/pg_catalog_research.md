# PostgreSQL System Catalog Research for pgsqlite

## Overview
This document outlines the PostgreSQL system catalog tables and the queries that psql uses for its `\d` commands. This research will guide the implementation of catalog query handlers in pgsqlite.

## Key PostgreSQL Catalog Tables

### pg_class
Stores information about tables, indexes, sequences, views, materialized views, composite types, and other relations.

Key columns:
- `oid` (oid): Row object identifier
- `relname` (name): Name of the table, index, view, etc.
- `relnamespace` (oid): OID of namespace (schema) containing this relation
- `reltype` (oid): OID of data type corresponding to table's row type
- `relowner` (oid): Owner of the relation
- `relam` (oid): Access method used (0 for tables)
- `relfilenode` (oid): Name of on-disk file
- `reltablespace` (oid): Tablespace in which relation is stored
- `relpages` (int4): Size of on-disk representation in pages
- `reltuples` (float4): Number of rows in the table
- `reltoastrelid` (oid): OID of TOAST table
- `relhasindex` (bool): True if table has indexes
- `relisshared` (bool): True if table is shared across databases
- `relpersistence` (char): 'p' = permanent, 't' = temporary, 'u' = unlogged
- `relkind` (char): 'r' = table, 'i' = index, 'S' = sequence, 'v' = view, 'm' = materialized view, 'c' = composite type, 'f' = foreign table, 'p' = partitioned table
- `relnatts` (int2): Number of user columns
- `relchecks` (int2): Number of CHECK constraints
- `relhasrules` (bool): Table has rules
- `relhastriggers` (bool): Table has triggers
- `relhassubclass` (bool): Table has subclasses
- `relrowsecurity` (bool): Row-level security enabled
- `relforcerowsecurity` (bool): Row security forced for table owner
- `relispopulated` (bool): True if relation is populated
- `relreplident` (char): Columns used to form replica identity
- `relispartition` (bool): True if table is a partition
- `relrewrite` (oid): OID of relation being rewritten, else 0
- `relfrozenxid` (xid): Transaction ID before which table data is frozen
- `relminmxid` (xid): Minimum multixact ID in table

### pg_attribute
Stores information about table columns.

Key columns:
- `attrelid` (oid): Table this column belongs to
- `attname` (name): Column name
- `atttypid` (oid): Data type OID
- `attstattarget` (int4): Statistics target
- `attlen` (int2): Length of data type
- `attnum` (int2): Column number (1-based)
- `attndims` (int4): Number of array dimensions
- `attcacheoff` (int4): Cached offset
- `atttypmod` (int4): Type modifier (e.g., varchar length)
- `attbyval` (bool): Is type passed by value?
- `attstorage` (char): Storage strategy
- `attalign` (char): Alignment
- `attnotnull` (bool): NOT NULL constraint
- `atthasdef` (bool): Has default value
- `atthasmissing` (bool): Has missing value
- `attidentity` (char): Identity column type
- `attgenerated` (char): Generated column type
- `attisdropped` (bool): Column has been dropped
- `attislocal` (bool): Defined locally
- `attinhcount` (int4): Number of inheritance ancestors
- `attcollation` (oid): Collation OID
- `attacl` (aclitem[]): Access privileges
- `attoptions` (text[]): Attribute options
- `attfdwoptions` (text[]): FDW options
- `attmissingval` (anyarray): Missing value

### pg_namespace
Stores schemas.

Key columns:
- `oid` (oid): Row object identifier
- `nspname` (name): Name of namespace
- `nspowner` (oid): Owner of namespace
- `nspacl` (aclitem[]): Access privileges

### pg_index
Stores indexes.

Key columns:
- `indexrelid` (oid): OID of index
- `indrelid` (oid): OID of table this index is for
- `indnatts` (int2): Number of columns in index
- `indnkeyatts` (int2): Number of key columns
- `indisunique` (bool): Is unique index
- `indisprimary` (bool): Is primary key
- `indisexclusion` (bool): Is exclusion constraint
- `indimmediate` (bool): Is constraint immediate
- `indisclustered` (bool): Is clustered index
- `indisvalid` (bool): Is index valid for queries
- `indcheckxmin` (bool): Must check xmin
- `indisready` (bool): Is ready for inserts
- `indislive` (bool): Is index alive
- `indisreplident` (bool): Is replica identity
- `indkey` (int2vector): Column numbers
- `indcollation` (oidvector): Collation OIDs
- `indclass` (oidvector): Operator class OIDs
- `indoption` (int2vector): Per-column flags
- `indexprs` (pg_node_tree): Expression trees for expression indexes
- `indpred` (pg_node_tree): Partial index predicate

### pg_constraint
Stores check, primary key, unique, foreign key, and exclusion constraints.

Key columns:
- `oid` (oid): Row object identifier
- `conname` (name): Constraint name
- `connamespace` (oid): Namespace OID
- `contype` (char): 'c' = check, 'f' = foreign key, 'p' = primary key, 'u' = unique, 'x' = exclusion
- `condeferrable` (bool): Is deferrable
- `condeferred` (bool): Is initially deferred
- `convalidated` (bool): Is validated
- `conrelid` (oid): Table this constraint is on
- `contypid` (oid): Domain this constraint is on
- `conindid` (oid): Index supporting this constraint
- `conparentid` (oid): Parent constraint
- `confrelid` (oid): Referenced table if foreign key
- `confupdtype` (char): Foreign key update action
- `confdeltype` (char): Foreign key delete action
- `confmatchtype` (char): Foreign key match type
- `conislocal` (bool): Is locally defined
- `coninhcount` (int4): Number of inheritance ancestors
- `connoinherit` (bool): Constraint is non-inheritable
- `conkey` (int2[]): Column numbers
- `confkey` (int2[]): Referenced columns if foreign key
- `conpfeqop` (oid[]): Equality operators if foreign key
- `conppeqop` (oid[]): PK = PK equality operators
- `conffeqop` (oid[]): FK = FK equality operators
- `conexclop` (oid[]): Exclusion operators
- `conbin` (pg_node_tree): Check constraint expression

## Common psql \d Queries

### \d - List all relations
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
WHERE c.relkind IN ('r','p','v','m','S','f','')
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;
```

### \dt - List tables only
```sql
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind 
    WHEN 'r' THEN 'table' 
    WHEN 'p' THEN 'partitioned table'
  END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p')
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;
```

### \d table_name - Describe a specific table
```sql
SELECT a.attname,
  pg_catalog.format_type(a.atttypid, a.atttypmod) as "Type",
  a.attnotnull as "Nullable",
  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
   FROM pg_catalog.pg_attrdef d
   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) as "Default",
  a.attidentity as "Identity",
  a.attgenerated as "Generated"
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = 'schema.table_name'::regclass
  AND a.attnum > 0 
  AND NOT a.attisdropped
ORDER BY a.attnum;
```

Plus indexes:
```sql
SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid,
  pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
  pg_catalog.pg_get_constraintdef(con.oid, true), contype,
  condeferrable, condeferred, i.indisreplident, c2.reltablespace
FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
WHERE c.oid = 'schema.table_name'::regclass 
  AND c.oid = i.indrelid 
  AND i.indexrelid = c2.oid
ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
```

### \di - List indexes
```sql
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'i' THEN 'index' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
  c2.relname as "Table"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
WHERE c.relkind IN ('i','I')
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;
```

## Mapping SQLite to PostgreSQL Catalogs

### SQLite Sources
- `PRAGMA table_list` - List of tables
- `PRAGMA table_info(table)` - Column information
- `PRAGMA index_list(table)` - Indexes on a table
- `PRAGMA index_info(index)` - Columns in an index
- `PRAGMA foreign_key_list(table)` - Foreign keys
- `sqlite_master` table - Schema information

### Mapping Strategy

#### pg_class mapping
- `oid`: Generate unique ID from table name hash
- `relname`: From SQLite table name
- `relnamespace`: Use fixed value for 'public' schema (2200)
- `reltype`: Generate from oid
- `relowner`: Use fixed value
- `relkind`: 'r' for regular tables
- `relnatts`: Count from PRAGMA table_info
- Other fields: Use sensible defaults

#### pg_attribute mapping
- `attrelid`: From pg_class oid
- `attname`: Column name from PRAGMA table_info
- `atttypid`: Map SQLite types to PostgreSQL type OIDs
- `attnum`: Column position (cid + 1)
- `attnotnull`: From notnull field
- `atthasdef`: From dflt_value presence
- `atttypmod`: Extract from type declaration (e.g., VARCHAR(50))

#### pg_index mapping
- `indexrelid`: Generate OID for index
- `indrelid`: Table OID from pg_class
- `indisunique`: From PRAGMA index_list
- `indisprimary`: Detect from index name or type
- `indkey`: Column numbers from PRAGMA index_info

#### pg_constraint mapping
- Primary keys: From PRAGMA table_info (pk field)
- Foreign keys: From PRAGMA foreign_key_list
- Check constraints: Parse from sqlite_master.sql
- Unique constraints: From unique indexes