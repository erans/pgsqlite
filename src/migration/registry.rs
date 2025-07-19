use super::{Migration, MigrationAction};
use lazy_static::lazy_static;
use std::collections::BTreeMap;

lazy_static! {
    pub static ref MIGRATIONS: BTreeMap<u32, Migration> = {
        let mut registry = BTreeMap::new();
        
        // Register all migrations
        register_v1_initial_schema(&mut registry);
        register_v2_enum_support(&mut registry);
        register_v3_datetime_support(&mut registry);
        register_v4_datetime_integer_storage(&mut registry);
        register_v5_pg_catalog_tables(&mut registry);
        register_v6_varchar_constraints(&mut registry);
        register_v7_numeric_constraints(&mut registry);
        register_v8_array_support(&mut registry);
        
        registry
    };
}

/// Version 1: Initial schema
fn register_v1_initial_schema(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(1, Migration {
        version: 1,
        name: "initial_schema",
        description: "Create initial pgsqlite system tables",
        up: MigrationAction::Sql(r#"
            -- Core schema tracking (matching existing structure)
            CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                pg_type TEXT NOT NULL,
                sqlite_type TEXT NOT NULL,
                PRIMARY KEY (table_name, column_name)
            );
            
            -- System metadata
            CREATE TABLE IF NOT EXISTS __pgsqlite_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                updated_at REAL DEFAULT (strftime('%s', 'now'))
            );
            
            -- Migration tracking
            CREATE TABLE IF NOT EXISTS __pgsqlite_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                applied_at REAL NOT NULL,
                execution_time_ms INTEGER,
                checksum TEXT NOT NULL,
                status TEXT CHECK(status IN ('pending', 'running', 'completed', 'failed', 'rolled_back')),
                error_message TEXT,
                rolled_back_at REAL
            );
            
            -- Migration locks table (prevent concurrent migrations)
            CREATE TABLE IF NOT EXISTS __pgsqlite_migration_locks (
                id INTEGER PRIMARY KEY CHECK (id = 1),  -- Only one row allowed
                locked_by TEXT NOT NULL,  -- Process/connection identifier
                locked_at REAL NOT NULL,
                expires_at REAL NOT NULL  -- Timeout for stale locks
            );
            
            -- Set initial version
            INSERT INTO __pgsqlite_metadata (key, value) VALUES 
                ('schema_version', '1'),
                ('pgsqlite_version', '0.1.0');
        "#),
        down: None,  // Cannot rollback initial schema
        dependencies: vec![],
    });
}

/// Version 2: ENUM support (matching existing schema)
fn register_v2_enum_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(2, Migration {
        version: 2,
        name: "enum_type_support",
        description: "Add PostgreSQL ENUM type support",
        up: MigrationAction::SqlBatch(&[
            r#"
            -- Track ENUM type definitions
            CREATE TABLE IF NOT EXISTS __pgsqlite_enum_types (
                type_oid INTEGER PRIMARY KEY,
                type_name TEXT NOT NULL UNIQUE,
                namespace_oid INTEGER DEFAULT 2200, -- public schema
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            "#,
            r#"
            -- Track ENUM values with ordering
            CREATE TABLE IF NOT EXISTS __pgsqlite_enum_values (
                value_oid INTEGER PRIMARY KEY,
                type_oid INTEGER NOT NULL,
                label TEXT NOT NULL,
                sort_order REAL NOT NULL,
                FOREIGN KEY (type_oid) REFERENCES __pgsqlite_enum_types(type_oid),
                UNIQUE (type_oid, label)
            );
            "#,
            r#"
            -- Index for efficient lookups
            CREATE INDEX IF NOT EXISTS idx_enum_values_type ON __pgsqlite_enum_values(type_oid);
            CREATE INDEX IF NOT EXISTS idx_enum_values_label ON __pgsqlite_enum_values(type_oid, label);
            "#,
            r#"
            -- Track ENUM usage in tables
            CREATE TABLE IF NOT EXISTS __pgsqlite_enum_usage (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                enum_type TEXT NOT NULL,
                PRIMARY KEY (table_name, column_name),
                FOREIGN KEY (enum_type) REFERENCES __pgsqlite_enum_types(type_name) ON DELETE CASCADE
            );
            "#,
            r#"
            -- Update schema version
            UPDATE __pgsqlite_metadata 
            SET value = '2', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            DROP TABLE IF EXISTS __pgsqlite_enum_usage;
            DROP INDEX IF EXISTS idx_enum_values_label;
            DROP INDEX IF EXISTS idx_enum_values_type;
            DROP TABLE IF EXISTS __pgsqlite_enum_values;
            DROP TABLE IF EXISTS __pgsqlite_enum_types;
            UPDATE __pgsqlite_metadata 
            SET value = '1', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![1],
    });
}

/// Version 3: DateTime and Timezone support
fn register_v3_datetime_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(3, Migration {
        version: 3,
        name: "datetime_timezone_support",
        description: "Add datetime format and timezone metadata for PostgreSQL datetime types",
        up: MigrationAction::SqlBatch(&[
            r#"
            -- Add datetime format column to track which PostgreSQL datetime type is used
            ALTER TABLE __pgsqlite_schema ADD COLUMN datetime_format TEXT;
            "#,
            r#"
            -- Add timezone offset column for TIMETZ type (stores offset in seconds from UTC)
            ALTER TABLE __pgsqlite_schema ADD COLUMN timezone_offset INTEGER;
            "#,
            r#"
            -- Create datetime conversion cache table for performance
            CREATE TABLE IF NOT EXISTS __pgsqlite_datetime_cache (
                query_hash TEXT NOT NULL,
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                has_datetime BOOLEAN NOT NULL,
                datetime_columns TEXT,  -- JSON array of datetime column info
                PRIMARY KEY (query_hash, table_name, column_name)
            );
            "#,
            r#"
            -- Index for efficient cache lookups
            CREATE INDEX IF NOT EXISTS idx_datetime_cache_table 
            ON __pgsqlite_datetime_cache(table_name);
            "#,
            r#"
            -- Track session timezone settings
            CREATE TABLE IF NOT EXISTS __pgsqlite_session_settings (
                session_id TEXT PRIMARY KEY,
                timezone TEXT DEFAULT 'UTC',
                timezone_offset_seconds INTEGER DEFAULT 0,
                datestyle TEXT DEFAULT 'ISO, MDY',
                created_at REAL DEFAULT (strftime('%s', 'now')),
                updated_at REAL DEFAULT (strftime('%s', 'now'))
            );
            "#,
            r#"
            -- Update schema version
            UPDATE __pgsqlite_metadata 
            SET value = '3', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- Note: SQLite doesn't support DROP COLUMN in older versions
            -- We would need to recreate the table without the columns
            DROP TABLE IF EXISTS __pgsqlite_session_settings;
            DROP INDEX IF EXISTS idx_datetime_cache_table;
            DROP TABLE IF EXISTS __pgsqlite_datetime_cache;
            
            -- For __pgsqlite_schema, we'd need to recreate it without the new columns
            -- This is left as an exercise since downgrade is rarely needed
            
            UPDATE __pgsqlite_metadata 
            SET value = '2', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![2],
    });
}

/// Version 4: Convert datetime storage from REAL/TEXT to INTEGER microseconds
fn register_v4_datetime_integer_storage(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(4, Migration {
        version: 4,
        name: "datetime_integer_storage",
        description: "Convert all datetime types to INTEGER storage using microseconds",
        up: MigrationAction::SqlBatch(&[
            // Update type mappings in __pgsqlite_schema
            r#"
            UPDATE __pgsqlite_schema 
            SET sqlite_type = 'INTEGER'
            WHERE pg_type IN ('DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMPTZ', 
                              'date', 'time', 'timestamp', 'timestamptz',
                              'timestamp with time zone', 'timestamp without time zone',
                              'time with time zone', 'time without time zone',
                              'timetz', 'interval');
            "#,
            
            // Note: Data conversion would happen here in a real migration
            // Since we're not supporting backwards compatibility, existing databases
            // would need to be recreated or have their data converted separately
            
            r#"
            -- Update schema version
            UPDATE __pgsqlite_metadata 
            SET value = '4', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: None, // No backwards compatibility needed
        dependencies: vec![3],
    });
}

/// Version 5: PostgreSQL Catalog Tables
fn register_v5_pg_catalog_tables(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(5, Migration {
        version: 5,
        name: "pg_catalog_tables",
        description: "Create PostgreSQL-compatible catalog tables and views for psql compatibility",
        up: MigrationAction::Combined {
            pre_sql: Some(r#"
                -- pg_namespace view (schemas)
                CREATE VIEW IF NOT EXISTS pg_namespace AS
                SELECT 
                    11 as oid,
                    'pg_catalog' as nspname,
                    10 as nspowner,
                    NULL as nspacl
                UNION ALL
                SELECT 
                    2200 as oid,
                    'public' as nspname,
                    10 as nspowner,
                    NULL as nspacl;
                
                -- pg_am view (access methods)
                CREATE VIEW IF NOT EXISTS pg_am AS
                SELECT 
                    403 as oid,
                    'btree' as amname,
                    'i' as amtype;
                
                -- pg_type view (data types)
                CREATE VIEW IF NOT EXISTS pg_type AS
                SELECT 
                    oid,
                    typname,
                    typtype,
                    typelem,
                    typbasetype,
                    typnamespace
                FROM (
                    -- Basic types
                    SELECT 16 as oid, 'bool' as typname, 'b' as typtype, 0 as typelem, 0 as typbasetype, 11 as typnamespace
                    UNION ALL SELECT 17, 'bytea', 'b', 0, 0, 11
                    UNION ALL SELECT 20, 'int8', 'b', 0, 0, 11
                    UNION ALL SELECT 21, 'int2', 'b', 0, 0, 11
                    UNION ALL SELECT 23, 'int4', 'b', 0, 0, 11
                    UNION ALL SELECT 25, 'text', 'b', 0, 0, 11
                    UNION ALL SELECT 114, 'json', 'b', 0, 0, 11
                    UNION ALL SELECT 700, 'float4', 'b', 0, 0, 11
                    UNION ALL SELECT 701, 'float8', 'b', 0, 0, 11
                    UNION ALL SELECT 1042, 'char', 'b', 0, 0, 11
                    UNION ALL SELECT 1043, 'varchar', 'b', 0, 0, 11
                    UNION ALL SELECT 1082, 'date', 'b', 0, 0, 11
                    UNION ALL SELECT 1083, 'time', 'b', 0, 0, 11
                    UNION ALL SELECT 1114, 'timestamp', 'b', 0, 0, 11
                    UNION ALL SELECT 1184, 'timestamptz', 'b', 0, 0, 11
                    UNION ALL SELECT 1700, 'numeric', 'b', 0, 0, 11
                    UNION ALL SELECT 2950, 'uuid', 'b', 0, 0, 11
                    UNION ALL SELECT 3802, 'jsonb', 'b', 0, 0, 11
                );
                
                -- pg_attribute view (column information)
                CREATE VIEW IF NOT EXISTS pg_attribute AS
                SELECT 
                    CAST(oid_hash(m.name) AS TEXT) as attrelid,     -- table OID
                    p.cid + 1 as attnum,                             -- column number (1-based)
                    p.name as attname,                               -- column name
                    CASE 
                        WHEN p.type LIKE '%INT%' THEN 23            -- int4
                        WHEN p.type LIKE '%CHAR%' THEN 1043         -- varchar
                        WHEN p.type LIKE '%TEXT%' THEN 25           -- text
                        WHEN p.type LIKE '%REAL%' OR p.type LIKE '%FLOA%' OR p.type LIKE '%DOUB%' THEN 701  -- float8
                        WHEN p.type LIKE '%NUMERIC%' OR p.type LIKE '%DECIMAL%' THEN 1700  -- numeric
                        WHEN p.type LIKE '%DATE%' THEN 1082         -- date
                        WHEN p.type LIKE '%TIME%' THEN 1083         -- time
                        ELSE 25                                      -- default to text
                    END as atttypid,                                -- type OID
                    -1 as attstattarget,
                    -1 as attlen,
                    p.cid + 1 as attnum,
                    0 as attndims,
                    -1 as attcacheoff,
                    -1 as atttypmod,
                    'f' as attbyval,
                    's' as attstorage,
                    'p' as attalign,
                    CASE WHEN p."notnull" = 1 THEN 't' ELSE 'f' END as attnotnull,
                    'f' as atthasdef,
                    'f' as atthasmissing,
                    '' as attidentity,
                    '' as attgenerated,
                    'f' as attisdropped,
                    't' as attislocal,
                    0 as attinhcount,
                    0 as attcollation,
                    NULL as attacl,
                    NULL as attoptions,
                    NULL as attfdwoptions,
                    NULL as attmissingval
                FROM sqlite_master m
                JOIN pragma_table_info(m.name) p
                WHERE m.type = 'table'
                  AND m.name NOT LIKE 'sqlite_%'
                  AND m.name NOT LIKE '__pgsqlite_%';
                
                -- Enhanced pg_class view that works with JOINs
                CREATE VIEW IF NOT EXISTS pg_class AS
                SELECT 
                    -- Generate stable OID from table name using hash function
                    -- Cast to TEXT to handle both numeric and string comparisons
                    CAST(oid_hash(name) AS TEXT) as oid,
                    name as relname,
                    2200 as relnamespace,  -- public schema
                    CASE 
                        WHEN type = 'table' THEN 'r'
                        WHEN type = 'view' THEN 'v'
                        WHEN type = 'index' THEN 'i'
                    END as relkind,
                    10 as relowner,
                    CASE WHEN type = 'index' THEN 403 ELSE 0 END as relam,
                    0 as relfilenode,
                    0 as reltablespace,
                    0 as relpages,
                    -1 as reltuples,
                    0 as relallvisible,
                    0 as reltoastrelid,
                    CASE WHEN type = 'table' THEN 't' ELSE 'f' END as relhasindex,
                    'f' as relisshared,
                    'p' as relpersistence,
                    CAST(oid_hash(name || '_type') AS TEXT) as reltype,
                    0 as reloftype,
                    0 as relnatts,
                    0 as relchecks,
                    'f' as relhasrules,
                    'f' as relhastriggers,
                    'f' as relhassubclass,
                    'f' as relrowsecurity,
                    'f' as relforcerowsecurity,
                    't' as relispopulated,
                    'd' as relreplident,
                    'f' as relispartition,
                    0 as relrewrite,
                    0 as relfrozenxid,
                    0 as relminmxid,
                    NULL as relacl,
                    NULL as reloptions,
                    NULL as relpartbound
                FROM sqlite_master
                WHERE type IN ('table', 'view', 'index')
                  AND name NOT LIKE 'sqlite_%'
                  AND name NOT LIKE '__pgsqlite_%';
                
                -- pg_constraint table for constraints
                CREATE TABLE IF NOT EXISTS pg_constraint (
                    oid TEXT PRIMARY KEY,
                    conname TEXT NOT NULL,
                    connamespace INTEGER DEFAULT 2200,
                    contype CHAR(1) NOT NULL,  -- 'p' primary, 'u' unique, 'c' check, 'f' foreign
                    condeferrable BOOLEAN DEFAULT 0,
                    condeferred BOOLEAN DEFAULT 0,
                    convalidated BOOLEAN DEFAULT 1,
                    conrelid TEXT NOT NULL,  -- table OID
                    contypid INTEGER DEFAULT 0,
                    conindid INTEGER DEFAULT 0,  -- index OID for unique/primary
                    conparentid INTEGER DEFAULT 0,
                    confrelid INTEGER DEFAULT 0, -- referenced table for foreign keys
                    confupdtype CHAR(1) DEFAULT ' ',
                    confdeltype CHAR(1) DEFAULT ' ',
                    confmatchtype CHAR(1) DEFAULT ' ',
                    conislocal BOOLEAN DEFAULT 1,
                    coninhcount INTEGER DEFAULT 0,
                    connoinherit BOOLEAN DEFAULT 0,
                    conkey TEXT,    -- column numbers as comma-separated list
                    confkey TEXT,   -- referenced columns
                    conpfeqop TEXT,
                    conppeqop TEXT,
                    conffeqop TEXT,
                    conexclop TEXT,
                    conbin TEXT,    -- expression tree
                    consrc TEXT     -- human-readable
                );
                
                -- pg_attrdef table for column defaults
                CREATE TABLE IF NOT EXISTS pg_attrdef (
                    oid TEXT PRIMARY KEY,
                    adrelid TEXT NOT NULL,    -- table OID
                    adnum SMALLINT NOT NULL,     -- column number
                    adbin TEXT,                  -- expression tree
                    adsrc TEXT                   -- human-readable default
                );
                
                -- pg_index table for indexes
                CREATE TABLE IF NOT EXISTS pg_index (
                    indexrelid TEXT PRIMARY KEY,  -- index OID
                    indrelid TEXT NOT NULL,       -- table OID
                    indnatts SMALLINT NOT NULL,
                    indnkeyatts SMALLINT NOT NULL,
                    indisunique BOOLEAN DEFAULT 0,
                    indisprimary BOOLEAN DEFAULT 0,
                    indisexclusion BOOLEAN DEFAULT 0,
                    indimmediate BOOLEAN DEFAULT 1,
                    indisclustered BOOLEAN DEFAULT 0,
                    indisvalid BOOLEAN DEFAULT 1,
                    indcheckxmin BOOLEAN DEFAULT 0,
                    indisready BOOLEAN DEFAULT 1,
                    indislive BOOLEAN DEFAULT 1,
                    indisreplident BOOLEAN DEFAULT 0,
                    indkey TEXT,                     -- column numbers
                    indcollation TEXT,
                    indclass TEXT,
                    indoption TEXT,
                    indexprs TEXT,                   -- expression trees
                    indpred TEXT                     -- partial index predicate
                );
                
                -- Update schema version
                UPDATE __pgsqlite_metadata 
                SET value = '5', updated_at = strftime('%s', 'now')
                WHERE key = 'schema_version';
            "#),
            function: populate_catalog_tables,
            post_sql: None,
        },
        down: Some(MigrationAction::Sql(r#"
            DROP VIEW IF EXISTS pg_type;
            DROP VIEW IF EXISTS pg_attribute;
            DROP VIEW IF EXISTS pg_class;
            DROP VIEW IF EXISTS pg_am;
            DROP VIEW IF EXISTS pg_namespace;
            DROP TABLE IF EXISTS pg_index;
            DROP TABLE IF EXISTS pg_attrdef;
            DROP TABLE IF EXISTS pg_constraint;
            UPDATE __pgsqlite_metadata 
            SET value = '4', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![4],
    });
}

/// Populate catalog tables with metadata from sqlite_master
fn populate_catalog_tables(conn: &rusqlite::Connection) -> anyhow::Result<()> {
    use rusqlite::params;
    
    // Get all tables
    let mut stmt = conn.prepare("
        SELECT name, sql FROM sqlite_master 
        WHERE type = 'table' 
        AND name NOT LIKE 'sqlite_%'
        AND name NOT LIKE '__pgsqlite_%'
    ")?;
    
    let tables = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?.collect::<Result<Vec<_>, rusqlite::Error>>()?;
    
    for (table_name, create_sql) in tables {
        // Generate table OID (same as in pg_class view)
        let table_oid = generate_table_oid(&table_name);
        
        // Parse CREATE TABLE statement to extract constraints
        if let Some(constraints) = parse_table_constraints(&table_name, &create_sql) {
            for constraint in constraints {
                // Insert into pg_constraint
                conn.execute("
                    INSERT OR IGNORE INTO pg_constraint (
                        oid, conname, contype, conrelid, conkey, consrc
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ", params![
                    constraint.oid,
                    constraint.name,
                    constraint.contype,
                    table_oid,
                    constraint.columns.join(","),
                    constraint.definition
                ])?;
            }
        }
        
        // Parse column defaults
        if let Some(defaults) = parse_column_defaults(&table_name, &create_sql) {
            for default in defaults {
                conn.execute("
                    INSERT OR IGNORE INTO pg_attrdef (
                        oid, adrelid, adnum, adsrc
                    ) VALUES (?1, ?2, ?3, ?4)
                ", params![
                    default.oid,
                    table_oid,
                    default.column_num,
                    default.default_expr
                ])?;
            }
        }
    }
    
    // Populate pg_index from sqlite_master indexes
    let mut stmt = conn.prepare("
        SELECT name, tbl_name, sql FROM sqlite_master 
        WHERE type = 'index' 
        AND sql IS NOT NULL
    ")?;
    
    let indexes = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?
        ))
    })?.collect::<Result<Vec<_>, _>>()?;
    
    for (index_name, table_name, create_sql) in indexes {
        let index_oid = generate_table_oid(&index_name);
        let table_oid = generate_table_oid(&table_name);
        
        // Parse index info
        let is_unique = create_sql.to_uppercase().contains("UNIQUE");
        
        conn.execute("
            INSERT OR IGNORE INTO pg_index (
                indexrelid, indrelid, indnatts, indnkeyatts, 
                indisunique, indisprimary
            ) VALUES (?1, ?2, 1, 1, ?3, 0)
        ", params![
            index_oid,
            table_oid,
            is_unique as i32
        ])?;
    }
    
    Ok(())
}

// Helper functions for parsing and OID generation
fn generate_table_oid(name: &str) -> i32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    ((hasher.finish() & 0x7FFFFFFF) % 1000000 + 16384) as i32
}

struct ConstraintInfo {
    oid: i32,
    name: String,
    contype: String,
    columns: Vec<String>,
    definition: String,
}

fn parse_table_constraints(table_name: &str, create_sql: &str) -> Option<Vec<ConstraintInfo>> {
    use regex::Regex;
    
    let mut constraints = Vec::new();
    
    // Parse PRIMARY KEY constraints
    // Look for both inline PRIMARY KEY and table-level PRIMARY KEY
    if let Ok(pk_regex) = Regex::new(r"(?i)\b(\w+)\s+[^,\)]*\bPRIMARY\s+KEY\b") {
        for cap in pk_regex.captures_iter(create_sql) {
            if let Some(column_name) = cap.get(1) {
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&format!("{}_pkey", table_name)),
                    name: format!("{}_pkey", table_name),
                    contype: "p".to_string(),
                    columns: vec![column_name.as_str().to_string()],
                    definition: "PRIMARY KEY".to_string(),
                });
            }
        }
    }
    
    // Parse table-level PRIMARY KEY constraints
    if let Ok(table_pk_regex) = Regex::new(r"(?i)PRIMARY\s+KEY\s*\(\s*([^)]+)\s*\)") {
        for cap in table_pk_regex.captures_iter(create_sql) {
            if let Some(columns_str) = cap.get(1) {
                let columns: Vec<String> = columns_str.as_str()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&format!("{}_pkey", table_name)),
                    name: format!("{}_pkey", table_name),
                    contype: "p".to_string(),
                    columns,
                    definition: "PRIMARY KEY".to_string(),
                });
            }
        }
    }
    
    // Parse UNIQUE constraints
    if let Ok(unique_regex) = Regex::new(r"(?i)\b(\w+)\s+[^,\)]*\bUNIQUE\b") {
        for cap in unique_regex.captures_iter(create_sql) {
            if let Some(column_name) = cap.get(1) {
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&format!("{}_{}_key", table_name, column_name.as_str())),
                    name: format!("{}_{}_key", table_name, column_name.as_str()),
                    contype: "u".to_string(),
                    columns: vec![column_name.as_str().to_string()],
                    definition: "UNIQUE".to_string(),
                });
            }
        }
    }
    
    // Parse table-level UNIQUE constraints
    if let Ok(table_unique_regex) = Regex::new(r"(?i)UNIQUE\s*\(\s*([^)]+)\s*\)") {
        for cap in table_unique_regex.captures_iter(create_sql) {
            if let Some(columns_str) = cap.get(1) {
                let columns: Vec<String> = columns_str.as_str()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect();
                let constraint_name = format!("{}_{}_key", table_name, columns.join("_"));
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&constraint_name),
                    name: constraint_name,
                    contype: "u".to_string(),
                    columns,
                    definition: "UNIQUE".to_string(),
                });
            }
        }
    }
    
    // Parse CHECK constraints
    if let Ok(check_regex) = Regex::new(r"(?i)CHECK\s*\(\s*([^)]+)\s*\)") {
        for (i, cap) in check_regex.captures_iter(create_sql).enumerate() {
            if let Some(check_expr) = cap.get(1) {
                let constraint_name = format!("{}_check{}", table_name, i + 1);
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&constraint_name),
                    name: constraint_name,
                    contype: "c".to_string(),
                    columns: vec![], // CHECK constraints don't have specific columns
                    definition: format!("CHECK ({})", check_expr.as_str()),
                });
            }
        }
    }
    
    // Parse NOT NULL constraints (treated as check constraints in PostgreSQL)
    if let Ok(not_null_regex) = Regex::new(r"(?i)\b(\w+)\s+[^,\)]*\bNOT\s+NULL\b") {
        for cap in not_null_regex.captures_iter(create_sql) {
            if let Some(column_name) = cap.get(1) {
                let constraint_name = format!("{}_{}_not_null", table_name, column_name.as_str());
                constraints.push(ConstraintInfo {
                    oid: generate_table_oid(&constraint_name),
                    name: constraint_name,
                    contype: "c".to_string(),
                    columns: vec![column_name.as_str().to_string()],
                    definition: format!("{} IS NOT NULL", column_name.as_str()),
                });
            }
        }
    }
    
    if constraints.is_empty() {
        None
    } else {
        Some(constraints)
    }
}

struct DefaultInfo {
    oid: i32,
    column_num: i16,
    default_expr: String,
}

fn parse_column_defaults(table_name: &str, create_sql: &str) -> Option<Vec<DefaultInfo>> {
    use regex::Regex;
    
    let mut defaults = Vec::new();
    
    // Parse DEFAULT clauses - look for column definitions with DEFAULT
    if let Ok(default_regex) = Regex::new(r"(?i)\b(\w+)\s+[^,\)]*\bDEFAULT\s+([^,\)]+)") {
        for cap in default_regex.captures_iter(create_sql) {
            if let (Some(column_name), Some(default_value)) = (cap.get(1), cap.get(2)) {
                // Get column number by counting columns before this one
                let column_num = get_column_number(create_sql, column_name.as_str()).unwrap_or(1);
                
                defaults.push(DefaultInfo {
                    oid: generate_table_oid(&format!("{}_{}_default", table_name, column_name.as_str())),
                    column_num,
                    default_expr: default_value.as_str().trim().to_string(),
                });
            }
        }
    }
    
    if defaults.is_empty() {
        None
    } else {
        Some(defaults)
    }
}

/// Get the column number (1-based) for a given column name in a CREATE TABLE statement
fn get_column_number(create_sql: &str, target_column: &str) -> Option<i16> {
    use regex::Regex;
    
    // Extract the column definitions from CREATE TABLE
    if let Ok(table_regex) = Regex::new(r"(?i)CREATE\s+TABLE\s+[^(]+\(\s*(.+)\s*\)") {
        if let Some(cap) = table_regex.captures(create_sql) {
            if let Some(columns_part) = cap.get(1) {
                // Split by comma and look for our target column
                let columns_str = columns_part.as_str();
                let mut column_count = 0i16;
                
                // Simple column parsing - split by commas but be careful of nested parentheses
                let mut paren_depth = 0;
                let mut current_column = String::new();
                
                for ch in columns_str.chars() {
                    match ch {
                        '(' => {
                            paren_depth += 1;
                            current_column.push(ch);
                        }
                        ')' => {
                            paren_depth -= 1;
                            current_column.push(ch);
                        }
                        ',' if paren_depth == 0 => {
                            // End of column definition
                            column_count += 1;
                            if current_column.trim().starts_with(target_column) {
                                return Some(column_count);
                            }
                            current_column.clear();
                        }
                        _ => {
                            current_column.push(ch);
                        }
                    }
                }
                
                // Check the last column
                if !current_column.trim().is_empty() {
                    column_count += 1;
                    if current_column.trim().starts_with(target_column) {
                        return Some(column_count);
                    }
                }
            }
        }
    }
    
    None
}

/// Version 6: VARCHAR/CHAR length constraints
fn register_v6_varchar_constraints(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(6, Migration {
        version: 6,
        name: "varchar_constraints",
        description: "Add support for VARCHAR/CHAR length constraints",
        up: MigrationAction::SqlBatch(&[
            r#"
            -- Add type_modifier column to store length constraints
            ALTER TABLE __pgsqlite_schema ADD COLUMN type_modifier INTEGER;
            "#,
            r#"
            -- Create table to cache string constraints for performance
            CREATE TABLE IF NOT EXISTS __pgsqlite_string_constraints (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                max_length INTEGER NOT NULL,
                is_char_type BOOLEAN NOT NULL DEFAULT 0,  -- 1 for CHAR (needs padding), 0 for VARCHAR
                PRIMARY KEY (table_name, column_name)
            );
            "#,
            r#"
            -- Create index for fast constraint lookups
            CREATE INDEX IF NOT EXISTS idx_string_constraints_table 
            ON __pgsqlite_string_constraints(table_name);
            "#,
            r#"
            -- Update schema version
            UPDATE __pgsqlite_metadata 
            SET value = '6', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- Note: SQLite doesn't support DROP COLUMN in older versions
            -- We would need to recreate the table without the column
            DROP INDEX IF EXISTS idx_string_constraints_table;
            DROP TABLE IF EXISTS __pgsqlite_string_constraints;
            
            -- For __pgsqlite_schema, we'd need to recreate it without type_modifier
            -- This is left as an exercise since downgrade is rarely needed
            
            UPDATE __pgsqlite_metadata 
            SET value = '5', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![5],
    });
}

/// Version 7: NUMERIC/DECIMAL precision and scale constraints
fn register_v7_numeric_constraints(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(7, Migration {
        version: 7,
        name: "numeric_constraints",
        description: "Add support for NUMERIC/DECIMAL precision and scale constraints",
        up: MigrationAction::SqlBatch(&[
            // Create table for numeric constraints
            r#"
            CREATE TABLE IF NOT EXISTS __pgsqlite_numeric_constraints (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                precision INTEGER NOT NULL,
                scale INTEGER NOT NULL,
                PRIMARY KEY (table_name, column_name)
            );
            "#,
            
            // Create index for efficient lookups
            r#"
            CREATE INDEX IF NOT EXISTS idx_numeric_constraints_table 
            ON __pgsqlite_numeric_constraints(table_name);
            "#,
            
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata 
            SET value = '7', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            DROP INDEX IF EXISTS idx_numeric_constraints_table;
            DROP TABLE IF EXISTS __pgsqlite_numeric_constraints;
            
            UPDATE __pgsqlite_metadata 
            SET value = '6', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![6],
    });
}

/// Version 8: Array type support
fn register_v8_array_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(8, Migration {
        version: 8,
        name: "array_support",
        description: "Add support for PostgreSQL array types",
        up: MigrationAction::SqlBatch(&[
            // Create table for array type metadata
            r#"
            CREATE TABLE IF NOT EXISTS __pgsqlite_array_types (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                element_type TEXT NOT NULL,
                dimensions INTEGER DEFAULT 1,
                PRIMARY KEY (table_name, column_name)
            );
            "#,
            
            // Create index for efficient lookups
            r#"
            CREATE INDEX IF NOT EXISTS idx_array_types_table 
            ON __pgsqlite_array_types(table_name);
            "#,
            
            // Drop the old pg_type view
            r#"
            DROP VIEW IF EXISTS pg_type;
            "#,
            
            // Recreate pg_type view with typarray field
            r#"
            CREATE VIEW pg_type AS
            SELECT 
                oid,
                typname,
                typtype,
                typelem,
                typarray,
                typbasetype,
                typnamespace
            FROM (
                -- Basic types with their array types
                SELECT 16 as oid, 'bool' as typname, 'b' as typtype, 0 as typelem, 1000 as typarray, 0 as typbasetype, 11 as typnamespace
                UNION ALL SELECT 17, 'bytea', 'b', 0, 1001, 0, 11
                UNION ALL SELECT 20, 'int8', 'b', 0, 1016, 0, 11
                UNION ALL SELECT 21, 'int2', 'b', 0, 1005, 0, 11
                UNION ALL SELECT 23, 'int4', 'b', 0, 1007, 0, 11
                UNION ALL SELECT 25, 'text', 'b', 0, 1009, 0, 11
                UNION ALL SELECT 114, 'json', 'b', 0, 199, 0, 11
                UNION ALL SELECT 700, 'float4', 'b', 0, 1021, 0, 11
                UNION ALL SELECT 701, 'float8', 'b', 0, 1022, 0, 11
                UNION ALL SELECT 1042, 'char', 'b', 0, 1014, 0, 11
                UNION ALL SELECT 1043, 'varchar', 'b', 0, 1015, 0, 11
                UNION ALL SELECT 1082, 'date', 'b', 0, 1182, 0, 11
                UNION ALL SELECT 1083, 'time', 'b', 0, 1183, 0, 11
                UNION ALL SELECT 1114, 'timestamp', 'b', 0, 1115, 0, 11
                UNION ALL SELECT 1184, 'timestamptz', 'b', 0, 1185, 0, 11
                UNION ALL SELECT 1700, 'numeric', 'b', 0, 1231, 0, 11
                UNION ALL SELECT 2950, 'uuid', 'b', 0, 2951, 0, 11
                UNION ALL SELECT 3802, 'jsonb', 'b', 0, 3807, 0, 11
                -- Array types
                UNION ALL SELECT 1000, '_bool', 'b', 16, 0, 0, 11
                UNION ALL SELECT 1001, '_bytea', 'b', 17, 0, 0, 11
                UNION ALL SELECT 1005, '_int2', 'b', 21, 0, 0, 11
                UNION ALL SELECT 1007, '_int4', 'b', 23, 0, 0, 11
                UNION ALL SELECT 1009, '_text', 'b', 25, 0, 0, 11
                UNION ALL SELECT 1014, '_char', 'b', 1042, 0, 0, 11
                UNION ALL SELECT 1015, '_varchar', 'b', 1043, 0, 0, 11
                UNION ALL SELECT 1016, '_int8', 'b', 20, 0, 0, 11
                UNION ALL SELECT 1021, '_float4', 'b', 700, 0, 0, 11
                UNION ALL SELECT 1022, '_float8', 'b', 701, 0, 0, 11
                UNION ALL SELECT 1182, '_date', 'b', 1082, 0, 0, 11
                UNION ALL SELECT 1183, '_time', 'b', 1083, 0, 0, 11
                UNION ALL SELECT 1115, '_timestamp', 'b', 1114, 0, 0, 11
                UNION ALL SELECT 1185, '_timestamptz', 'b', 1184, 0, 0, 11
                UNION ALL SELECT 1231, '_numeric', 'b', 1700, 0, 0, 11
                UNION ALL SELECT 199, '_json', 'b', 114, 0, 0, 11
                UNION ALL SELECT 2951, '_uuid', 'b', 2950, 0, 0, 11
                UNION ALL SELECT 3807, '_jsonb', 'b', 3802, 0, 0, 11
            );
            "#,
            
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata 
            SET value = '8', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            DROP INDEX IF EXISTS idx_array_types_table;
            DROP TABLE IF EXISTS __pgsqlite_array_types;
            
            -- Restore original pg_type view without typarray
            DROP VIEW IF EXISTS pg_type;
            CREATE VIEW pg_type AS
            SELECT 
                oid,
                typname,
                typtype,
                typelem,
                typbasetype,
                typnamespace
            FROM (
                SELECT 16 as oid, 'bool' as typname, 'b' as typtype, 0 as typelem, 0 as typbasetype, 11 as typnamespace
                UNION ALL SELECT 17, 'bytea', 'b', 0, 0, 11
                UNION ALL SELECT 20, 'int8', 'b', 0, 0, 11
                UNION ALL SELECT 21, 'int2', 'b', 0, 0, 11
                UNION ALL SELECT 23, 'int4', 'b', 0, 0, 11
                UNION ALL SELECT 25, 'text', 'b', 0, 0, 11
                UNION ALL SELECT 114, 'json', 'b', 0, 0, 11
                UNION ALL SELECT 700, 'float4', 'b', 0, 0, 11
                UNION ALL SELECT 701, 'float8', 'b', 0, 0, 11
                UNION ALL SELECT 1042, 'char', 'b', 0, 0, 11
                UNION ALL SELECT 1043, 'varchar', 'b', 0, 0, 11
                UNION ALL SELECT 1082, 'date', 'b', 0, 0, 11
                UNION ALL SELECT 1083, 'time', 'b', 0, 0, 11
                UNION ALL SELECT 1114, 'timestamp', 'b', 0, 0, 11
                UNION ALL SELECT 1184, 'timestamptz', 'b', 0, 0, 11
                UNION ALL SELECT 1700, 'numeric', 'b', 0, 0, 11
                UNION ALL SELECT 2950, 'uuid', 'b', 0, 0, 11
                UNION ALL SELECT 3802, 'jsonb', 'b', 0, 0, 11
            );
            
            UPDATE __pgsqlite_metadata 
            SET value = '7', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![7],
    });
}