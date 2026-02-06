use super::{Migration, MigrationAction};
use std::collections::BTreeMap;

pub(super) fn register_v6_varchar_constraints(registry: &mut BTreeMap<u32, Migration>) {
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
pub(super) fn register_v7_numeric_constraints(registry: &mut BTreeMap<u32, Migration>) {
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
pub(super) fn register_v8_array_support(registry: &mut BTreeMap<u32, Migration>) {
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

/// Version 9: Full-Text Search support
pub(super) fn register_v9_fts_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(9, Migration {
        version: 9,
        name: "fts_support",
        description: "Add PostgreSQL Full-Text Search support using FTS5",
        up: MigrationAction::SqlBatch(&[
            // Create FTS metadata table
            r#"
            CREATE TABLE IF NOT EXISTS __pgsqlite_fts_metadata (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                fts_table_name TEXT NOT NULL,
                config_name TEXT NOT NULL DEFAULT 'english',
                tokenizer TEXT NOT NULL DEFAULT 'porter unicode61',
                stop_words TEXT,  -- JSON array
                PRIMARY KEY (table_name, column_name)
            );
            "#,
            
            // Add FTS columns to schema table
            r#"
            ALTER TABLE __pgsqlite_schema ADD COLUMN fts_table_name TEXT;
            "#,
            r#"
            ALTER TABLE __pgsqlite_schema ADD COLUMN fts_config TEXT DEFAULT 'english';
            "#,
            r#"
            ALTER TABLE __pgsqlite_schema ADD COLUMN fts_weights TEXT;  -- JSON mapping
            "#,
            
            // Create index for efficient FTS metadata lookups
            r#"
            CREATE INDEX IF NOT EXISTS idx_fts_metadata_table 
            ON __pgsqlite_fts_metadata(table_name);
            "#,
            
            // Create table for type map if it doesn't exist
            r#"
            CREATE TABLE IF NOT EXISTS __pgsqlite_type_map (
                pg_type TEXT PRIMARY KEY,
                sqlite_type TEXT NOT NULL,
                oid INTEGER NOT NULL UNIQUE
            );
            "#,
            
            // Register FTS types in type map
            r#"
            INSERT OR IGNORE INTO __pgsqlite_type_map (pg_type, sqlite_type, oid)
            VALUES 
                ('tsvector', 'TEXT', 3614),
                ('tsquery', 'TEXT', 3615),
                ('regconfig', 'TEXT', 3734);
            "#,
            
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata 
            SET value = '9', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            DROP INDEX IF EXISTS idx_fts_metadata_table;
            DROP TABLE IF EXISTS __pgsqlite_fts_metadata;
            
            -- Note: We can't easily remove columns from __pgsqlite_schema in SQLite
            -- Would need to recreate the table without the FTS columns
            
            -- Remove FTS types from type map
            DELETE FROM __pgsqlite_type_map 
            WHERE pg_type IN ('tsvector', 'tsquery', 'regconfig');
            
            UPDATE __pgsqlite_metadata 
            SET value = '8', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![8],
    });
}

/// Version 10: Add typcategory column to pg_type view
pub(super) fn register_v10_typcategory_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(10, Migration {
        version: 10,
        name: "typcategory_support",
        description: "Add typcategory column to pg_type view for PostgreSQL compatibility",
        up: MigrationAction::SqlBatch(&[
            // Drop the old pg_type view
            r#"
            DROP VIEW IF EXISTS pg_type;
            "#,
            
            // Recreate pg_type view with typcategory field
            r#"
            CREATE VIEW pg_type AS
            SELECT 
                oid,
                typname,
                typtype,
                typelem,
                typarray,
                typbasetype,
                typnamespace,
                typcategory
            FROM (
                -- Basic types with their array types and categories
                SELECT 16 as oid, 'bool' as typname, 'b' as typtype, 0 as typelem, 1000 as typarray, 0 as typbasetype, 11 as typnamespace, 'B' as typcategory
                UNION ALL SELECT 17, 'bytea', 'b', 0, 1001, 0, 11, 'U'
                UNION ALL SELECT 20, 'int8', 'b', 0, 1016, 0, 11, 'N'
                UNION ALL SELECT 21, 'int2', 'b', 0, 1005, 0, 11, 'N'
                UNION ALL SELECT 23, 'int4', 'b', 0, 1007, 0, 11, 'N'
                UNION ALL SELECT 25, 'text', 'b', 0, 1009, 0, 11, 'S'
                UNION ALL SELECT 114, 'json', 'b', 0, 199, 0, 11, 'U'
                UNION ALL SELECT 700, 'float4', 'b', 0, 1021, 0, 11, 'N'
                UNION ALL SELECT 701, 'float8', 'b', 0, 1022, 0, 11, 'N'
                UNION ALL SELECT 1042, 'char', 'b', 0, 1014, 0, 11, 'S'
                UNION ALL SELECT 1043, 'varchar', 'b', 0, 1015, 0, 11, 'S'
                UNION ALL SELECT 1082, 'date', 'b', 0, 1182, 0, 11, 'D'
                UNION ALL SELECT 1083, 'time', 'b', 0, 1183, 0, 11, 'D'
                UNION ALL SELECT 1114, 'timestamp', 'b', 0, 1115, 0, 11, 'D'
                UNION ALL SELECT 1184, 'timestamptz', 'b', 0, 1185, 0, 11, 'D'
                UNION ALL SELECT 1186, 'interval', 'b', 0, 1187, 0, 11, 'T'
                UNION ALL SELECT 1266, 'timetz', 'b', 0, 1270, 0, 11, 'D'
                UNION ALL SELECT 1560, 'bit', 'b', 0, 1561, 0, 11, 'V'
                UNION ALL SELECT 1562, 'varbit', 'b', 0, 1563, 0, 11, 'V'
                UNION ALL SELECT 1700, 'numeric', 'b', 0, 1231, 0, 11, 'N'
                UNION ALL SELECT 2950, 'uuid', 'b', 0, 2951, 0, 11, 'U'
                UNION ALL SELECT 3614, 'tsvector', 'b', 0, 3643, 0, 11, 'U'
                UNION ALL SELECT 3615, 'tsquery', 'b', 0, 3645, 0, 11, 'U'
                UNION ALL SELECT 3734, 'regconfig', 'b', 0, 3735, 0, 11, 'U'
                UNION ALL SELECT 3802, 'jsonb', 'b', 0, 3807, 0, 11, 'U'
                -- Array types (all have category 'A')
                UNION ALL SELECT 1000, '_bool', 'b', 16, 0, 0, 11, 'A'
                UNION ALL SELECT 1001, '_bytea', 'b', 17, 0, 0, 11, 'A'
                UNION ALL SELECT 1005, '_int2', 'b', 21, 0, 0, 11, 'A'
                UNION ALL SELECT 1007, '_int4', 'b', 23, 0, 0, 11, 'A'
                UNION ALL SELECT 1009, '_text', 'b', 25, 0, 0, 11, 'A'
                UNION ALL SELECT 1014, '_char', 'b', 1042, 0, 0, 11, 'A'
                UNION ALL SELECT 1015, '_varchar', 'b', 1043, 0, 0, 11, 'A'
                UNION ALL SELECT 1016, '_int8', 'b', 20, 0, 0, 11, 'A'
                UNION ALL SELECT 1021, '_float4', 'b', 700, 0, 0, 11, 'A'
                UNION ALL SELECT 1022, '_float8', 'b', 701, 0, 0, 11, 'A'
                UNION ALL SELECT 1115, '_timestamp', 'b', 1114, 0, 0, 11, 'A'
                UNION ALL SELECT 1182, '_date', 'b', 1082, 0, 0, 11, 'A'
                UNION ALL SELECT 1183, '_time', 'b', 1083, 0, 0, 11, 'A'
                UNION ALL SELECT 1185, '_timestamptz', 'b', 1184, 0, 0, 11, 'A'
                UNION ALL SELECT 1187, '_interval', 'b', 1186, 0, 0, 11, 'A'
                UNION ALL SELECT 1231, '_numeric', 'b', 1700, 0, 0, 11, 'A'
                UNION ALL SELECT 1270, '_timetz', 'b', 1266, 0, 0, 11, 'A'
                UNION ALL SELECT 1561, '_bit', 'b', 1560, 0, 0, 11, 'A'
                UNION ALL SELECT 1563, '_varbit', 'b', 1562, 0, 0, 11, 'A'
                UNION ALL SELECT 2951, '_uuid', 'b', 2950, 0, 0, 11, 'A'
                UNION ALL SELECT 3643, '_tsvector', 'b', 3614, 0, 0, 11, 'A'
                UNION ALL SELECT 3645, '_tsquery', 'b', 3615, 0, 0, 11, 'A'
                UNION ALL SELECT 3735, '_regconfig', 'b', 3734, 0, 0, 11, 'A'
                UNION ALL SELECT 3807, '_jsonb', 'b', 3802, 0, 0, 11, 'A'
                UNION ALL SELECT 199, '_json', 'b', 114, 0, 0, 11, 'A'
                -- ENUM types from __pgsqlite_enum_types (category 'E')
                UNION ALL
                SELECT 
                    e.type_oid as oid,
                    e.type_name as typname,
                    'e' as typtype,
                    0 as typelem,
                    0 as typarray,  -- ENUMs don't have array types in our schema
                    0 as typbasetype,
                    e.namespace_oid as typnamespace,
                    'E' as typcategory
                FROM __pgsqlite_enum_types e
            );
            "#,
            
            // Create pg_enum view for ENUM values
            r#"
            CREATE VIEW IF NOT EXISTS pg_enum AS
            SELECT 
                v.type_oid as enumtypid,
                v.sort_order as enumsortorder,
                v.label as enumlabel
            FROM __pgsqlite_enum_values v;
            "#,
            
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata 
            SET value = '10', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- Drop pg_enum view
            DROP VIEW IF EXISTS pg_enum;
            
            -- Restore v8 pg_type view without typcategory
            DROP VIEW IF EXISTS pg_type;
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
                UNION ALL SELECT 1186, 'interval', 'b', 0, 1187, 0, 11
                UNION ALL SELECT 1266, 'timetz', 'b', 0, 1270, 0, 11
                UNION ALL SELECT 1560, 'bit', 'b', 0, 1561, 0, 11
                UNION ALL SELECT 1562, 'varbit', 'b', 0, 1563, 0, 11
                UNION ALL SELECT 1700, 'numeric', 'b', 0, 1231, 0, 11
                UNION ALL SELECT 2950, 'uuid', 'b', 0, 2951, 0, 11
                UNION ALL SELECT 3614, 'tsvector', 'b', 0, 3643, 0, 11
                UNION ALL SELECT 3615, 'tsquery', 'b', 0, 3645, 0, 11
                UNION ALL SELECT 3734, 'regconfig', 'b', 0, 3735, 0, 11
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
                UNION ALL SELECT 1115, '_timestamp', 'b', 1114, 0, 0, 11
                UNION ALL SELECT 1182, '_date', 'b', 1082, 0, 0, 11
                UNION ALL SELECT 1183, '_time', 'b', 1083, 0, 0, 11
                UNION ALL SELECT 1185, '_timestamptz', 'b', 1184, 0, 0, 11
                UNION ALL SELECT 1187, '_interval', 'b', 1186, 0, 0, 11
                UNION ALL SELECT 1231, '_numeric', 'b', 1700, 0, 0, 11
                UNION ALL SELECT 1270, '_timetz', 'b', 1266, 0, 0, 11
                UNION ALL SELECT 1561, '_bit', 'b', 1560, 0, 0, 11
                UNION ALL SELECT 1563, '_varbit', 'b', 1562, 0, 0, 11
                UNION ALL SELECT 2951, '_uuid', 'b', 2950, 0, 0, 11
                UNION ALL SELECT 3643, '_tsvector', 'b', 3614, 0, 0, 11
                UNION ALL SELECT 3645, '_tsquery', 'b', 3615, 0, 0, 11
                UNION ALL SELECT 3735, '_regconfig', 'b', 3734, 0, 0, 11
                UNION ALL SELECT 3807, '_jsonb', 'b', 3802, 0, 0, 11
                UNION ALL SELECT 199, '_json', 'b', 114, 0, 0, 11
                -- ENUM types from __pgsqlite_enum_types
                UNION ALL
                SELECT 
                    e.type_oid as oid,
                    e.type_name as typname,
                    'e' as typtype,
                    0 as typelem,
                    0 as typarray,  -- ENUMs don't have array types in our schema
                    0 as typbasetype,
                    e.namespace_oid as typnamespace
                FROM __pgsqlite_enum_types e
            );
            
            UPDATE __pgsqlite_metadata 
            SET value = '9', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![9],
    });
}

/// Version 11: Fix catalog views to not use oid_hash function
pub(super) fn register_v11_fix_catalog_views(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(11, Migration {
        version: 11,
        name: "fix_catalog_views",
        description: "Replace oid_hash function with built-in SQLite functions in catalog views",
        up: MigrationAction::SqlBatch(&[
            // Drop existing views
            "DROP VIEW IF EXISTS pg_attribute;",
            "DROP VIEW IF EXISTS pg_class;",
            
            // Recreate pg_class view with built-in functions
            r#"
            CREATE VIEW IF NOT EXISTS pg_class AS
            SELECT 
                -- Generate stable OID from table name using SQLite's built-in functions
                -- Use a deterministic formula based on the table name's character codes
                -- Cast to TEXT to handle both numeric and string comparisons
                CAST(
                    (
                        (unicode(substr(name, 1, 1)) * 1000000) +
                        (unicode(substr(name || ' ', 2, 1)) * 10000) +
                        (unicode(substr(name || '  ', 3, 1)) * 100) +
                        (length(name) * 7)
                    ) % 1000000 + 16384
                AS TEXT) as oid,
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
                -- Generate type OID using a different formula to avoid collisions
                CAST(
                    (
                        (unicode(substr(name || '_type', 1, 1)) * 1000000) +
                        (unicode(substr(name || '_type' || ' ', 2, 1)) * 10000) +
                        (unicode(substr(name || '_type' || '  ', 3, 1)) * 100) +
                        (length(name || '_type') * 7)
                    ) % 1000000 + 16384
                AS TEXT) as reltype,
                0 as reloftype,
                0 as relnatts,
                0 as relchecks,
                'f' as relhasrules,
                'f' as relhastriggers,
                'f' as relhassubclass,
                'f' as relrowsecurity,
                'f' as relforcerowsecurity,
                't' as relispopulated,
                'p' as relreplident,
                't' as relispartition,
                0 as relrewrite,
                0 as relfrozenxid,
                '{}' as relminmxid,
                '' as relacl,
                '' as reloptions,
                '' as relpartbound
            FROM sqlite_master
            WHERE type IN ('table', 'view', 'index')
              AND name NOT LIKE 'sqlite_%'
              AND name NOT LIKE '__pgsqlite_%';
            "#,
            
            // Recreate pg_attribute view with built-in functions
            r#"
            CREATE VIEW IF NOT EXISTS pg_attribute AS
            SELECT 
                -- Use same formula as pg_class to ensure consistent OIDs
                CAST(
                    (
                        (unicode(substr(m.name, 1, 1)) * 1000000) +
                        (unicode(substr(m.name || ' ', 2, 1)) * 10000) +
                        (unicode(substr(m.name || '  ', 3, 1)) * 100) +
                        (length(m.name) * 7)
                    ) % 1000000 + 16384
                AS TEXT) as attrelid,     -- table OID
                p.cid + 1 as attnum,                             -- column number (1-based)
                p.name as attname,                               -- column name
                CASE 
                    WHEN p.type LIKE '%INT%' THEN 23            -- int4
                    WHEN p.type = 'TEXT' THEN 25                -- text
                    WHEN p.type = 'REAL' THEN 700               -- float4
                    WHEN p.type = 'BLOB' THEN 17                -- bytea
                    WHEN p.type LIKE '%CHAR%' THEN 1043         -- varchar
                    WHEN p.type = 'BOOLEAN' THEN 16             -- bool
                    WHEN p.type = 'DATE' THEN 1082              -- date
                    WHEN p.type LIKE 'TIME%' THEN 1083          -- time
                    WHEN p.type LIKE 'TIMESTAMP%' THEN 1114     -- timestamp
                    ELSE 25                                      -- default to text
                END as atttypid,
                -1 as attstattarget,
                0 as attlen,
                0 as attndims,
                -1 as attcacheoff,
                CASE WHEN p.type LIKE '%NOT NULL%' THEN 't' ELSE 'f' END as attnotnull,
                'f' as atthasdef,
                'f' as atthasmissing,
                '' as attidentity,
                '' as attgenerated,
                't' as attisdropped,
                't' as attislocal,
                0 as attinhcount,
                0 as attcollation,
                '' as attacl,
                '' as attoptions,
                '' as attfdwoptions,
                '' as attmissingval
            FROM pragma_table_info(m.name) p
            JOIN sqlite_master m ON m.type = 'table'
            WHERE m.type = 'table'
              AND m.name NOT LIKE 'sqlite_%'
              AND m.name NOT LIKE '__pgsqlite_%';
            "#,
            
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata 
            SET value = '11', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            // Drop new views
            "DROP VIEW IF EXISTS pg_attribute;",
            "DROP VIEW IF EXISTS pg_class;",
            
            // Restore old views with oid_hash (note: this won't work without the function)
            r#"
            CREATE VIEW IF NOT EXISTS pg_class AS
            SELECT 
                CAST(oid_hash(name) AS TEXT) as oid,
                name as relname,
                2200 as relnamespace,
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
                'p' as relreplident,
                't' as relispartition,
                0 as relrewrite,
                0 as relfrozenxid,
                '{}' as relminmxid,
                '' as relacl,
                '' as reloptions,
                '' as relpartbound
            FROM sqlite_master
            WHERE type IN ('table', 'view', 'index')
              AND name NOT LIKE 'sqlite_%'
              AND name NOT LIKE '__pgsqlite_%';
            "#,
            
            r#"
            CREATE VIEW IF NOT EXISTS pg_attribute AS
            SELECT 
                CAST(
                    (
                        (unicode(substr(m.name, 1, 1)) * 1000000) +
                        (unicode(substr(m.name || ' ', 2, 1)) * 10000) +
                        (unicode(substr(m.name || '  ', 3, 1)) * 100) +
                        (length(m.name) * 7)
                    ) % 1000000 + 16384
                AS TEXT) as attrelid,
                p.cid + 1 as attnum,
                p.name as attname,
                CASE 
                    WHEN p.type LIKE '%INT%' THEN 23
                    WHEN p.type = 'TEXT' THEN 25
                    WHEN p.type = 'REAL' THEN 700
                    WHEN p.type = 'BLOB' THEN 17
                    WHEN p.type LIKE '%CHAR%' THEN 1043
                    WHEN p.type = 'BOOLEAN' THEN 16
                    WHEN p.type = 'DATE' THEN 1082
                    WHEN p.type LIKE 'TIME%' THEN 1083
                    WHEN p.type LIKE 'TIMESTAMP%' THEN 1114
                    ELSE 25
                END as atttypid,
                -1 as attstattarget,
                0 as attlen,
                0 as attndims,
                -1 as attcacheoff,
                CASE WHEN p.type LIKE '%NOT NULL%' THEN 't' ELSE 'f' END as attnotnull,
                'f' as atthasdef,
                'f' as atthasmissing,
                '' as attidentity,
                '' as attgenerated,
                't' as attisdropped,
                't' as attislocal,
                0 as attinhcount,
                0 as attcollation,
                '' as attacl,
                '' as attoptions,
                '' as attfdwoptions,
                '' as attmissingval
            FROM pragma_table_info(m.name) p
            JOIN sqlite_master m ON m.type = 'table'
            WHERE m.type = 'table'
              AND m.name NOT LIKE 'sqlite_%'
              AND m.name NOT LIKE '__pgsqlite_%';
            "#,
            
            // Restore version
            r#"
            UPDATE __pgsqlite_metadata 
            SET value = '10', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![10],
    });
}

/// Version 12: Comment system
pub(super) fn register_v12_comment_system(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(12, Migration {
        version: 12,
        name: "comment_system",
        description: "Add support for PostgreSQL-style comments on database objects",
        up: MigrationAction::Sql(r#"
            -- Comments table to store object comments
            CREATE TABLE IF NOT EXISTS __pgsqlite_comments (
                object_oid INTEGER NOT NULL,        -- OID of the commented object
                catalog_name TEXT NOT NULL,         -- 'pg_class', 'pg_proc', 'pg_type', etc.
                subobject_id INTEGER DEFAULT 0,     -- Column number for column comments, 0 for others
                comment_text TEXT,                  -- The actual comment (NULL = remove comment)
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (object_oid, catalog_name, subobject_id)
            );

            -- Index for fast lookups
            CREATE INDEX IF NOT EXISTS idx_comments_lookup 
            ON __pgsqlite_comments(object_oid, catalog_name, subobject_id);
            
            -- Update schema version
            UPDATE __pgsqlite_metadata 
            SET value = '12', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#),
        down: Some(MigrationAction::Sql(r#"
            -- Remove comments table
            DROP TABLE IF EXISTS __pgsqlite_comments;
            DROP INDEX IF EXISTS idx_comments_lookup;
            
            -- Restore schema version
            UPDATE __pgsqlite_metadata 
            SET value = '11', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![11],
    });
}

/// Version 13: PostgreSQL statistics and system views
pub(super) fn register_v13_pg_stat_views(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(13, Migration {
        version: 13,
        name: "pg_stat_views",
        description: "Add minimal PostgreSQL statistics views, pg_database, and pg_foreign_data_wrapper for compatibility",
        up: MigrationAction::Sql(r#"
            -- Create pg_stat_activity view with minimal but essential columns
            CREATE VIEW IF NOT EXISTS pg_stat_activity AS
            SELECT
                1 as datid,                                            -- Database OID
                'main' as datname,                                     -- Database name (SQLite default)
                1 as pid,                                              -- Process ID (static in SQLite)
                NULL as leader_pid,                                    -- Parallel leader PID (not applicable)
                10 as usesysid,                                        -- User OID (default owner)
                'postgres' as usename,                                 -- Username (default)
                'pgsqlite' as application_name,                        -- Application name
                NULL as client_addr,                                   -- Client address (local)
                NULL as client_hostname,                               -- Client hostname
                NULL as client_port,                                   -- Client port
                datetime('now') as backend_start,                      -- Backend start time
                NULL as xact_start,                                    -- Transaction start
                NULL as query_start,                                   -- Query start
                datetime('now') as state_change,                       -- Last state change
                NULL as wait_event_type,                               -- Wait event type
                NULL as wait_event,                                    -- Wait event name
                'idle' as state,                                       -- Current state
                NULL as backend_xid,                                   -- Transaction ID
                NULL as backend_xmin,                                  -- Transaction min ID
                NULL as query_id,                                      -- Query identifier
                '<IDLE>' as query,                                     -- Current query
                'client backend' as backend_type;                      -- Backend type

            -- Create pg_stat_database view with database-wide statistics
            CREATE VIEW IF NOT EXISTS pg_stat_database AS
            SELECT
                1 as datid,                                            -- Database OID
                'main' as datname,                                     -- Database name
                1 as numbackends,                                      -- Number of backends
                0 as xact_commit,                                      -- Committed transactions
                0 as xact_rollback,                                    -- Rolled back transactions
                0 as blks_read,                                        -- Blocks read
                0 as blks_hit,                                         -- Blocks hit
                0 as tup_returned,                                     -- Tuples returned
                0 as tup_fetched,                                      -- Tuples fetched
                0 as tup_inserted,                                     -- Tuples inserted
                0 as tup_updated,                                      -- Tuples updated
                0 as tup_deleted,                                      -- Tuples deleted
                0 as conflicts,                                        -- Conflicts
                0 as temp_files,                                       -- Temp files
                0 as temp_bytes,                                       -- Temp bytes
                0 as deadlocks,                                        -- Deadlocks
                0 as checksum_failures,                                -- Checksum failures
                NULL as checksum_last_failure,                         -- Last checksum failure
                0 as blk_read_time,                                    -- Block read time
                0 as blk_write_time,                                   -- Block write time
                NULL as session_time,                                  -- Session time
                NULL as active_time,                                   -- Active time
                NULL as idle_in_transaction_time,                      -- Idle in transaction time
                0 as sessions,                                         -- Sessions
                0 as sessions_abandoned,                               -- Abandoned sessions
                0 as sessions_fatal,                                   -- Fatal sessions
                0 as sessions_killed,                                  -- Killed sessions
                datetime('now') as stats_reset;                        -- Stats reset time

            -- Create pg_stat_user_tables view with table access statistics
            CREATE VIEW IF NOT EXISTS pg_stat_user_tables AS
            SELECT
                CAST(
                    (
                        (unicode(substr(name, 1, 1)) * 1000000) +
                        (unicode(substr(name || ' ', 2, 1)) * 10000) +
                        (unicode(substr(name || '  ', 3, 1)) * 100) +
                        (length(name) * 7)
                    ) % 1000000 + 16384
                AS TEXT) as relid,                 -- Table OID
                'public' as schemaname,                                -- Schema name
                name as relname,                                       -- Table name
                0 as seq_scan,                                         -- Sequential scans
                NULL as last_seq_scan,                                 -- Last sequential scan
                0 as seq_tup_read,                                     -- Sequential tuples read
                0 as idx_scan,                                         -- Index scans
                NULL as last_idx_scan,                                 -- Last index scan
                0 as idx_tup_fetch,                                    -- Index tuples fetched
                0 as n_tup_ins,                                        -- Tuples inserted
                0 as n_tup_upd,                                        -- Tuples updated
                0 as n_tup_del,                                        -- Tuples deleted
                0 as n_tup_hot_upd,                                    -- Hot updated tuples
                0 as n_tup_newpage_upd,                                -- New page updated tuples
                0 as n_live_tup,                                       -- Live tuples
                0 as n_dead_tup,                                       -- Dead tuples
                0 as n_mod_since_analyze,                              -- Modified since analyze
                0 as n_ins_since_vacuum,                               -- Inserts since vacuum
                NULL as last_vacuum,                                   -- Last vacuum
                NULL as last_autovacuum,                               -- Last autovacuum
                NULL as last_analyze,                                  -- Last analyze
                NULL as last_autoanalyze,                              -- Last autoanalyze
                0 as vacuum_count,                                     -- Vacuum count
                0 as autovacuum_count,                                 -- Autovacuum count
                0 as analyze_count,                                    -- Analyze count
                0 as autoanalyze_count                                 -- Autoanalyze count
            FROM sqlite_master
            WHERE type = 'table'
            AND name NOT LIKE '__pgsqlite_%'
            AND name NOT LIKE 'sqlite_%';

            -- Create pg_database view with database catalog information
            CREATE VIEW IF NOT EXISTS pg_database AS
            SELECT
                1 as oid,                                              -- Database OID
                'main' as datname,                                     -- Database name
                10 as datdba,                                          -- Database owner OID
                6 as encoding,                                         -- Encoding (UTF8)
                'c' as datlocprovider,                                 -- Locale provider
                false as datistemplate,                                -- Is template
                true as datallowconn,                                  -- Allow connections
                false as dathasloginevt,                               -- Has login events
                -1 as datconnlimit,                                    -- Connection limit
                1 as datfrozenxid,                                     -- Frozen transaction ID
                1 as datminmxid,                                       -- Minimum multixact ID
                1663 as dattablespace,                                 -- Default tablespace OID
                'en_US.UTF-8' as datcollate,                           -- Collation
                'en_US.UTF-8' as datctype,                             -- Character type
                'en_US.UTF-8' as datlocale,                            -- Locale
                NULL as daticurules,                                   -- ICU rules
                NULL as datcollversion,                                -- Collation version
                NULL as datacl;                                        -- Access control list

            -- Create pg_foreign_data_wrapper view (empty but compatible)
            CREATE VIEW IF NOT EXISTS pg_foreign_data_wrapper AS
            SELECT
                NULL as oid,                                           -- FDW OID
                NULL as fdwname,                                       -- FDW name
                NULL as fdwowner,                                      -- FDW owner OID
                NULL as fdwhandler,                                    -- Handler function OID
                NULL as fdwvalidator,                                  -- Validator function OID
                NULL as fdwacl,                                        -- Access control list
                NULL as fdwoptions                                     -- FDW options
            WHERE 0 = 1;  -- Always empty (no FDWs in SQLite)

            -- Additional statistics views commonly queried
            CREATE VIEW IF NOT EXISTS pg_stat_all_tables AS
            SELECT * FROM pg_stat_user_tables;

            CREATE VIEW IF NOT EXISTS pg_stat_user_indexes AS
            SELECT
                NULL as relid,                                         -- Table OID
                NULL as indexrelid,                                    -- Index OID
                'public' as schemaname,                                -- Schema name
                NULL as relname,                                       -- Table name
                NULL as indexrelname,                                  -- Index name
                0 as idx_scan,                                         -- Index scans
                0 as idx_tup_read,                                     -- Index tuples read
                0 as idx_tup_fetch                                     -- Index tuples fetched
            WHERE 0 = 1;  -- Empty view

            CREATE VIEW IF NOT EXISTS pg_stat_all_indexes AS
            SELECT * FROM pg_stat_user_indexes;

            -- Update schema version
            UPDATE __pgsqlite_metadata
            SET value = '13', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#),
        down: Some(MigrationAction::Sql(r#"
            -- Remove all the statistics views
            DROP VIEW IF EXISTS pg_stat_activity;
            DROP VIEW IF EXISTS pg_stat_database;
            DROP VIEW IF EXISTS pg_stat_user_tables;
            DROP VIEW IF EXISTS pg_stat_all_tables;
            DROP VIEW IF EXISTS pg_stat_user_indexes;
            DROP VIEW IF EXISTS pg_stat_all_indexes;
            DROP VIEW IF EXISTS pg_database;
            DROP VIEW IF EXISTS pg_foreign_data_wrapper;

            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '12', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![12],
    });
}

/// Version 14: Information schema views as real SQLite views
pub(super) fn register_v14_information_schema_views(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(14, Migration {
        version: 14,
        name: "information_schema_views",
        description: "Create information_schema views as real SQLite views to enable JOINs for ORM compatibility",
        up: MigrationAction::SqlBatch(&[
            // Create information_schema views as real SQLite views with underscores
            // These can be JOINed and the interceptor will query them
            // Use existing pg_* catalog tables for consistency
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_tables AS
            SELECT
                'main' as table_catalog,
                'public' as table_schema,
                relname as table_name,
                CASE relkind
                    WHEN 'r' THEN 'BASE TABLE'
                    WHEN 'v' THEN 'VIEW'
                    ELSE 'UNKNOWN'
                END as table_type,
                'YES' as is_insertable_into,
                NULL as self_referencing_column_name,
                NULL as reference_generation,
                NULL as user_defined_type_catalog,
                NULL as user_defined_type_schema,
                NULL as user_defined_type_name,
                'NO' as is_typed,
                'NO' as commit_action
            FROM pg_class
            WHERE relkind IN ('r', 'v');
            "#,

            // Create information_schema.columns view
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_columns AS
            SELECT
                'main' as table_catalog,
                'public' as table_schema,
                c.relname as table_name,
                a.attname as column_name,
                a.attnum as ordinal_position,
                NULL as column_default,
                CASE WHEN a.attnotnull = 't' THEN 'NO' ELSE 'YES' END as is_nullable,
                CASE a.atttypid
                    WHEN 23 THEN 'integer'
                    WHEN 25 THEN 'text'
                    WHEN 700 THEN 'real'
                    WHEN 701 THEN 'double precision'
                    WHEN 17 THEN 'bytea'
                    WHEN 1043 THEN 'character varying'
                    WHEN 1042 THEN 'character'
                    WHEN 16 THEN 'boolean'
                    WHEN 1082 THEN 'date'
                    WHEN 1083 THEN 'time without time zone'
                    WHEN 1114 THEN 'timestamp without time zone'
                    WHEN 1184 THEN 'timestamp with time zone'
                    WHEN 1700 THEN 'numeric'
                    ELSE 'text'
                END as data_type,
                NULL as character_maximum_length,
                NULL as character_octet_length,
                NULL as numeric_precision,
                NULL as numeric_precision_radix,
                NULL as numeric_scale,
                NULL as datetime_precision,
                NULL as interval_type,
                NULL as interval_precision,
                NULL as character_set_catalog,
                NULL as character_set_schema,
                NULL as character_set_name,
                NULL as collation_catalog,
                NULL as collation_schema,
                NULL as collation_name,
                NULL as domain_catalog,
                NULL as domain_schema,
                NULL as domain_name,
                NULL as udt_catalog,
                NULL as udt_schema,
                NULL as udt_name,
                NULL as scope_catalog,
                NULL as scope_schema,
                NULL as scope_name,
                NULL as maximum_cardinality,
                NULL as dtd_identifier,
                'NO' as is_self_referencing,
                'NO' as is_identity,
                NULL as identity_generation,
                NULL as identity_start,
                NULL as identity_increment,
                NULL as identity_maximum,
                NULL as identity_minimum,
                NULL as identity_cycle,
                'NO' as is_generated,
                NULL as generation_expression,
                'NO' as is_updatable
            FROM pg_class c
            JOIN pg_attribute a ON c.oid = a.attrelid
            WHERE c.relkind = 'r'
              AND a.attnum > 0;
            "#,

            // Create information_schema.key_column_usage view
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_key_column_usage AS
            SELECT
                'main' as constraint_catalog,
                'public' as constraint_schema,
                con.conname as constraint_name,
                'main' as table_catalog,
                'public' as table_schema,
                c.relname as table_name,
                a.attname as column_name,
                a.attnum as ordinal_position,
                NULL as position_in_unique_constraint
            FROM pg_constraint con
            JOIN pg_class c ON con.conrelid = c.oid
            JOIN pg_attribute a ON c.oid = a.attrelid
            WHERE con.contype IN ('p', 'u', 'f')
              AND a.attnum > 0
              AND (',' || con.conkey || ',') LIKE ('%,' || a.attnum || ',%');
            "#,

            // Create information_schema.table_constraints view
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_table_constraints AS
            SELECT
                'main' as constraint_catalog,
                'public' as constraint_schema,
                con.conname as constraint_name,
                'main' as table_catalog,
                'public' as table_schema,
                c.relname as table_name,
                CASE con.contype
                    WHEN 'p' THEN 'PRIMARY KEY'
                    WHEN 'u' THEN 'UNIQUE'
                    WHEN 'f' THEN 'FOREIGN KEY'
                    WHEN 'c' THEN 'CHECK'
                    ELSE 'UNKNOWN'
                END as constraint_type,
                CASE WHEN con.condeferrable THEN 'YES' ELSE 'NO' END as is_deferrable,
                CASE WHEN con.condeferred THEN 'YES' ELSE 'NO' END as initially_deferred,
                CASE WHEN con.convalidated THEN 'YES' ELSE 'NO' END as enforced
            FROM pg_constraint con
            JOIN pg_class c ON con.conrelid = c.oid;
            "#,

            // Create information_schema.referential_constraints view
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_referential_constraints AS
            SELECT
                'main' as constraint_catalog,
                'public' as constraint_schema,
                con.conname as constraint_name,
                'main' as unique_constraint_catalog,
                'public' as unique_constraint_schema,
                ref_c.relname || '_pkey' as unique_constraint_name,
                'NONE' as match_option,
                CASE con.confupdtype
                    WHEN 'a' THEN 'NO ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                    ELSE 'NO ACTION'
                END as update_rule,
                CASE con.confdeltype
                    WHEN 'a' THEN 'NO ACTION'
                    WHEN 'r' THEN 'RESTRICT'
                    WHEN 'c' THEN 'CASCADE'
                    WHEN 'n' THEN 'SET NULL'
                    WHEN 'd' THEN 'SET DEFAULT'
                    ELSE 'NO ACTION'
                END as delete_rule
            FROM pg_constraint con
            JOIN pg_class c ON con.conrelid = c.oid
            LEFT JOIN pg_class ref_c ON con.confrelid = ref_c.oid
            WHERE con.contype = 'f';
            "#,

            // Create information_schema.schemata view
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_schemata AS
            SELECT
                'main' as catalog_name,
                'public' as schema_name,
                'postgres' as schema_owner,
                NULL as default_character_set_catalog,
                NULL as default_character_set_schema,
                NULL as default_character_set_name,
                NULL as sql_path;
            "#,

            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '14', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- Remove information_schema views
            DROP VIEW IF EXISTS information_schema_tables;
            DROP VIEW IF EXISTS information_schema_columns;
            DROP VIEW IF EXISTS information_schema_key_column_usage;
            DROP VIEW IF EXISTS information_schema_table_constraints;
            DROP VIEW IF EXISTS information_schema_referential_constraints;
            DROP VIEW IF EXISTS information_schema_schemata;

            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '13', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![13],
    });
}
