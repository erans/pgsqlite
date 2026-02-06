use super::{Migration, MigrationAction};
use std::collections::BTreeMap;

pub(super) fn register_v24_pg_tablespace_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(24, Migration {
        version: 24,
        name: "pg_tablespace_support",
        description: "Add PostgreSQL pg_tablespace catalog support for tablespace introspection and ORM compatibility",
        up: MigrationAction::SqlBatch(&[
            // Note: pg_tablespace is handled by the catalog interceptor, no SQLite view needed
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '24', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- pg_tablespace is handled by catalog interceptor, no view to remove
            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '23', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![23],
    });
}

/// Version 25: information_schema.triggers support
pub(super) fn register_v25_information_schema_triggers_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(25, Migration {
        version: 25,
        name: "information_schema_triggers_support",
        description: "Add PostgreSQL information_schema.triggers support for trigger introspection and ORM compatibility",
        up: MigrationAction::SqlBatch(&[
            // Note: information_schema.triggers is handled by the catalog interceptor, no SQLite view needed
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '25', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- information_schema.triggers is handled by catalog interceptor, no view to remove
            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '24', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![24],
    });
}

/// Version 26: Enhanced pg_attribute support with proper default and identity detection
pub(super) fn register_v26_enhanced_pg_attribute_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(26, Migration {
        version: 26,
        name: "enhanced_pg_attribute_support",
        description: "Enhanced pg_attribute view with proper default and identity column detection for JOIN queries",
        up: MigrationAction::SqlBatch(&[
            // Drop existing views separately to ensure they're really gone
            r#"DROP VIEW IF EXISTS pg_attribute"#,
            r#"DROP VIEW IF EXISTS pg_class"#,

            // Recreate pg_class with SQLite built-in functions for consistent OID generation
            r#"
            CREATE VIEW IF NOT EXISTS pg_class AS
            SELECT
                -- Use SQLite built-in functions for consistent OID generation
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
                'h' as relkind_full,
                't' as relispopulated,
                'v' as relreplident,
                't' as relispartition,
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
            "#,

            // Create enhanced pg_attribute view with proper default and identity detection
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
                CASE WHEN p."notnull" = 1 THEN 't' ELSE 'f' END as attnotnull,

                -- Enhanced default detection using pg_attrdef table
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM pg_attrdef def
                        WHERE def.adrelid = CAST(
                            (
                                (unicode(substr(m.name, 1, 1)) * 1000000) +
                                (unicode(substr(m.name || ' ', 2, 1)) * 10000) +
                                (unicode(substr(m.name || '  ', 3, 1)) * 100) +
                                (length(m.name) * 7)
                            ) % 1000000 + 16384
                        AS TEXT)
                        AND def.adnum = CAST(p.cid + 1 AS TEXT)
                    ) THEN 't'
                    ELSE 'f'
                END as atthasdef,

                'f' as atthasmissing,

                -- Enhanced identity column detection for INTEGER PRIMARY KEY
                CASE
                    WHEN p.type LIKE '%INT%' AND p.pk = 1 THEN 'd'
                    ELSE ''
                END as attidentity,

                '' as attgenerated,
                'f' as attisdropped,
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
            SET value = '26', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            // Drop enhanced view
            r#"
            DROP VIEW IF EXISTS pg_attribute;
            "#,

            // Restore original simple view
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

            // Restore schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '25', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![25],
    });
}

/// Version 27: Fix pg_proc column types
pub(super) fn register_v27_fix_pg_proc_types(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(27, Migration {
        version: 27,
        name: "fix_pg_proc_types",
        description: "Fix pg_proc view column types to ensure proper text type inference",
        up: MigrationAction::SqlBatch(&[
            // Drop and recreate the view with proper type casting
            r#"DROP VIEW IF EXISTS pg_proc;"#,

            r#"
            CREATE VIEW IF NOT EXISTS pg_proc AS
            SELECT
                (16384 + row_number() OVER ()) as oid,          -- Unique OID starting from 16384
                func_name as proname,                           -- Function name
                11 as pronamespace,                             -- pg_catalog namespace
                10 as proowner,                                 -- postgres user OID
                12 as prolang,                                  -- SQL language OID
                1 as procost,                                   -- Cost estimate
                0 as prorows,                                   -- Estimated result rows
                0 as provariadic,                               -- Variadic argument OID
                0 as prosupport,                                -- Support function OID
                CAST(func_kind AS TEXT) as prokind,             -- Function kind ('f', 'a', 'p')
                CAST('f' AS TEXT) as prosecdef,                 -- Security definer
                CAST('f' AS TEXT) as proleakproof,              -- Leak proof
                CAST(func_strict AS TEXT) as proisstrict,       -- Strict (returns null on null input)
                CAST(func_retset AS TEXT) as proretset,         -- Returns set
                CAST(func_volatile AS TEXT) as provolatile,     -- Volatility ('i', 's', 'v')
                CAST('s' AS TEXT) as proparallel,               -- Parallel safety
                0 as pronargs,                                  -- Number of arguments (simplified)
                0 as pronargdefaults,                           -- Number of default arguments
                func_rettype as prorettype,                     -- Return type OID
                '' as proargtypes,                              -- Argument types (simplified)
                NULL as proallargtypes,                         -- All argument types
                NULL as proargmodes,                            -- Argument modes
                NULL as proargnames,                            -- Argument names
                NULL as proargdefaults,                         -- Default expressions
                NULL as protrftypes,                            -- Transform types
                '' as prosrc,                                   -- Source code
                NULL as probin,                                 -- Binary location
                NULL as prosqlbody,                             -- SQL body
                NULL as proconfig,                              -- Configuration
                NULL as proacl                                  -- Access privileges
            FROM (
                -- String functions
                SELECT 'length' as func_name, 'f' as func_kind, 't' as func_strict, 'f' as func_retset, 'i' as func_volatile, 23 as func_rettype
                UNION ALL SELECT 'lower', 'f', 't', 'f', 'i', 25
                UNION ALL SELECT 'upper', 'f', 't', 'f', 'i', 25
                UNION ALL SELECT 'substr', 'f', 't', 'f', 'i', 25
                UNION ALL SELECT 'replace', 'f', 't', 'f', 'i', 25
                UNION ALL SELECT 'trim', 'f', 't', 'f', 'i', 25
                UNION ALL SELECT 'ltrim', 'f', 't', 'f', 'i', 25
                UNION ALL SELECT 'rtrim', 'f', 't', 'f', 'i', 25

                -- Math functions
                UNION ALL SELECT 'abs', 'f', 't', 'f', 'i', 23
                UNION ALL SELECT 'round', 'f', 't', 'f', 'i', 1700
                UNION ALL SELECT 'ceil', 'f', 't', 'f', 'i', 1700
                UNION ALL SELECT 'floor', 'f', 't', 'f', 'i', 1700
                UNION ALL SELECT 'sqrt', 'f', 't', 'f', 'i', 701
                UNION ALL SELECT 'power', 'f', 't', 'f', 'i', 701

                -- Aggregate functions
                UNION ALL SELECT 'count', 'a', 'f', 't', 'v', 20  -- bigint
                UNION ALL SELECT 'sum', 'a', 'f', 't', 'v', 1700  -- numeric
                UNION ALL SELECT 'avg', 'a', 'f', 't', 'v', 1700  -- numeric
                UNION ALL SELECT 'max', 'a', 'f', 't', 'v', 2283  -- any
                UNION ALL SELECT 'min', 'a', 'f', 't', 'v', 2283  -- any

                -- Date/time functions
                UNION ALL SELECT 'now', 'f', 'f', 'f', 'v', 1184  -- timestamptz
                UNION ALL SELECT 'date', 'f', 't', 'f', 'i', 1082  -- date
                UNION ALL SELECT 'extract', 'f', 't', 'f', 'i', 701  -- float8

                -- JSON functions
                UNION ALL SELECT 'json_agg', 'a', 'f', 't', 'v', 114     -- json
                UNION ALL SELECT 'jsonb_agg', 'a', 'f', 't', 'v', 3802   -- jsonb
                UNION ALL SELECT 'json_object_agg', 'a', 'f', 't', 'v', 114  -- json
                UNION ALL SELECT 'json_extract', 'f', 't', 'f', 'i', 25   -- text
                UNION ALL SELECT 'jsonb_set', 'f', 't', 'f', 'i', 3802    -- jsonb

                -- Array functions
                UNION ALL SELECT 'array_agg', 'a', 'f', 't', 'v', 2277    -- anyarray
                UNION ALL SELECT 'unnest', 'f', 'f', 't', 'i', 2283       -- setof any
                UNION ALL SELECT 'array_length', 'f', 't', 'f', 'i', 23   -- int4

                -- UUID functions
                UNION ALL SELECT 'uuid_generate_v4', 'f', 'f', 'f', 'v', 2950  -- uuid

                -- System functions
                UNION ALL SELECT 'version', 'f', 'f', 'f', 's', 25         -- text
                UNION ALL SELECT 'current_database', 'f', 'f', 'f', 's', 19  -- name
                UNION ALL SELECT 'current_user', 'f', 'f', 'f', 's', 19      -- name
                UNION ALL SELECT 'session_user', 'f', 'f', 'f', 's', 19      -- name
                UNION ALL SELECT 'current_schema', 'f', 'f', 'f', 's', 19    -- name
                UNION ALL SELECT 'current_setting', 'f', 'f', 'f', 's', 25   -- text
                UNION ALL SELECT 'current_schemas', 'f', 'f', 'f', 's', 1003 -- name[]
                UNION ALL SELECT 'user', 'f', 'f', 'f', 's', 19              -- name
                UNION ALL SELECT 'pg_backend_pid', 'f', 'f', 'f', 's', 23    -- int4
                UNION ALL SELECT 'pg_is_in_recovery', 'f', 'f', 'f', 's', 16 -- boolean

                -- PostgreSQL system functions
                UNION ALL SELECT 'pg_has_role', 'f', 'f', 'f', 's', 16       -- boolean
                UNION ALL SELECT 'has_table_privilege', 'f', 'f', 'f', 's', 16  -- boolean

                -- Full-text search
                UNION ALL SELECT 'to_tsvector', 'f', 't', 'f', 'i', 3614     -- tsvector
                UNION ALL SELECT 'to_tsquery', 'f', 't', 'f', 'i', 3615      -- tsquery
                UNION ALL SELECT 'plainto_tsquery', 'f', 't', 'f', 'i', 3615 -- tsquery
            );
            "#,

            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '27', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            // Restore previous version
            r#"DROP VIEW IF EXISTS pg_proc;"#,

            r#"
            UPDATE __pgsqlite_metadata
            SET value = '26', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![26],
    });
}

/// Version 28: Add pg_stat_io, pg_locks, pg_prepared_statements, pg_prepared_xacts, pg_cursors support
pub(super) fn register_v28_pg_stat_io_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(28, Migration {
        version: 28,
        name: "pg_stat_io_and_locks_support",
        description: "Add pg_stat_io (PostgreSQL 16+), pg_locks, pg_prepared_statements, pg_prepared_xacts, and pg_cursors views for compatibility",
        up: MigrationAction::SqlBatch(&[
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '28', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            // Restore previous version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '27', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![27],
    });
}

/// Version 29: Create complete information_schema views as real SQLite views
/// This migration adds the missing information_schema views that were previously
/// handled only by catalog interceptors. Having real views enables JOIN, COUNT,
/// and other SQL operations that require actual table structures.
pub(super) fn register_v29_information_schema_complete_views(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(29, Migration {
        version: 29,
        name: "information_schema_complete_views",
        description: "Create complete information_schema views (routines, views, check_constraints, triggers) as real SQLite views for JOIN/COUNT support",
        up: MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_routines;"#,
            r#"DROP VIEW IF EXISTS information_schema_views;"#,
            r#"DROP VIEW IF EXISTS information_schema_check_constraints;"#,
            r#"DROP VIEW IF EXISTS information_schema_triggers;"#,
            r#"DROP VIEW IF EXISTS information_schema_schemata;"#,
            r#"DROP VIEW IF EXISTS information_schema_tables;"#,
            r#"DROP VIEW IF EXISTS information_schema_columns;"#,

            r#"
            CREATE VIEW IF NOT EXISTS information_schema_schemata AS
            SELECT 'main' as catalog_name, 'public' as schema_name, 'postgres' as schema_owner,
                   NULL as default_character_set_catalog, NULL as default_character_set_schema,
                   NULL as default_character_set_name, NULL as sql_path
            UNION ALL
            SELECT 'main', 'pg_catalog', 'postgres', NULL, NULL, NULL, NULL
            UNION ALL
            SELECT 'main', 'information_schema', 'postgres', NULL, NULL, NULL, NULL;
            "#,

            r#"
            CREATE VIEW IF NOT EXISTS information_schema_tables AS
            SELECT
                'main' as table_catalog,
                'public' as table_schema,
                m.name as table_name,
                CASE m.type
                    WHEN 'table' THEN 'BASE TABLE'
                    WHEN 'view' THEN 'VIEW'
                    ELSE 'UNKNOWN'
                END as table_type,
                NULL as self_referencing_column_name,
                NULL as reference_generation,
                NULL as user_defined_type_catalog,
                NULL as user_defined_type_schema,
                NULL as user_defined_type_name,
                CASE m.type
                    WHEN 'view' THEN 'NO'
                    ELSE 'YES'
                END as is_insertable_into,
                'NO' as is_typed,
                'NO' as commit_action
            FROM sqlite_master m
            WHERE m.type IN ('table', 'view')
              AND m.name NOT LIKE 'sqlite_%'
              AND m.name NOT LIKE '__pgsqlite_%';
            "#,

            r#"
            CREATE VIEW IF NOT EXISTS information_schema_columns AS
            SELECT
                'main' as table_catalog,
                'public' as table_schema,
                m.name as table_name,
                p.name as column_name,
                p.cid + 1 as ordinal_position,
                NULL as column_default,
                CASE
                    WHEN p."notnull" = 1 OR p.pk = 1 THEN 'NO'
                    ELSE 'YES'
                END as is_nullable,
                CASE
                    WHEN s.pg_type IS NOT NULL THEN
                        CASE
                            WHEN upper(s.pg_type) LIKE 'VARCHAR%' THEN 'character varying'
                            WHEN upper(s.pg_type) LIKE 'CHARACTER VARYING%' THEN 'character varying'
                            WHEN upper(s.pg_type) LIKE 'CHARACTER%' THEN 'character'
                            WHEN upper(s.pg_type) LIKE 'CHAR%' THEN 'character'
                            WHEN upper(s.pg_type) LIKE 'TEXT%' THEN 'text'
                            WHEN upper(s.pg_type) LIKE 'INTEGER%' THEN 'integer'
                            WHEN upper(s.pg_type) LIKE 'INT%' THEN 'integer'
                            WHEN upper(s.pg_type) LIKE 'BIGINT%' THEN 'bigint'
                            WHEN upper(s.pg_type) LIKE 'SMALLINT%' THEN 'smallint'
                            WHEN upper(s.pg_type) LIKE 'DECIMAL%' THEN 'numeric'
                            WHEN upper(s.pg_type) LIKE 'NUMERIC%' THEN 'numeric'
                            WHEN upper(s.pg_type) LIKE 'BOOL%' THEN 'boolean'
                            WHEN upper(s.pg_type) LIKE 'BOOLEAN%' THEN 'boolean'
                            WHEN upper(s.pg_type) LIKE 'TIMESTAMP%' THEN 'timestamp without time zone'
                            WHEN upper(s.pg_type) LIKE 'DATE%' THEN 'date'
                            WHEN upper(s.pg_type) LIKE 'TIME%' THEN 'time without time zone'
                            ELSE lower(s.pg_type)
                        END
                    ELSE
                        CASE
                            WHEN upper(p.type) LIKE '%INT%' THEN 'integer'
                            WHEN upper(p.type) LIKE 'VARCHAR%' THEN 'character varying'
                            WHEN upper(p.type) LIKE 'CHARACTER VARYING%' THEN 'character varying'
                            WHEN upper(p.type) LIKE 'CHAR%' THEN 'character'
                            WHEN upper(p.type) = 'TEXT' THEN 'text'
                            WHEN upper(p.type) = 'REAL' THEN 'real'
                            WHEN upper(p.type) = 'DOUBLE' THEN 'double precision'
                            WHEN upper(p.type) = 'BLOB' THEN 'bytea'
                            WHEN upper(p.type) = 'BOOLEAN' THEN 'boolean'
                            WHEN upper(p.type) = 'DATE' THEN 'date'
                            WHEN upper(p.type) LIKE 'TIME%' THEN 'time without time zone'
                            WHEN upper(p.type) LIKE 'TIMESTAMP%' THEN 'timestamp without time zone'
                            WHEN upper(p.type) LIKE 'DECIMAL%' THEN 'numeric'
                            WHEN upper(p.type) LIKE 'NUMERIC%' THEN 'numeric'
                            ELSE 'text'
                        END
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
            FROM sqlite_master m
            JOIN pragma_table_info(m.name) p
            LEFT JOIN __pgsqlite_schema s
                ON s.table_name = m.name AND s.column_name = p.name
            WHERE m.type = 'table'
              AND m.name NOT LIKE 'sqlite_%'
              AND m.name NOT LIKE '__pgsqlite_%';
            "#,

            r#"
            CREATE VIEW IF NOT EXISTS information_schema_routines AS
            SELECT
                'main' as routine_catalog,
                'pg_catalog' as routine_schema,
                func_name as routine_name,
                'main' as specific_catalog,
                'pg_catalog' as specific_schema,
                func_name as specific_name,
                CASE func_kind
                    WHEN 'f' THEN 'FUNCTION'
                    WHEN 'a' THEN 'FUNCTION'
                    WHEN 'p' THEN 'PROCEDURE'
                    ELSE 'FUNCTION'
                END as routine_type,
                NULL as module_catalog,
                NULL as module_schema,
                NULL as module_name,
                NULL as udt_catalog,
                NULL as udt_schema,
                NULL as udt_name,
                CASE func_rettype
                    WHEN 23 THEN 'integer'
                    WHEN 25 THEN 'text'
                    WHEN 16 THEN 'boolean'
                    WHEN 20 THEN 'bigint'
                    WHEN 21 THEN 'smallint'
                    WHEN 700 THEN 'real'
                    WHEN 701 THEN 'double precision'
                    WHEN 1043 THEN 'character varying'
                    WHEN 1082 THEN 'date'
                    WHEN 1083 THEN 'time without time zone'
                    WHEN 1114 THEN 'timestamp without time zone'
                    WHEN 1184 THEN 'timestamp with time zone'
                    WHEN 1700 THEN 'numeric'
                    WHEN 114 THEN 'json'
                    WHEN 3802 THEN 'jsonb'
                    WHEN 2950 THEN 'uuid'
                    WHEN 3614 THEN 'tsvector'
                    WHEN 3615 THEN 'tsquery'
                    WHEN 2277 THEN 'anyarray'
                    WHEN 2283 THEN 'anyelement'
                    ELSE 'text'
                END as data_type,
                NULL as character_maximum_length,
                NULL as character_octet_length,
                NULL as character_set_catalog,
                NULL as character_set_schema,
                NULL as character_set_name,
                NULL as collation_catalog,
                NULL as collation_schema,
                NULL as collation_name,
                NULL as numeric_precision,
                NULL as numeric_precision_radix,
                NULL as numeric_scale,
                NULL as datetime_precision,
                NULL as interval_type,
                NULL as interval_precision,
                NULL as type_udt_catalog,
                NULL as type_udt_schema,
                NULL as type_udt_name,
                NULL as scope_catalog,
                NULL as scope_schema,
                NULL as scope_name,
                NULL as maximum_cardinality,
                NULL as dtd_identifier,
                'EXTERNAL' as routine_body,
                '' as routine_definition,
                NULL as external_name,
                'SQL' as external_language,
                'SQL' as parameter_style,
                CASE func_strict WHEN 't' THEN 'YES' ELSE 'NO' END as is_deterministic,
                'CONTAINS_SQL' as sql_data_access,
                CASE func_kind WHEN 'p' THEN 'YES' ELSE NULL END as is_null_call,
                NULL as sql_path,
                'YES' as schema_level_routine,
                0 as max_dynamic_result_sets,
                'NO' as is_user_defined_cast,
                'NO' as is_implicitly_invocable,
                'INVOKER' as security_type,
                NULL as to_sql_specific_catalog,
                NULL as to_sql_specific_schema,
                NULL as to_sql_specific_name,
                'NO' as as_locator,
                NULL as created,
                NULL as last_altered,
                NULL as new_savepoint_level,
                'NO' as is_udt_dependent,
                NULL as result_cast_from_data_type,
                NULL as result_cast_as_locator,
                NULL as result_cast_char_max_length,
                NULL as result_cast_char_octet_length,
                NULL as result_cast_char_set_catalog,
                NULL as result_cast_char_set_schema,
                NULL as result_cast_char_set_name,
                NULL as result_cast_collation_catalog,
                NULL as result_cast_collation_schema,
                NULL as result_cast_collation_name,
                NULL as result_cast_numeric_precision,
                NULL as result_cast_numeric_precision_radix,
                NULL as result_cast_numeric_scale,
                NULL as result_cast_datetime_precision,
                NULL as result_cast_interval_type,
                NULL as result_cast_interval_precision,
                NULL as result_cast_type_udt_catalog,
                NULL as result_cast_type_udt_schema,
                NULL as result_cast_type_udt_name,
                NULL as result_cast_scope_catalog,
                NULL as result_cast_scope_schema,
                NULL as result_cast_scope_name,
                NULL as result_cast_maximum_cardinality,
                NULL as result_cast_dtd_identifier
            FROM (
                -- String functions
                SELECT 'length' as func_name, 'f' as func_kind, 't' as func_strict, 23 as func_rettype
                UNION ALL SELECT 'lower', 'f', 't', 25
                UNION ALL SELECT 'upper', 'f', 't', 25
                UNION ALL SELECT 'substr', 'f', 't', 25
                UNION ALL SELECT 'substring', 'f', 't', 25
                UNION ALL SELECT 'replace', 'f', 't', 25
                UNION ALL SELECT 'trim', 'f', 't', 25
                UNION ALL SELECT 'ltrim', 'f', 't', 25
                UNION ALL SELECT 'rtrim', 'f', 't', 25
                UNION ALL SELECT 'concat', 'f', 'f', 25
                UNION ALL SELECT 'concat_ws', 'f', 'f', 25
                UNION ALL SELECT 'left', 'f', 't', 25
                UNION ALL SELECT 'right', 'f', 't', 25
                UNION ALL SELECT 'repeat', 'f', 't', 25
                UNION ALL SELECT 'reverse', 'f', 't', 25
                UNION ALL SELECT 'split_part', 'f', 't', 25
                UNION ALL SELECT 'string_agg', 'a', 'f', 25
                UNION ALL SELECT 'translate', 'f', 't', 25
                UNION ALL SELECT 'ascii', 'f', 't', 23
                UNION ALL SELECT 'chr', 'f', 't', 25
                UNION ALL SELECT 'initcap', 'f', 't', 25
                UNION ALL SELECT 'lpad', 'f', 't', 25
                UNION ALL SELECT 'rpad', 'f', 't', 25
                UNION ALL SELECT 'position', 'f', 't', 23
                UNION ALL SELECT 'strpos', 'f', 't', 23

                -- Math functions
                UNION ALL SELECT 'abs', 'f', 't', 23
                UNION ALL SELECT 'round', 'f', 't', 1700
                UNION ALL SELECT 'ceil', 'f', 't', 1700
                UNION ALL SELECT 'ceiling', 'f', 't', 1700
                UNION ALL SELECT 'floor', 'f', 't', 1700
                UNION ALL SELECT 'trunc', 'f', 't', 1700
                UNION ALL SELECT 'sqrt', 'f', 't', 701
                UNION ALL SELECT 'power', 'f', 't', 701
                UNION ALL SELECT 'exp', 'f', 't', 701
                UNION ALL SELECT 'ln', 'f', 't', 701
                UNION ALL SELECT 'log', 'f', 't', 701
                UNION ALL SELECT 'mod', 'f', 't', 23
                UNION ALL SELECT 'sign', 'f', 't', 23
                UNION ALL SELECT 'random', 'f', 'f', 701
                UNION ALL SELECT 'pi', 'f', 'f', 701
                UNION ALL SELECT 'degrees', 'f', 't', 701
                UNION ALL SELECT 'radians', 'f', 't', 701
                UNION ALL SELECT 'sin', 'f', 't', 701
                UNION ALL SELECT 'cos', 'f', 't', 701
                UNION ALL SELECT 'tan', 'f', 't', 701
                UNION ALL SELECT 'asin', 'f', 't', 701
                UNION ALL SELECT 'acos', 'f', 't', 701
                UNION ALL SELECT 'atan', 'f', 't', 701
                UNION ALL SELECT 'atan2', 'f', 't', 701

                -- Aggregate functions
                UNION ALL SELECT 'count', 'a', 'f', 20
                UNION ALL SELECT 'sum', 'a', 'f', 1700
                UNION ALL SELECT 'avg', 'a', 'f', 1700
                UNION ALL SELECT 'max', 'a', 'f', 2283
                UNION ALL SELECT 'min', 'a', 'f', 2283
                UNION ALL SELECT 'array_agg', 'a', 'f', 2277
                UNION ALL SELECT 'bool_and', 'a', 'f', 16
                UNION ALL SELECT 'bool_or', 'a', 'f', 16
                UNION ALL SELECT 'every', 'a', 'f', 16
                UNION ALL SELECT 'bit_and', 'a', 'f', 23
                UNION ALL SELECT 'bit_or', 'a', 'f', 23

                -- Date/time functions
                UNION ALL SELECT 'now', 'f', 'f', 1184
                UNION ALL SELECT 'current_timestamp', 'f', 'f', 1184
                UNION ALL SELECT 'current_date', 'f', 'f', 1082
                UNION ALL SELECT 'current_time', 'f', 'f', 1083
                UNION ALL SELECT 'localtime', 'f', 'f', 1083
                UNION ALL SELECT 'localtimestamp', 'f', 'f', 1114
                UNION ALL SELECT 'date', 'f', 't', 1082
                UNION ALL SELECT 'time', 'f', 't', 1083
                UNION ALL SELECT 'timestamp', 'f', 't', 1114
                UNION ALL SELECT 'extract', 'f', 't', 701
                UNION ALL SELECT 'date_part', 'f', 't', 701
                UNION ALL SELECT 'date_trunc', 'f', 't', 1184
                UNION ALL SELECT 'age', 'f', 't', 1186
                UNION ALL SELECT 'to_char', 'f', 't', 25
                UNION ALL SELECT 'to_date', 'f', 't', 1082
                UNION ALL SELECT 'to_timestamp', 'f', 't', 1184
                UNION ALL SELECT 'make_date', 'f', 't', 1082
                UNION ALL SELECT 'make_time', 'f', 't', 1083
                UNION ALL SELECT 'make_timestamp', 'f', 't', 1114
                UNION ALL SELECT 'make_timestamptz', 'f', 't', 1184

                -- JSON functions
                UNION ALL SELECT 'json_agg', 'a', 'f', 114
                UNION ALL SELECT 'jsonb_agg', 'a', 'f', 3802
                UNION ALL SELECT 'json_object_agg', 'a', 'f', 114
                UNION ALL SELECT 'jsonb_object_agg', 'a', 'f', 3802
                UNION ALL SELECT 'to_json', 'f', 't', 114
                UNION ALL SELECT 'to_jsonb', 'f', 't', 3802
                UNION ALL SELECT 'row_to_json', 'f', 't', 114
                UNION ALL SELECT 'json_build_array', 'f', 'f', 114
                UNION ALL SELECT 'jsonb_build_array', 'f', 'f', 3802
                UNION ALL SELECT 'json_build_object', 'f', 'f', 114
                UNION ALL SELECT 'jsonb_build_object', 'f', 'f', 3802
                UNION ALL SELECT 'json_extract_path', 'f', 't', 114
                UNION ALL SELECT 'jsonb_extract_path', 'f', 't', 3802
                UNION ALL SELECT 'json_extract_path_text', 'f', 't', 25
                UNION ALL SELECT 'jsonb_extract_path_text', 'f', 't', 25
                UNION ALL SELECT 'json_array_length', 'f', 't', 23
                UNION ALL SELECT 'jsonb_array_length', 'f', 't', 23
                UNION ALL SELECT 'json_typeof', 'f', 't', 25
                UNION ALL SELECT 'jsonb_typeof', 'f', 't', 25
                UNION ALL SELECT 'jsonb_set', 'f', 't', 3802
                UNION ALL SELECT 'jsonb_insert', 'f', 't', 3802
                UNION ALL SELECT 'jsonb_delete', 'f', 't', 3802
                UNION ALL SELECT 'jsonb_pretty', 'f', 't', 25
                UNION ALL SELECT 'json_each', 'f', 't', 2249
                UNION ALL SELECT 'jsonb_each', 'f', 't', 2249
                UNION ALL SELECT 'json_each_text', 'f', 't', 2249
                UNION ALL SELECT 'jsonb_each_text', 'f', 't', 2249
                UNION ALL SELECT 'json_array_elements', 'f', 't', 114
                UNION ALL SELECT 'jsonb_array_elements', 'f', 't', 3802
                UNION ALL SELECT 'json_array_elements_text', 'f', 't', 25
                UNION ALL SELECT 'jsonb_array_elements_text', 'f', 't', 25
                UNION ALL SELECT 'json_object_keys', 'f', 't', 25
                UNION ALL SELECT 'jsonb_object_keys', 'f', 't', 25
                UNION ALL SELECT 'json_populate_record', 'f', 't', 2249
                UNION ALL SELECT 'jsonb_populate_record', 'f', 't', 2249
                UNION ALL SELECT 'json_to_record', 'f', 't', 2249
                UNION ALL SELECT 'jsonb_to_record', 'f', 't', 2249
                UNION ALL SELECT 'json_strip_nulls', 'f', 't', 114
                UNION ALL SELECT 'jsonb_strip_nulls', 'f', 't', 3802

                -- Array functions
                UNION ALL SELECT 'unnest', 'f', 'f', 2283
                UNION ALL SELECT 'array_length', 'f', 't', 23
                UNION ALL SELECT 'array_dims', 'f', 't', 25
                UNION ALL SELECT 'array_lower', 'f', 't', 23
                UNION ALL SELECT 'array_upper', 'f', 't', 23
                UNION ALL SELECT 'array_ndims', 'f', 't', 23
                UNION ALL SELECT 'array_position', 'f', 't', 23
                UNION ALL SELECT 'array_positions', 'f', 't', 1007
                UNION ALL SELECT 'array_remove', 'f', 't', 2277
                UNION ALL SELECT 'array_replace', 'f', 't', 2277
                UNION ALL SELECT 'array_cat', 'f', 't', 2277
                UNION ALL SELECT 'array_append', 'f', 't', 2277
                UNION ALL SELECT 'array_prepend', 'f', 't', 2277
                UNION ALL SELECT 'array_to_string', 'f', 't', 25
                UNION ALL SELECT 'string_to_array', 'f', 't', 1009
                UNION ALL SELECT 'cardinality', 'f', 't', 23

                -- UUID functions
                UNION ALL SELECT 'gen_random_uuid', 'f', 'f', 2950
                UNION ALL SELECT 'uuid_generate_v1', 'f', 'f', 2950
                UNION ALL SELECT 'uuid_generate_v1mc', 'f', 'f', 2950
                UNION ALL SELECT 'uuid_generate_v3', 'f', 't', 2950
                UNION ALL SELECT 'uuid_generate_v4', 'f', 'f', 2950
                UNION ALL SELECT 'uuid_generate_v5', 'f', 't', 2950
                UNION ALL SELECT 'uuid_nil', 'f', 'f', 2950
                UNION ALL SELECT 'uuid_ns_dns', 'f', 'f', 2950
                UNION ALL SELECT 'uuid_ns_url', 'f', 'f', 2950
                UNION ALL SELECT 'uuid_ns_oid', 'f', 'f', 2950
                UNION ALL SELECT 'uuid_ns_x500', 'f', 'f', 2950

                -- unaccent
                UNION ALL SELECT 'unaccent', 'f', 't', 25

                -- Full-text search
                UNION ALL SELECT 'to_tsvector', 'f', 't', 3614
                UNION ALL SELECT 'to_tsquery', 'f', 't', 3615
                UNION ALL SELECT 'plainto_tsquery', 'f', 't', 3615
                UNION ALL SELECT 'phraseto_tsquery', 'f', 't', 3615
                UNION ALL SELECT 'websearch_to_tsquery', 'f', 't', 3615
                UNION ALL SELECT 'ts_headline', 'f', 't', 25
                UNION ALL SELECT 'ts_rank', 'f', 't', 700
                UNION ALL SELECT 'ts_rank_cd', 'f', 't', 700

                -- System/session functions
                UNION ALL SELECT 'version', 'f', 'f', 25
                UNION ALL SELECT 'current_database', 'f', 'f', 19
                UNION ALL SELECT 'current_user', 'f', 'f', 19
                UNION ALL SELECT 'session_user', 'f', 'f', 19
                UNION ALL SELECT 'current_schema', 'f', 'f', 19
                UNION ALL SELECT 'current_schemas', 'f', 'f', 1003
                UNION ALL SELECT 'current_setting', 'f', 'f', 25
                UNION ALL SELECT 'set_config', 'f', 't', 25
                UNION ALL SELECT 'pg_backend_pid', 'f', 'f', 23
                UNION ALL SELECT 'pg_is_in_recovery', 'f', 'f', 16
                UNION ALL SELECT 'pg_postmaster_start_time', 'f', 'f', 1184
                UNION ALL SELECT 'pg_conf_load_time', 'f', 'f', 1184
                UNION ALL SELECT 'inet_server_addr', 'f', 'f', 869
                UNION ALL SELECT 'inet_server_port', 'f', 'f', 23
                UNION ALL SELECT 'inet_client_addr', 'f', 'f', 869
                UNION ALL SELECT 'inet_client_port', 'f', 'f', 23

                -- PostgreSQL catalog functions
                UNION ALL SELECT 'pg_has_role', 'f', 'f', 16
                UNION ALL SELECT 'has_table_privilege', 'f', 'f', 16
                UNION ALL SELECT 'has_schema_privilege', 'f', 'f', 16
                UNION ALL SELECT 'has_database_privilege', 'f', 'f', 16
                UNION ALL SELECT 'pg_table_is_visible', 'f', 't', 16
                UNION ALL SELECT 'pg_type_is_visible', 'f', 't', 16
                UNION ALL SELECT 'pg_function_is_visible', 'f', 't', 16
                UNION ALL SELECT 'pg_get_constraintdef', 'f', 't', 25
                UNION ALL SELECT 'pg_get_indexdef', 'f', 't', 25
                UNION ALL SELECT 'pg_get_viewdef', 'f', 't', 25
                UNION ALL SELECT 'pg_get_triggerdef', 'f', 't', 25
                UNION ALL SELECT 'pg_get_expr', 'f', 't', 25
                UNION ALL SELECT 'pg_get_userbyid', 'f', 't', 19
                UNION ALL SELECT 'format_type', 'f', 't', 25
                UNION ALL SELECT 'to_regtype', 'f', 't', 2206
                UNION ALL SELECT 'to_regclass', 'f', 't', 2205
                UNION ALL SELECT 'to_regproc', 'f', 't', 24
                UNION ALL SELECT 'pg_typeof', 'f', 't', 2206
                UNION ALL SELECT 'pg_column_size', 'f', 't', 23
                UNION ALL SELECT 'pg_database_size', 'f', 't', 20
                UNION ALL SELECT 'pg_table_size', 'f', 't', 20
                UNION ALL SELECT 'pg_total_relation_size', 'f', 't', 20
                UNION ALL SELECT 'pg_size_pretty', 'f', 't', 25
                UNION ALL SELECT 'pg_relation_size', 'f', 't', 20
                UNION ALL SELECT 'pg_indexes_size', 'f', 't', 20

                -- Coalesce/nullif/case
                UNION ALL SELECT 'coalesce', 'f', 'f', 2283
                UNION ALL SELECT 'nullif', 'f', 't', 2283
                UNION ALL SELECT 'greatest', 'f', 'f', 2283
                UNION ALL SELECT 'least', 'f', 'f', 2283

                -- Type casting
                UNION ALL SELECT 'cast', 'f', 't', 2283

                -- Boolean
                UNION ALL SELECT 'bool', 'f', 't', 16
            );
            "#,

            // Create information_schema_views view
            // This view provides metadata about user-defined views
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_views AS
            SELECT
                'main' as table_catalog,
                'public' as table_schema,
                name as table_name,
                sql as view_definition,
                'NONE' as check_option,
                'NO' as is_updatable,
                'NO' as is_insertable_into,
                'NO' as is_trigger_updatable,
                'NO' as is_trigger_deletable,
                'NO' as is_trigger_insertable_into
            FROM sqlite_master
            WHERE type = 'view'
              AND name NOT LIKE 'sqlite_%'
              AND name NOT LIKE '__pgsqlite_%'
              AND name NOT LIKE 'pg_%'
              AND name NOT LIKE 'information_schema_%';
            "#,

            // Create information_schema_check_constraints view
            // This view provides metadata about CHECK constraints
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_check_constraints AS
            SELECT
                'main' as constraint_catalog,
                'public' as constraint_schema,
                con.conname as constraint_name,
                COALESCE(con.consrc, '') as check_clause
            FROM pg_constraint con
            WHERE con.contype = 'c';
            "#,

            // Create information_schema_triggers view
            // This view provides metadata about triggers
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_triggers AS
            SELECT
                'main' as trigger_catalog,
                'main' as trigger_schema,
                name as trigger_name,
                CASE
                    WHEN sql LIKE '%BEFORE%' THEN 'BEFORE'
                    WHEN sql LIKE '%AFTER%' THEN 'AFTER'
                    WHEN sql LIKE '%INSTEAD OF%' THEN 'INSTEAD OF'
                    ELSE 'AFTER'
                END as action_timing,
                CASE
                    WHEN sql LIKE '%INSERT%' THEN 'INSERT'
                    WHEN sql LIKE '%UPDATE%' THEN 'UPDATE'
                    WHEN sql LIKE '%DELETE%' THEN 'DELETE'
                    ELSE 'INSERT'
                END as event_manipulation,
                'main' as event_object_catalog,
                'public' as event_object_schema,
                tbl_name as event_object_table,
                NULL as event_object_column,
                0 as action_order,
                NULL as action_condition,
                sql as action_statement,
                'ROW' as action_orientation,
                'ROW' as action_timing_original,
                NULL as action_reference_old_table,
                NULL as action_reference_new_table,
                NULL as action_reference_old_row,
                NULL as action_reference_new_row,
                NULL as created
            FROM sqlite_master
            WHERE type = 'trigger'
              AND name NOT LIKE 'sqlite_%'
              AND name NOT LIKE '__pgsqlite_%';
            "#,

            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '29', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_routines;"#,
            r#"DROP VIEW IF EXISTS information_schema_views;"#,
            r#"DROP VIEW IF EXISTS information_schema_check_constraints;"#,
            r#"DROP VIEW IF EXISTS information_schema_triggers;"#,
            r#"DROP VIEW IF EXISTS information_schema_schemata;"#,
            r#"DROP VIEW IF EXISTS information_schema_tables;"#,
            r#"DROP VIEW IF EXISTS information_schema_columns;"#,

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

            r#"
            CREATE VIEW IF NOT EXISTS information_schema_schemata AS
            SELECT 'main' as catalog_name, 'public' as schema_name, 'postgres' as schema_owner,
                   NULL as default_character_set_catalog, NULL as default_character_set_schema,
                   NULL as default_character_set_name, NULL as sql_path;
            "#,

            r#"
            UPDATE __pgsqlite_metadata
            SET value = '28', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![28],
    });
}

/// Version 30: Add schema metadata table and dynamic schema views
pub(super) fn register_v30_schema_metadata_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(30, Migration {
        version: 30,
        name: "schema_metadata_support",
        description: "Add __pgsqlite_schemas table and dynamic pg_namespace/information_schema.schemata views",
        up: MigrationAction::SqlBatch(&[
            r#"
            CREATE TABLE IF NOT EXISTS __pgsqlite_schemas (
                schema_name TEXT PRIMARY KEY,
                schema_oid INTEGER NOT NULL,
                schema_owner TEXT NOT NULL,
                is_system INTEGER NOT NULL DEFAULT 0
            );
            "#,
            r#"
            INSERT OR IGNORE INTO __pgsqlite_schemas (schema_name, schema_oid, schema_owner, is_system)
            VALUES
                ('pg_catalog', 11, 'postgres', 1),
                ('public', 2200, 'postgres', 0),
                ('information_schema', 13445, 'postgres', 1);
            "#,
            r#"DROP VIEW IF EXISTS pg_namespace;"#,
            r#"
            CREATE VIEW IF NOT EXISTS pg_namespace AS
            SELECT
                schema_oid as oid,
                schema_name as nspname,
                10 as nspowner,
                NULL as nspacl
            FROM __pgsqlite_schemas;
            "#,
            r#"DROP VIEW IF EXISTS information_schema_schemata;"#,
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_schemata AS
            SELECT
                'main' as catalog_name,
                schema_name as schema_name,
                schema_owner as schema_owner,
                NULL as default_character_set_catalog,
                NULL as default_character_set_schema,
                NULL as default_character_set_name,
                NULL as sql_path
            FROM __pgsqlite_schemas;
            "#,
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '30', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_schemata;"#,
            r#"DROP VIEW IF EXISTS pg_namespace;"#,
            r#"DROP TABLE IF EXISTS __pgsqlite_schemas;"#,
            r#"
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
            "#,
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_schemata AS
            SELECT 'main' as catalog_name, 'public' as schema_name, 'postgres' as schema_owner,
                   NULL as default_character_set_catalog, NULL as default_character_set_schema,
                   NULL as default_character_set_name, NULL as sql_path
            UNION ALL
            SELECT 'main', 'pg_catalog', 'postgres', NULL, NULL, NULL, NULL
            UNION ALL
            SELECT 'main', 'information_schema', 'postgres', NULL, NULL, NULL, NULL;
            "#,
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '29', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![29],
    });
}

/// Version 31: Persist SQL-language user functions and expose via pg_proc
pub(super) fn register_v31_user_sql_functions_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(31, Migration {
        version: 31,
        name: "user_sql_functions_support",
        description: "Persist SQL-language user functions and include them in pg_proc / routines introspection",
        up: MigrationAction::SqlBatch(&[
            r#"
            CREATE TABLE IF NOT EXISTS __pgsqlite_user_functions (
                schema_name TEXT NOT NULL,
                func_name TEXT NOT NULL,
                func_nargs INTEGER NOT NULL,
                func_kind TEXT NOT NULL DEFAULT 'f',
                func_strict TEXT NOT NULL DEFAULT 'f',
                func_retset TEXT NOT NULL DEFAULT 'f',
                func_volatile TEXT NOT NULL DEFAULT 'i',
                func_rettype INTEGER NOT NULL DEFAULT 25,
                arg_names TEXT NULL,      -- JSON array
                arg_types TEXT NULL,      -- JSON array
                body_expr TEXT NOT NULL,  -- scalar expression template (uses $1..$n)
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                PRIMARY KEY (schema_name, func_name, func_nargs)
            );
            "#,

            r#"
            UPDATE __pgsqlite_metadata
            SET value = '31', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            r#"DROP TABLE IF EXISTS __pgsqlite_user_functions;"#,
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '30', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![30],
    });
}

/// Version 32: Rebuild information_schema_routines view to include user SQL functions
pub(super) fn register_v32_information_schema_routines_user_functions(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(32, Migration {
        version: 32,
        name: "information_schema_routines_user_functions",
        description: "Include __pgsqlite_user_functions in information_schema.routines view",
        up: MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_routines;"#,
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_routines AS
            SELECT
                'main' as routine_catalog,
                schema_name as routine_schema,
                func_name as routine_name,
                'main' as specific_catalog,
                schema_name as specific_schema,
                func_name as specific_name,
                CASE func_kind
                    WHEN 'f' THEN 'FUNCTION'
                    WHEN 'a' THEN 'FUNCTION'
                    WHEN 'p' THEN 'PROCEDURE'
                    ELSE 'FUNCTION'
                END as routine_type,
                NULL as module_catalog,
                NULL as module_schema,
                NULL as module_name,
                NULL as udt_catalog,
                NULL as udt_schema,
                NULL as udt_name,
                CASE func_rettype
                    WHEN 23 THEN 'integer'
                    WHEN 25 THEN 'text'
                    WHEN 16 THEN 'boolean'
                    WHEN 20 THEN 'bigint'
                    WHEN 21 THEN 'smallint'
                    WHEN 700 THEN 'real'
                    WHEN 701 THEN 'double precision'
                    WHEN 1043 THEN 'character varying'
                    WHEN 1082 THEN 'date'
                    WHEN 1083 THEN 'time without time zone'
                    WHEN 1114 THEN 'timestamp without time zone'
                    WHEN 1184 THEN 'timestamp with time zone'
                    WHEN 1700 THEN 'numeric'
                    WHEN 114 THEN 'json'
                    WHEN 3802 THEN 'jsonb'
                    WHEN 2950 THEN 'uuid'
                    ELSE 'text'
                END as data_type,
                NULL as character_maximum_length,
                NULL as character_octet_length,
                NULL as character_set_catalog,
                NULL as character_set_schema,
                NULL as character_set_name,
                NULL as collation_catalog,
                NULL as collation_schema,
                NULL as collation_name,
                NULL as numeric_precision,
                NULL as numeric_precision_radix,
                NULL as numeric_scale,
                NULL as datetime_precision,
                NULL as interval_type,
                NULL as interval_precision,
                NULL as type_udt_catalog,
                NULL as type_udt_schema,
                NULL as type_udt_name,
                NULL as scope_catalog,
                NULL as scope_schema,
                NULL as scope_name,
                NULL as maximum_cardinality,
                NULL as dtd_identifier,
                'EXTERNAL' as routine_body,
                '' as routine_definition,
                NULL as external_name,
                'SQL' as external_language,
                'SQL' as parameter_style,
                'NO' as is_deterministic,
                'CONTAINS_SQL' as sql_data_access,
                NULL as is_null_call,
                NULL as sql_path,
                'YES' as schema_level_routine,
                0 as max_dynamic_result_sets,
                'NO' as is_user_defined_cast,
                'NO' as is_implicitly_invocable,
                'INVOKER' as security_type,
                NULL as to_sql_specific_catalog,
                NULL as to_sql_specific_schema,
                NULL as to_sql_specific_name,
                'NO' as as_locator,
                NULL as created,
                NULL as last_altered,
                NULL as new_savepoint_level,
                'NO' as is_udt_dependent,
                NULL as result_cast_from_data_type,
                NULL as result_cast_as_locator,
                NULL as result_cast_char_max_length,
                NULL as result_cast_char_octet_length,
                NULL as result_cast_char_set_catalog,
                NULL as result_cast_char_set_schema,
                NULL as result_cast_char_set_name,
                NULL as result_cast_collation_catalog,
                NULL as result_cast_collation_schema,
                NULL as result_cast_collation_name,
                NULL as result_cast_numeric_precision,
                NULL as result_cast_numeric_precision_radix,
                NULL as result_cast_numeric_scale,
                NULL as result_cast_datetime_precision,
                NULL as result_cast_interval_type,
                NULL as result_cast_interval_precision,
                NULL as result_cast_type_udt_catalog,
                NULL as result_cast_type_udt_schema,
                NULL as result_cast_type_udt_name,
                NULL as result_cast_scope_catalog,
                NULL as result_cast_scope_schema,
                NULL as result_cast_scope_name,
                NULL as result_cast_maximum_cardinality,
                NULL as result_cast_dtd_identifier
            FROM (
                SELECT 'pg_catalog' as schema_name, func_name, func_kind, func_strict, func_rettype
                FROM (
                    SELECT 'length' as func_name, 'f' as func_kind, 't' as func_strict, 23 as func_rettype
                    UNION ALL SELECT 'lower', 'f', 't', 25
                    UNION ALL SELECT 'upper', 'f', 't', 25
                    UNION ALL SELECT 'substr', 'f', 't', 25
                    UNION ALL SELECT 'replace', 'f', 't', 25
                    UNION ALL SELECT 'trim', 'f', 't', 25
                    UNION ALL SELECT 'ltrim', 'f', 't', 25
                    UNION ALL SELECT 'rtrim', 'f', 't', 25
                    UNION ALL SELECT 'count', 'a', 'f', 20
                    UNION ALL SELECT 'now', 'f', 'f', 1184
                    UNION ALL SELECT 'current_user', 'f', 'f', 19
                    UNION ALL SELECT 'current_database', 'f', 'f', 19
                    UNION ALL SELECT 'current_schema', 'f', 'f', 19
                    UNION ALL SELECT 'current_setting', 'f', 'f', 25
                    UNION ALL SELECT 'unaccent', 'f', 't', 25
                )

                UNION ALL
                SELECT schema_name, func_name, func_kind, func_strict, func_rettype
                FROM __pgsqlite_user_functions
            );
            "#,
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '32', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_routines;"#,
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '31', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![31],
    });
}

/// Version 33: Rebuild information_schema_routines view with full builtin list and user SQL functions
pub(super) fn register_v33_information_schema_routines_full_list(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(33, Migration {
        version: 33,
        name: "information_schema_routines_full_list",
        description: "Restore full builtin routines list and include user SQL functions",
        up: MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_routines;"#,
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_routines AS
            SELECT
                'main' as routine_catalog,
                schema_name as routine_schema,
                func_name as routine_name,
                'main' as specific_catalog,
                schema_name as specific_schema,
                func_name as specific_name,
                CASE func_kind
                    WHEN 'f' THEN 'FUNCTION'
                    WHEN 'a' THEN 'FUNCTION'
                    WHEN 'p' THEN 'PROCEDURE'
                    ELSE 'FUNCTION'
                END as routine_type,
                NULL as module_catalog,
                NULL as module_schema,
                NULL as module_name,
                NULL as udt_catalog,
                NULL as udt_schema,
                NULL as udt_name,
                CASE func_rettype
                    WHEN 23 THEN 'integer'
                    WHEN 25 THEN 'text'
                    WHEN 16 THEN 'boolean'
                    WHEN 20 THEN 'bigint'
                    WHEN 21 THEN 'smallint'
                    WHEN 700 THEN 'real'
                    WHEN 701 THEN 'double precision'
                    WHEN 1043 THEN 'character varying'
                    WHEN 1082 THEN 'date'
                    WHEN 1083 THEN 'time without time zone'
                    WHEN 1114 THEN 'timestamp without time zone'
                    WHEN 1184 THEN 'timestamp with time zone'
                    WHEN 1700 THEN 'numeric'
                    WHEN 114 THEN 'json'
                    WHEN 3802 THEN 'jsonb'
                    WHEN 2950 THEN 'uuid'
                    WHEN 3614 THEN 'tsvector'
                    WHEN 3615 THEN 'tsquery'
                    WHEN 2277 THEN 'anyarray'
                    WHEN 2283 THEN 'anyelement'
                    ELSE 'text'
                END as data_type,
                NULL as character_maximum_length,
                NULL as character_octet_length,
                NULL as character_set_catalog,
                NULL as character_set_schema,
                NULL as character_set_name,
                NULL as collation_catalog,
                NULL as collation_schema,
                NULL as collation_name,
                NULL as numeric_precision,
                NULL as numeric_precision_radix,
                NULL as numeric_scale,
                NULL as datetime_precision,
                NULL as interval_type,
                NULL as interval_precision,
                NULL as type_udt_catalog,
                NULL as type_udt_schema,
                NULL as type_udt_name,
                NULL as scope_catalog,
                NULL as scope_schema,
                NULL as scope_name,
                NULL as maximum_cardinality,
                NULL as dtd_identifier,
                'EXTERNAL' as routine_body,
                '' as routine_definition,
                NULL as external_name,
                'SQL' as external_language,
                'SQL' as parameter_style,
                CASE func_strict WHEN 't' THEN 'YES' ELSE 'NO' END as is_deterministic,
                'CONTAINS_SQL' as sql_data_access,
                CASE func_kind WHEN 'p' THEN 'YES' ELSE NULL END as is_null_call,
                NULL as sql_path,
                'YES' as schema_level_routine,
                0 as max_dynamic_result_sets,
                'NO' as is_user_defined_cast,
                'NO' as is_implicitly_invocable,
                'INVOKER' as security_type,
                NULL as to_sql_specific_catalog,
                NULL as to_sql_specific_schema,
                NULL as to_sql_specific_name,
                'NO' as as_locator,
                NULL as created,
                NULL as last_altered,
                NULL as new_savepoint_level,
                'NO' as is_udt_dependent,
                NULL as result_cast_from_data_type,
                NULL as result_cast_as_locator,
                NULL as result_cast_char_max_length,
                NULL as result_cast_char_octet_length,
                NULL as result_cast_char_set_catalog,
                NULL as result_cast_char_set_schema,
                NULL as result_cast_char_set_name,
                NULL as result_cast_collation_catalog,
                NULL as result_cast_collation_schema,
                NULL as result_cast_collation_name,
                NULL as result_cast_numeric_precision,
                NULL as result_cast_numeric_precision_radix,
                NULL as result_cast_numeric_scale,
                NULL as result_cast_datetime_precision,
                NULL as result_cast_interval_type,
                NULL as result_cast_interval_precision,
                NULL as result_cast_type_udt_catalog,
                NULL as result_cast_type_udt_schema,
                NULL as result_cast_type_udt_name,
                NULL as result_cast_scope_catalog,
                NULL as result_cast_scope_schema,
                NULL as result_cast_scope_name,
                NULL as result_cast_maximum_cardinality,
                NULL as result_cast_dtd_identifier
            FROM (
                SELECT 'pg_catalog' as schema_name, func_name, func_kind, func_strict, func_rettype
                FROM (
                    -- String functions
                    SELECT 'length' as func_name, 'f' as func_kind, 't' as func_strict, 23 as func_rettype
                    UNION ALL SELECT 'lower', 'f', 't', 25
                    UNION ALL SELECT 'upper', 'f', 't', 25
                    UNION ALL SELECT 'substr', 'f', 't', 25
                    UNION ALL SELECT 'substring', 'f', 't', 25
                    UNION ALL SELECT 'replace', 'f', 't', 25
                    UNION ALL SELECT 'trim', 'f', 't', 25
                    UNION ALL SELECT 'ltrim', 'f', 't', 25
                    UNION ALL SELECT 'rtrim', 'f', 't', 25
                    UNION ALL SELECT 'concat', 'f', 'f', 25
                    UNION ALL SELECT 'concat_ws', 'f', 'f', 25
                    UNION ALL SELECT 'left', 'f', 't', 25
                    UNION ALL SELECT 'right', 'f', 't', 25
                    UNION ALL SELECT 'repeat', 'f', 't', 25
                    UNION ALL SELECT 'reverse', 'f', 't', 25
                    UNION ALL SELECT 'split_part', 'f', 't', 25
                    UNION ALL SELECT 'string_agg', 'a', 'f', 25
                    UNION ALL SELECT 'translate', 'f', 't', 25
                    UNION ALL SELECT 'ascii', 'f', 't', 23
                    UNION ALL SELECT 'chr', 'f', 't', 25
                    UNION ALL SELECT 'initcap', 'f', 't', 25
                    UNION ALL SELECT 'lpad', 'f', 't', 25
                    UNION ALL SELECT 'rpad', 'f', 't', 25
                    UNION ALL SELECT 'position', 'f', 't', 23
                    UNION ALL SELECT 'strpos', 'f', 't', 23

                    -- Math functions
                    UNION ALL SELECT 'abs', 'f', 't', 23
                    UNION ALL SELECT 'round', 'f', 't', 1700
                    UNION ALL SELECT 'ceil', 'f', 't', 1700
                    UNION ALL SELECT 'ceiling', 'f', 't', 1700
                    UNION ALL SELECT 'floor', 'f', 't', 1700
                    UNION ALL SELECT 'trunc', 'f', 't', 1700
                    UNION ALL SELECT 'sqrt', 'f', 't', 701
                    UNION ALL SELECT 'power', 'f', 't', 701
                    UNION ALL SELECT 'exp', 'f', 't', 701
                    UNION ALL SELECT 'ln', 'f', 't', 701
                    UNION ALL SELECT 'log', 'f', 't', 701
                    UNION ALL SELECT 'mod', 'f', 't', 23
                    UNION ALL SELECT 'sign', 'f', 't', 23
                    UNION ALL SELECT 'random', 'f', 'f', 701
                    UNION ALL SELECT 'pi', 'f', 'f', 701
                    UNION ALL SELECT 'degrees', 'f', 't', 701
                    UNION ALL SELECT 'radians', 'f', 't', 701
                    UNION ALL SELECT 'sin', 'f', 't', 701
                    UNION ALL SELECT 'cos', 'f', 't', 701
                    UNION ALL SELECT 'tan', 'f', 't', 701
                    UNION ALL SELECT 'asin', 'f', 't', 701
                    UNION ALL SELECT 'acos', 'f', 't', 701
                    UNION ALL SELECT 'atan', 'f', 't', 701
                    UNION ALL SELECT 'atan2', 'f', 't', 701

                    -- Aggregate functions
                    UNION ALL SELECT 'count', 'a', 'f', 20
                    UNION ALL SELECT 'sum', 'a', 'f', 1700
                    UNION ALL SELECT 'avg', 'a', 'f', 1700
                    UNION ALL SELECT 'max', 'a', 'f', 2283
                    UNION ALL SELECT 'min', 'a', 'f', 2283
                    UNION ALL SELECT 'array_agg', 'a', 'f', 2277
                    UNION ALL SELECT 'bool_and', 'a', 'f', 16
                    UNION ALL SELECT 'bool_or', 'a', 'f', 16
                    UNION ALL SELECT 'every', 'a', 'f', 16
                    UNION ALL SELECT 'bit_and', 'a', 'f', 23
                    UNION ALL SELECT 'bit_or', 'a', 'f', 23

                    -- Date/time functions
                    UNION ALL SELECT 'now', 'f', 'f', 1184
                    UNION ALL SELECT 'current_timestamp', 'f', 'f', 1184
                    UNION ALL SELECT 'current_date', 'f', 'f', 1082
                    UNION ALL SELECT 'current_time', 'f', 'f', 1083
                    UNION ALL SELECT 'localtime', 'f', 'f', 1083
                    UNION ALL SELECT 'localtimestamp', 'f', 'f', 1114
                    UNION ALL SELECT 'date', 'f', 't', 1082
                    UNION ALL SELECT 'time', 'f', 't', 1083
                    UNION ALL SELECT 'timestamp', 'f', 't', 1114
                    UNION ALL SELECT 'extract', 'f', 't', 701
                    UNION ALL SELECT 'date_part', 'f', 't', 701
                    UNION ALL SELECT 'date_trunc', 'f', 't', 1184
                    UNION ALL SELECT 'age', 'f', 't', 1186
                    UNION ALL SELECT 'to_char', 'f', 't', 25
                    UNION ALL SELECT 'to_date', 'f', 't', 1082
                    UNION ALL SELECT 'to_timestamp', 'f', 't', 1184
                    UNION ALL SELECT 'make_date', 'f', 't', 1082
                    UNION ALL SELECT 'make_time', 'f', 't', 1083
                    UNION ALL SELECT 'make_timestamp', 'f', 't', 1114
                    UNION ALL SELECT 'make_timestamptz', 'f', 't', 1184

                    -- JSON functions
                    UNION ALL SELECT 'json_agg', 'a', 'f', 114
                    UNION ALL SELECT 'jsonb_agg', 'a', 'f', 3802
                    UNION ALL SELECT 'json_object_agg', 'a', 'f', 114
                    UNION ALL SELECT 'jsonb_object_agg', 'a', 'f', 3802
                    UNION ALL SELECT 'to_json', 'f', 't', 114
                    UNION ALL SELECT 'to_jsonb', 'f', 't', 3802
                    UNION ALL SELECT 'row_to_json', 'f', 't', 114
                    UNION ALL SELECT 'json_build_array', 'f', 'f', 114
                    UNION ALL SELECT 'jsonb_build_array', 'f', 'f', 3802
                    UNION ALL SELECT 'json_build_object', 'f', 'f', 114
                    UNION ALL SELECT 'jsonb_build_object', 'f', 'f', 3802
                    UNION ALL SELECT 'json_extract_path', 'f', 't', 114
                    UNION ALL SELECT 'jsonb_extract_path', 'f', 't', 3802
                    UNION ALL SELECT 'json_extract_path_text', 'f', 't', 25
                    UNION ALL SELECT 'jsonb_extract_path_text', 'f', 't', 25
                    UNION ALL SELECT 'json_array_length', 'f', 't', 23
                    UNION ALL SELECT 'jsonb_array_length', 'f', 't', 23
                    UNION ALL SELECT 'json_typeof', 'f', 't', 25
                    UNION ALL SELECT 'jsonb_typeof', 'f', 't', 25
                    UNION ALL SELECT 'jsonb_set', 'f', 't', 3802
                    UNION ALL SELECT 'jsonb_insert', 'f', 't', 3802
                    UNION ALL SELECT 'jsonb_delete', 'f', 't', 3802
                    UNION ALL SELECT 'jsonb_pretty', 'f', 't', 25
                    UNION ALL SELECT 'json_each', 'f', 't', 2249
                    UNION ALL SELECT 'jsonb_each', 'f', 't', 2249
                    UNION ALL SELECT 'json_each_text', 'f', 't', 2249
                    UNION ALL SELECT 'jsonb_each_text', 'f', 't', 2249
                    UNION ALL SELECT 'json_array_elements', 'f', 't', 114
                    UNION ALL SELECT 'jsonb_array_elements', 'f', 't', 3802
                    UNION ALL SELECT 'json_array_elements_text', 'f', 't', 25
                    UNION ALL SELECT 'jsonb_array_elements_text', 'f', 't', 25
                    UNION ALL SELECT 'json_object_keys', 'f', 't', 25
                    UNION ALL SELECT 'jsonb_object_keys', 'f', 't', 25
                    UNION ALL SELECT 'json_populate_record', 'f', 't', 2249
                    UNION ALL SELECT 'jsonb_populate_record', 'f', 't', 2249
                    UNION ALL SELECT 'json_to_record', 'f', 't', 2249
                    UNION ALL SELECT 'jsonb_to_record', 'f', 't', 2249
                    UNION ALL SELECT 'json_strip_nulls', 'f', 't', 114
                    UNION ALL SELECT 'jsonb_strip_nulls', 'f', 't', 3802

                    -- Array functions
                    UNION ALL SELECT 'unnest', 'f', 'f', 2283
                    UNION ALL SELECT 'array_length', 'f', 't', 23
                    UNION ALL SELECT 'array_dims', 'f', 't', 25
                    UNION ALL SELECT 'array_lower', 'f', 't', 23
                    UNION ALL SELECT 'array_upper', 'f', 't', 23
                    UNION ALL SELECT 'array_ndims', 'f', 't', 23
                    UNION ALL SELECT 'array_position', 'f', 't', 23
                    UNION ALL SELECT 'array_positions', 'f', 't', 1007
                    UNION ALL SELECT 'array_remove', 'f', 't', 2277
                    UNION ALL SELECT 'array_replace', 'f', 't', 2277
                    UNION ALL SELECT 'array_cat', 'f', 't', 2277
                    UNION ALL SELECT 'array_append', 'f', 't', 2277
                    UNION ALL SELECT 'array_prepend', 'f', 't', 2277
                    UNION ALL SELECT 'array_to_string', 'f', 't', 25
                    UNION ALL SELECT 'string_to_array', 'f', 't', 1009
                    UNION ALL SELECT 'cardinality', 'f', 't', 23

                    -- UUID functions
                    UNION ALL SELECT 'gen_random_uuid', 'f', 'f', 2950
                    UNION ALL SELECT 'uuid_generate_v1', 'f', 'f', 2950
                    UNION ALL SELECT 'uuid_generate_v1mc', 'f', 'f', 2950
                    UNION ALL SELECT 'uuid_generate_v3', 'f', 't', 2950
                    UNION ALL SELECT 'uuid_generate_v4', 'f', 'f', 2950
                    UNION ALL SELECT 'uuid_generate_v5', 'f', 't', 2950
                    UNION ALL SELECT 'uuid_nil', 'f', 'f', 2950
                    UNION ALL SELECT 'uuid_ns_dns', 'f', 'f', 2950
                    UNION ALL SELECT 'uuid_ns_url', 'f', 'f', 2950
                    UNION ALL SELECT 'uuid_ns_oid', 'f', 'f', 2950
                    UNION ALL SELECT 'uuid_ns_x500', 'f', 'f', 2950

                    -- unaccent
                    UNION ALL SELECT 'unaccent', 'f', 't', 25

                    -- Full-text search
                    UNION ALL SELECT 'to_tsvector', 'f', 't', 3614
                    UNION ALL SELECT 'to_tsquery', 'f', 't', 3615
                    UNION ALL SELECT 'plainto_tsquery', 'f', 't', 3615
                    UNION ALL SELECT 'phraseto_tsquery', 'f', 't', 3615
                    UNION ALL SELECT 'websearch_to_tsquery', 'f', 't', 3615
                    UNION ALL SELECT 'ts_headline', 'f', 't', 25
                    UNION ALL SELECT 'ts_rank', 'f', 't', 700
                    UNION ALL SELECT 'ts_rank_cd', 'f', 't', 700

                    -- System/session functions
                    UNION ALL SELECT 'version', 'f', 'f', 25
                    UNION ALL SELECT 'current_database', 'f', 'f', 19
                    UNION ALL SELECT 'current_user', 'f', 'f', 19
                    UNION ALL SELECT 'session_user', 'f', 'f', 19
                    UNION ALL SELECT 'current_schema', 'f', 'f', 19
                    UNION ALL SELECT 'current_schemas', 'f', 'f', 1003
                    UNION ALL SELECT 'current_setting', 'f', 'f', 25
                    UNION ALL SELECT 'set_config', 'f', 't', 25
                    UNION ALL SELECT 'pg_backend_pid', 'f', 'f', 23
                    UNION ALL SELECT 'pg_is_in_recovery', 'f', 'f', 16
                    UNION ALL SELECT 'pg_postmaster_start_time', 'f', 'f', 1184
                    UNION ALL SELECT 'pg_conf_load_time', 'f', 'f', 1184
                    UNION ALL SELECT 'inet_server_addr', 'f', 'f', 869
                    UNION ALL SELECT 'inet_server_port', 'f', 'f', 23
                    UNION ALL SELECT 'inet_client_addr', 'f', 'f', 869
                    UNION ALL SELECT 'inet_client_port', 'f', 'f', 23

                    -- PostgreSQL catalog functions
                    UNION ALL SELECT 'pg_has_role', 'f', 'f', 16
                    UNION ALL SELECT 'has_table_privilege', 'f', 'f', 16
                    UNION ALL SELECT 'has_schema_privilege', 'f', 'f', 16
                    UNION ALL SELECT 'has_database_privilege', 'f', 'f', 16
                    UNION ALL SELECT 'pg_table_is_visible', 'f', 't', 16
                    UNION ALL SELECT 'pg_type_is_visible', 'f', 't', 16
                    UNION ALL SELECT 'pg_function_is_visible', 'f', 't', 16
                    UNION ALL SELECT 'pg_get_constraintdef', 'f', 't', 25
                    UNION ALL SELECT 'pg_get_indexdef', 'f', 't', 25
                    UNION ALL SELECT 'pg_get_viewdef', 'f', 't', 25
                    UNION ALL SELECT 'pg_get_triggerdef', 'f', 't', 25
                    UNION ALL SELECT 'pg_get_expr', 'f', 't', 25
                    UNION ALL SELECT 'pg_get_userbyid', 'f', 't', 19
                    UNION ALL SELECT 'format_type', 'f', 't', 25
                    UNION ALL SELECT 'to_regtype', 'f', 't', 2206
                    UNION ALL SELECT 'to_regclass', 'f', 't', 2205
                    UNION ALL SELECT 'to_regproc', 'f', 't', 24
                    UNION ALL SELECT 'pg_typeof', 'f', 't', 2206
                    UNION ALL SELECT 'pg_column_size', 'f', 't', 23
                    UNION ALL SELECT 'pg_database_size', 'f', 't', 20
                    UNION ALL SELECT 'pg_table_size', 'f', 't', 20
                    UNION ALL SELECT 'pg_total_relation_size', 'f', 't', 20
                    UNION ALL SELECT 'pg_size_pretty', 'f', 't', 25
                    UNION ALL SELECT 'pg_relation_size', 'f', 't', 20
                    UNION ALL SELECT 'pg_indexes_size', 'f', 't', 20

                    -- Coalesce/nullif/case
                    UNION ALL SELECT 'coalesce', 'f', 'f', 2283
                    UNION ALL SELECT 'nullif', 'f', 't', 2283
                    UNION ALL SELECT 'greatest', 'f', 'f', 2283
                    UNION ALL SELECT 'least', 'f', 'f', 2283

                    -- Type casting
                    UNION ALL SELECT 'cast', 'f', 't', 2283

                    -- Boolean
                    UNION ALL SELECT 'bool', 'f', 't', 16
                )

                UNION ALL
                SELECT schema_name, func_name, func_kind, func_strict, func_rettype
                FROM __pgsqlite_user_functions
            );
            "#,
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '33', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_routines;"#,
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_routines AS
            SELECT
                'main' as routine_catalog,
                schema_name as routine_schema,
                func_name as routine_name,
                'main' as specific_catalog,
                schema_name as specific_schema,
                func_name as specific_name,
                CASE func_kind
                    WHEN 'f' THEN 'FUNCTION'
                    WHEN 'a' THEN 'FUNCTION'
                    WHEN 'p' THEN 'PROCEDURE'
                    ELSE 'FUNCTION'
                END as routine_type,
                NULL as module_catalog,
                NULL as module_schema,
                NULL as module_name,
                NULL as udt_catalog,
                NULL as udt_schema,
                NULL as udt_name,
                CASE func_rettype
                    WHEN 23 THEN 'integer'
                    WHEN 25 THEN 'text'
                    WHEN 16 THEN 'boolean'
                    WHEN 20 THEN 'bigint'
                    WHEN 21 THEN 'smallint'
                    WHEN 700 THEN 'real'
                    WHEN 701 THEN 'double precision'
                    WHEN 1043 THEN 'character varying'
                    WHEN 1082 THEN 'date'
                    WHEN 1083 THEN 'time without time zone'
                    WHEN 1114 THEN 'timestamp without time zone'
                    WHEN 1184 THEN 'timestamp with time zone'
                    WHEN 1700 THEN 'numeric'
                    WHEN 114 THEN 'json'
                    WHEN 3802 THEN 'jsonb'
                    WHEN 2950 THEN 'uuid'
                    ELSE 'text'
                END as data_type,
                NULL as character_maximum_length,
                NULL as character_octet_length,
                NULL as character_set_catalog,
                NULL as character_set_schema,
                NULL as character_set_name,
                NULL as collation_catalog,
                NULL as collation_schema,
                NULL as collation_name,
                NULL as numeric_precision,
                NULL as numeric_precision_radix,
                NULL as numeric_scale,
                NULL as datetime_precision,
                NULL as interval_type,
                NULL as interval_precision,
                NULL as type_udt_catalog,
                NULL as type_udt_schema,
                NULL as type_udt_name,
                NULL as scope_catalog,
                NULL as scope_schema,
                NULL as scope_name,
                NULL as maximum_cardinality,
                NULL as dtd_identifier,
                'EXTERNAL' as routine_body,
                '' as routine_definition,
                NULL as external_name,
                'SQL' as external_language,
                'SQL' as parameter_style,
                'NO' as is_deterministic,
                'CONTAINS_SQL' as sql_data_access,
                NULL as is_null_call,
                NULL as sql_path,
                'YES' as schema_level_routine,
                0 as max_dynamic_result_sets,
                'NO' as is_user_defined_cast,
                'NO' as is_implicitly_invocable,
                'INVOKER' as security_type,
                NULL as to_sql_specific_catalog,
                NULL as to_sql_specific_schema,
                NULL as to_sql_specific_name,
                'NO' as as_locator,
                NULL as created,
                NULL as last_altered,
                NULL as new_savepoint_level,
                'NO' as is_udt_dependent,
                NULL as result_cast_from_data_type,
                NULL as result_cast_as_locator,
                NULL as result_cast_char_max_length,
                NULL as result_cast_char_octet_length,
                NULL as result_cast_char_set_catalog,
                NULL as result_cast_char_set_schema,
                NULL as result_cast_char_set_name,
                NULL as result_cast_collation_catalog,
                NULL as result_cast_collation_schema,
                NULL as result_cast_collation_name,
                NULL as result_cast_numeric_precision,
                NULL as result_cast_numeric_precision_radix,
                NULL as result_cast_numeric_scale,
                NULL as result_cast_datetime_precision,
                NULL as result_cast_interval_type,
                NULL as result_cast_interval_precision,
                NULL as result_cast_type_udt_catalog,
                NULL as result_cast_type_udt_schema,
                NULL as result_cast_type_udt_name,
                NULL as result_cast_scope_catalog,
                NULL as result_cast_scope_schema,
                NULL as result_cast_scope_name,
                NULL as result_cast_maximum_cardinality,
                NULL as result_cast_dtd_identifier
            FROM (
                SELECT 'pg_catalog' as schema_name, func_name, func_kind, func_strict, func_rettype
                FROM (
                    SELECT 'length' as func_name, 'f' as func_kind, 't' as func_strict, 23 as func_rettype
                    UNION ALL SELECT 'lower', 'f', 't', 25
                    UNION ALL SELECT 'upper', 'f', 't', 25
                    UNION ALL SELECT 'substr', 'f', 't', 25
                    UNION ALL SELECT 'replace', 'f', 't', 25
                    UNION ALL SELECT 'trim', 'f', 't', 25
                    UNION ALL SELECT 'ltrim', 'f', 't', 25
                    UNION ALL SELECT 'rtrim', 'f', 't', 25
                    UNION ALL SELECT 'count', 'a', 'f', 20
                    UNION ALL SELECT 'now', 'f', 'f', 1184
                    UNION ALL SELECT 'current_user', 'f', 'f', 19
                    UNION ALL SELECT 'current_database', 'f', 'f', 19
                    UNION ALL SELECT 'current_schema', 'f', 'f', 19
                    UNION ALL SELECT 'current_setting', 'f', 'f', 25
                    UNION ALL SELECT 'unaccent', 'f', 't', 25
                )

                UNION ALL
                SELECT schema_name, func_name, func_kind, func_strict, func_rettype
                FROM __pgsqlite_user_functions
            );
            "#,
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '32', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![32],
    });
}

/// Version 34: Add nulls_distinct column to information_schema.table_constraints view
pub(super) fn register_v34_table_constraints_nulls_distinct(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(34, Migration {
        version: 34,
        name: "information_schema_table_constraints_nulls_distinct",
        description: "Add nulls_distinct column to information_schema.table_constraints",
        up: MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_table_constraints;"#,
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
                CASE WHEN con.convalidated THEN 'YES' ELSE 'NO' END as enforced,
                'YES' as nulls_distinct
            FROM pg_constraint con
            JOIN pg_class c ON con.conrelid = c.oid;
            "#,
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '34', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_table_constraints;"#,
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
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '33', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![33],
    });
}
