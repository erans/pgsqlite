use super::{Migration, MigrationAction};
use std::collections::BTreeMap;

pub(super) fn register_v15_pg_depend_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(15, Migration {
        version: 15,
        name: "pg_depend_support",
        description: "Add pg_depend view for Rails sequence ownership detection and ORM compatibility",
        up: MigrationAction::SqlBatch(&[
            // Create pg_depend table for storing object dependencies
            r#"
            CREATE TABLE IF NOT EXISTS pg_depend (
                classid TEXT NOT NULL,      -- OID of system catalog (e.g., '1259' for pg_class)
                objid TEXT NOT NULL,        -- OID of dependent object
                objsubid INTEGER NOT NULL,  -- Column number for table dependencies, 0 otherwise
                refclassid TEXT NOT NULL,   -- OID of system catalog where referenced object is listed
                refobjid TEXT NOT NULL,     -- OID of referenced object
                refobjsubid INTEGER NOT NULL, -- Column number for referenced object
                deptype CHAR(1) NOT NULL,   -- Dependency type: 'a' = automatic, 'n' = normal, etc.
                PRIMARY KEY (classid, objid, objsubid, refclassid, refobjid, refobjsubid)
            );
            "#,

            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '15', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- Remove pg_depend table
            DROP TABLE IF EXISTS pg_depend;

            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '14', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![14],
    });
}

/// Version 16: pg_proc support for function introspection and \df command
pub(super) fn register_v16_pg_proc_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(16, Migration {
        version: 16,
        name: "pg_proc_support",
        description: "Add pg_proc view for function introspection, \\df command, and complete ORM compatibility",
        up: MigrationAction::SqlBatch(&[
            // Create pg_proc view with essential function metadata
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
                NULL as proargdefaults,                         -- Argument defaults
                NULL as protrftypes,                            -- Transform types
                '' as prosrc,                                   -- Function source
                NULL as probin,                                 -- Object file
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
                UNION ALL SELECT 'version', 'f', 'f', 'f', 's', 25        -- text
                UNION ALL SELECT 'current_database', 'f', 'f', 'f', 's', 19  -- name
                UNION ALL SELECT 'current_user', 'f', 'f', 'f', 's', 19   -- name
                UNION ALL SELECT 'pg_table_is_visible', 'f', 't', 'f', 's', 16  -- bool
                UNION ALL SELECT 'format_type', 'f', 't', 'f', 'i', 25    -- text
                UNION ALL SELECT 'pg_get_constraintdef', 'f', 't', 'f', 's', 25  -- text
            );
            "#,

            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '16', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- Remove pg_proc view
            DROP VIEW IF EXISTS pg_proc;

            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '15', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![15],
    });
}

/// Version 17: pg_description support for object comments and documentation
pub(super) fn register_v17_pg_description_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(17, Migration {
        version: 17,
        name: "pg_description_support",
        description: "Add pg_description view for object comments, table/column documentation, and complete ORM compatibility",
        up: MigrationAction::SqlBatch(&[
            // Create pg_description view with object comment metadata
            r#"
            CREATE VIEW IF NOT EXISTS pg_description AS
            SELECT
                objoid,
                classoid,
                objsubid,
                description
            FROM (
                -- Table comments (objsubid = 0)
                SELECT
                    object_oid as objoid,
                    1259 as classoid,                                  -- pg_class OID
                    subobject_id as objsubid,                          -- 0 for table itself
                    comment_text as description
                FROM __pgsqlite_comments
                WHERE catalog_name = 'pg_class' AND subobject_id = 0

                UNION ALL

                -- Column comments (objsubid = column number)
                SELECT
                    object_oid as objoid,
                    1259 as classoid,                                  -- pg_class OID
                    subobject_id as objsubid,                          -- Column number
                    comment_text as description
                FROM __pgsqlite_comments
                WHERE catalog_name = 'pg_class' AND subobject_id > 0

                UNION ALL

                -- Function comments (objsubid = 0)
                SELECT
                    object_oid as objoid,
                    1255 as classoid,                                  -- pg_proc OID
                    subobject_id as objsubid,                          -- 0 for function itself
                    comment_text as description
                FROM __pgsqlite_comments
                WHERE catalog_name = 'pg_proc'
            )
            WHERE description IS NOT NULL AND description != '';
            "#,

            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '17', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- Remove pg_description view
            DROP VIEW IF EXISTS pg_description;

            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '16', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![16],
    });
}

/// Version 18: Add pg_roles and pg_user support
pub(super) fn register_v18_pg_roles_user_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(18, Migration {
        version: 18,
        name: "pg_roles_user_support",
        description: "Add PostgreSQL pg_roles and pg_user views for user and role management",
        up: MigrationAction::SqlBatch(&[
            // Create pg_roles view for role information
            r#"
            CREATE VIEW IF NOT EXISTS pg_roles AS
            SELECT
                10 as oid,
                'postgres' as rolname,
                't' as rolsuper,
                't' as rolinherit,
                't' as rolcreaterole,
                't' as rolcreatedb,
                't' as rolcanlogin,
                't' as rolreplication,
                -1 as rolconnlimit,
                '********' as rolpassword,
                NULL as rolvaliduntil,
                't' as rolbypassrls,
                NULL as rolconfig
            UNION ALL
            SELECT
                0 as oid,
                'public' as rolname,
                'f' as rolsuper,
                't' as rolinherit,
                'f' as rolcreaterole,
                'f' as rolcreatedb,
                'f' as rolcanlogin,
                'f' as rolreplication,
                -1 as rolconnlimit,
                NULL as rolpassword,
                NULL as rolvaliduntil,
                'f' as rolbypassrls,
                NULL as rolconfig
            UNION ALL
            SELECT
                100 as oid,
                'pgsqlite_user' as rolname,
                't' as rolsuper,
                't' as rolinherit,
                't' as rolcreaterole,
                't' as rolcreatedb,
                't' as rolcanlogin,
                'f' as rolreplication,
                -1 as rolconnlimit,
                '********' as rolpassword,
                NULL as rolvaliduntil,
                't' as rolbypassrls,
                NULL as rolconfig;
            "#,
            // Create pg_user view for user information
            r#"
            CREATE VIEW IF NOT EXISTS pg_user AS
            SELECT
                'postgres' as usename,
                10 as usesysid,
                't' as usecreatedb,
                't' as usesuper,
                't' as userepl,
                't' as usebypassrls,
                '********' as passwd,
                NULL as valuntil,
                NULL as useconfig
            UNION ALL
            SELECT
                'pgsqlite_user' as usename,
                100 as usesysid,
                't' as usecreatedb,
                't' as usesuper,
                'f' as userepl,
                't' as usebypassrls,
                '********' as passwd,
                NULL as valuntil,
                NULL as useconfig;
            "#,
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '18', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- Remove pg_roles and pg_user views
            DROP VIEW IF EXISTS pg_roles;
            DROP VIEW IF EXISTS pg_user;

            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '17', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![17],
    });
}

/// Version 19: Add pg_stats support for query optimization hints
pub(super) fn register_v19_pg_stats_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(19, Migration {
        version: 19,
        name: "pg_stats_support",
        description: "Add PostgreSQL pg_stats view for query optimization and performance hints",
        up: MigrationAction::SqlBatch(&[
            // Note: pg_stats is handled by the catalog interceptor, no SQLite view needed
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '19', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- pg_stats is handled by catalog interceptor, no view to remove
            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '18', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![18],
    });
}

/// Version 20: Add information_schema.routines support for function metadata
pub(super) fn register_v20_information_schema_routines_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(20, Migration {
        version: 20,
        name: "information_schema_routines_support",
        description: "Add PostgreSQL information_schema.routines view for function and procedure metadata",
        up: MigrationAction::SqlBatch(&[
            // Note: information_schema.routines is handled by the catalog interceptor, no SQLite view needed
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '20', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- information_schema.routines is handled by catalog interceptor, no view to remove
            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '19', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![19],
    });
}

/// Version 21: Add information_schema.views support for view metadata
pub(super) fn register_v21_information_schema_views_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(21, Migration {
        version: 21,
        name: "information_schema_views_support",
        description: "Add PostgreSQL information_schema.views view for view metadata and introspection",
        up: MigrationAction::SqlBatch(&[
            // Note: information_schema.views is handled by the catalog interceptor, no SQLite view needed
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '21', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- information_schema.views is handled by catalog interceptor, no view to remove
            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '20', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![20],
    });
}

pub(super) fn register_v22_information_schema_referential_constraints_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(22, Migration {
        version: 22,
        name: "information_schema_referential_constraints_support",
        description: "Add PostgreSQL information_schema.referential_constraints view for foreign key constraint metadata and introspection",
        up: MigrationAction::SqlBatch(&[
            // Note: information_schema.referential_constraints is handled by the catalog interceptor, no SQLite view needed
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '22', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- information_schema.referential_constraints is handled by catalog interceptor, no view to remove
            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '21', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![21],
    });
}

/// Version 23: information_schema.check_constraints support
pub(super) fn register_v23_information_schema_check_constraints_support(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(23, Migration {
        version: 23,
        name: "information_schema_check_constraints",
        description: "Add PostgreSQL information_schema.check_constraints view for check constraint metadata and introspection",
        up: MigrationAction::SqlBatch(&[
            // Note: information_schema.check_constraints is handled by the catalog interceptor, no SQLite view needed
            // Update schema version
            r#"
            UPDATE __pgsqlite_metadata
            SET value = '23', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::Sql(r#"
            -- information_schema.check_constraints is handled by catalog interceptor, no view to remove
            -- Restore schema version
            UPDATE __pgsqlite_metadata
            SET value = '22', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
        "#)),
        dependencies: vec![22],
    });
}
