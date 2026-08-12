//! Exact names of the relations pgsqlite's own migrations create.
//!
//! Prefix matching is not sufficient in either direction: it claims user tables
//! that merely start with `pg_`, and it misses the `idx_*` indexes migrations
//! create on `__pgsqlite_*` tables. Both failure modes are user-visible, so this
//! list is exact and is guarded against drift by
//! `tests/information_schema_namespace_test.rs::internal_relation_list_matches_migrated_database`.
//!
//! Adding a catalog relation to `src/migration/registry.rs` means adding it here.

/// Namespace OIDs, fixed by the `pg_namespace` view in migration v28.
pub const PG_CATALOG_NAMESPACE: i32 = 11;
pub const PUBLIC_NAMESPACE: i32 = 2200;
pub const INFORMATION_SCHEMA_NAMESPACE: i32 = 13000;

/// Relations pgsqlite creates that PostgreSQL keeps in `pg_catalog`.
pub const INTERNAL_PG_CATALOG_RELATIONS: &[&str] = &[
    // Real tables
    "pg_attrdef",
    "pg_constraint",
    "pg_depend",
    "pg_index",
    // Views
    "pg_am",
    "pg_attribute",
    "pg_class",
    "pg_database",
    "pg_description",
    "pg_enum",
    "pg_foreign_data_wrapper",
    "pg_namespace",
    "pg_proc",
    "pg_roles",
    "pg_stat_activity",
    "pg_stat_all_indexes",
    "pg_stat_all_tables",
    "pg_stat_database",
    "pg_stat_user_indexes",
    "pg_stat_user_tables",
    "pg_type",
    "pg_user",
    // Indexes on __pgsqlite_* tables. Not named __pgsqlite_*, so no prefix
    // filter catches them; they leak into \di today (#102).
    "idx_array_types_table",
    "idx_comments_lookup",
    "idx_datetime_cache_table",
    "idx_enum_values_label",
    "idx_enum_values_type",
    "idx_fts_metadata_table",
    "idx_numeric_constraints_table",
    "idx_string_constraints_table",
];

/// Relations pgsqlite creates that PostgreSQL keeps in `information_schema`.
pub const INTERNAL_INFORMATION_SCHEMA_RELATIONS: &[&str] = &[
    "information_schema_columns",
    "information_schema_key_column_usage",
    "information_schema_referential_constraints",
    "information_schema_schemata",
    "information_schema_table_constraints",
    "information_schema_tables",
];

/// Namespace OID for a relation name. Anything unlisted is a user relation.
///
/// Unlisted defaults to `public` deliberately: the failure mode is showing an
/// internal relation, never hiding a user's table.
pub fn relnamespace(name: &str) -> i32 {
    if INTERNAL_PG_CATALOG_RELATIONS.contains(&name) {
        PG_CATALOG_NAMESPACE
    } else if INTERNAL_INFORMATION_SCHEMA_RELATIONS.contains(&name) {
        INFORMATION_SCHEMA_NAMESPACE
    } else {
        PUBLIC_NAMESPACE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_pg_relations_map_to_pg_catalog() {
        assert_eq!(relnamespace("pg_class"), PG_CATALOG_NAMESPACE);
        assert_eq!(relnamespace("pg_constraint"), PG_CATALOG_NAMESPACE);
        assert_eq!(relnamespace("idx_enum_values_label"), PG_CATALOG_NAMESPACE);
    }

    #[test]
    fn internal_information_schema_relations_map_to_information_schema() {
        assert_eq!(relnamespace("information_schema_tables"), INFORMATION_SCHEMA_NAMESPACE);
        assert_eq!(relnamespace("information_schema_columns"), INFORMATION_SCHEMA_NAMESPACE);
    }

    #[test]
    fn user_tables_map_to_public() {
        assert_eq!(relnamespace("customers"), PUBLIC_NAMESPACE);
    }

    /// The reason this is an exact-name list rather than a `LIKE 'pg\_%'` test:
    /// a user table whose name merely starts with `pg_` is not ours. See #102.
    #[test]
    fn user_tables_named_like_catalog_relations_map_to_public() {
        assert_eq!(relnamespace("pg_myreport"), PUBLIC_NAMESPACE);
        assert_eq!(relnamespace("information_schema_export"), PUBLIC_NAMESPACE);
        assert_eq!(relnamespace("idx_customers_email"), PUBLIC_NAMESPACE);
    }

    #[test]
    fn lists_have_no_duplicates_and_do_not_overlap() {
        let mut all: Vec<&str> = INTERNAL_PG_CATALOG_RELATIONS
            .iter()
            .chain(INTERNAL_INFORMATION_SCHEMA_RELATIONS.iter())
            .copied()
            .collect();
        let total = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), total, "duplicate entry in the internal relation lists");
    }

    #[test]
    fn udf_is_callable_from_sql() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::functions::register_all_functions(&conn).unwrap();

        let ns: i32 = conn
            .query_row("SELECT __pgsqlite_relnamespace('pg_class')", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ns, PG_CATALOG_NAMESPACE);

        let ns: i32 = conn
            .query_row("SELECT __pgsqlite_relnamespace('pg_myreport')", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ns, PUBLIC_NAMESPACE);

        let ns: Option<i32> = conn
            .query_row("SELECT __pgsqlite_relnamespace(NULL)", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ns, None);
    }
}
