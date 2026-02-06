use crate::protocol::FieldDescription;

pub struct ColumnSpec {
    pub name: &'static str,
    pub type_oid: i32,
}

const PG_DATABASE_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "oid", type_oid: 23 },
    ColumnSpec { name: "datname", type_oid: 25 },
    ColumnSpec { name: "datdba", type_oid: 23 },
    ColumnSpec { name: "encoding", type_oid: 23 },
    ColumnSpec { name: "datlocprovider", type_oid: 25 },
    ColumnSpec { name: "datistemplate", type_oid: 25 },
    ColumnSpec { name: "datallowconn", type_oid: 25 },
    ColumnSpec { name: "dathasloginevt", type_oid: 25 },
    ColumnSpec { name: "datconnlimit", type_oid: 23 },
    ColumnSpec { name: "datfrozenxid", type_oid: 25 },
    ColumnSpec { name: "datminmxid", type_oid: 25 },
    ColumnSpec { name: "dattablespace", type_oid: 23 },
    ColumnSpec { name: "datcollate", type_oid: 25 },
    ColumnSpec { name: "datctype", type_oid: 25 },
    ColumnSpec { name: "datlocale", type_oid: 25 },
    ColumnSpec { name: "daticurules", type_oid: 25 },
    ColumnSpec { name: "datcollversion", type_oid: 25 },
    ColumnSpec { name: "datacl", type_oid: 25 },
];

const PG_CLASS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "oid", type_oid: 26 },
    ColumnSpec { name: "relname", type_oid: 25 },
    ColumnSpec { name: "relnamespace", type_oid: 26 },
    ColumnSpec { name: "reltype", type_oid: 26 },
    ColumnSpec { name: "reloftype", type_oid: 26 },
    ColumnSpec { name: "relowner", type_oid: 26 },
    ColumnSpec { name: "relam", type_oid: 26 },
    ColumnSpec { name: "relfilenode", type_oid: 26 },
    ColumnSpec { name: "reltablespace", type_oid: 26 },
    ColumnSpec { name: "relpages", type_oid: 23 },
    ColumnSpec { name: "reltuples", type_oid: 700 },
    ColumnSpec { name: "relallvisible", type_oid: 23 },
    ColumnSpec { name: "reltoastrelid", type_oid: 26 },
    ColumnSpec { name: "relhasindex", type_oid: 16 },
    ColumnSpec { name: "relisshared", type_oid: 16 },
    ColumnSpec { name: "relpersistence", type_oid: 18 },
    ColumnSpec { name: "relkind", type_oid: 18 },
    ColumnSpec { name: "relnatts", type_oid: 21 },
    ColumnSpec { name: "relchecks", type_oid: 21 },
    ColumnSpec { name: "relhasrules", type_oid: 16 },
    ColumnSpec { name: "relhastriggers", type_oid: 16 },
    ColumnSpec { name: "relhassubclass", type_oid: 16 },
    ColumnSpec { name: "relrowsecurity", type_oid: 16 },
    ColumnSpec { name: "relforcerowsecurity", type_oid: 16 },
    ColumnSpec { name: "relispopulated", type_oid: 16 },
    ColumnSpec { name: "relreplident", type_oid: 18 },
    ColumnSpec { name: "relispartition", type_oid: 16 },
    ColumnSpec { name: "relrewrite", type_oid: 26 },
    ColumnSpec { name: "relfrozenxid", type_oid: 28 },
    ColumnSpec { name: "relminmxid", type_oid: 28 },
    ColumnSpec { name: "relacl", type_oid: 1034 },
    ColumnSpec { name: "reloptions", type_oid: 1009 },
    ColumnSpec { name: "relpartbound", type_oid: 194 },
];

const PG_ATTRIBUTE_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "attrelid", type_oid: 26 },
    ColumnSpec { name: "attname", type_oid: 25 },
    ColumnSpec { name: "atttypid", type_oid: 26 },
    ColumnSpec { name: "attstattarget", type_oid: 23 },
    ColumnSpec { name: "attlen", type_oid: 21 },
    ColumnSpec { name: "attnum", type_oid: 21 },
    ColumnSpec { name: "attndims", type_oid: 23 },
    ColumnSpec { name: "attcacheoff", type_oid: 23 },
    ColumnSpec { name: "atttypmod", type_oid: 23 },
    ColumnSpec { name: "attbyval", type_oid: 16 },
    ColumnSpec { name: "attalign", type_oid: 18 },
    ColumnSpec { name: "attstorage", type_oid: 18 },
    ColumnSpec { name: "attcompression", type_oid: 18 },
    ColumnSpec { name: "attnotnull", type_oid: 16 },
    ColumnSpec { name: "atthasdef", type_oid: 16 },
    ColumnSpec { name: "atthasmissing", type_oid: 16 },
    ColumnSpec { name: "attidentity", type_oid: 18 },
    ColumnSpec { name: "attgenerated", type_oid: 18 },
    ColumnSpec { name: "attisdropped", type_oid: 16 },
    ColumnSpec { name: "attislocal", type_oid: 16 },
    ColumnSpec { name: "attinhcount", type_oid: 23 },
    ColumnSpec { name: "attcollation", type_oid: 26 },
    ColumnSpec { name: "attacl", type_oid: 25 },
    ColumnSpec { name: "attoptions", type_oid: 25 },
    ColumnSpec { name: "attfdwoptions", type_oid: 25 },
    ColumnSpec { name: "attmissingval", type_oid: 25 },
];

const PG_CONSTRAINT_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "oid", type_oid: 25 },
    ColumnSpec { name: "conname", type_oid: 25 },
    ColumnSpec { name: "connamespace", type_oid: 25 },
    ColumnSpec { name: "contype", type_oid: 18 },
    ColumnSpec { name: "condeferrable", type_oid: 16 },
    ColumnSpec { name: "condeferred", type_oid: 16 },
    ColumnSpec { name: "convalidated", type_oid: 16 },
    ColumnSpec { name: "conrelid", type_oid: 25 },
    ColumnSpec { name: "contypid", type_oid: 25 },
    ColumnSpec { name: "conindid", type_oid: 25 },
    ColumnSpec { name: "conparentid", type_oid: 25 },
    ColumnSpec { name: "confrelid", type_oid: 25 },
    ColumnSpec { name: "confupdtype", type_oid: 18 },
    ColumnSpec { name: "confdeltype", type_oid: 18 },
    ColumnSpec { name: "confmatchtype", type_oid: 18 },
    ColumnSpec { name: "conislocal", type_oid: 16 },
    ColumnSpec { name: "coninhcount", type_oid: 23 },
    ColumnSpec { name: "connoinherit", type_oid: 16 },
    ColumnSpec { name: "conkey", type_oid: 25 },
    ColumnSpec { name: "confkey", type_oid: 25 },
    ColumnSpec { name: "conpfeqop", type_oid: 25 },
    ColumnSpec { name: "conppeqop", type_oid: 25 },
    ColumnSpec { name: "conffeqop", type_oid: 25 },
    ColumnSpec { name: "confdelsetcols", type_oid: 25 },
    ColumnSpec { name: "conexclop", type_oid: 25 },
    ColumnSpec { name: "conbin", type_oid: 25 },
];

const PG_DEPEND_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "classid", type_oid: 25 },
    ColumnSpec { name: "objid", type_oid: 25 },
    ColumnSpec { name: "objsubid", type_oid: 23 },
    ColumnSpec { name: "refclassid", type_oid: 25 },
    ColumnSpec { name: "refobjid", type_oid: 25 },
    ColumnSpec { name: "refobjsubid", type_oid: 23 },
    ColumnSpec { name: "deptype", type_oid: 18 },
];

const INFORMATION_SCHEMA_SCHEMATA_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "catalog_name", type_oid: 25 },
    ColumnSpec { name: "schema_name", type_oid: 25 },
    ColumnSpec { name: "schema_owner", type_oid: 25 },
    ColumnSpec { name: "default_character_set_catalog", type_oid: 25 },
    ColumnSpec { name: "default_character_set_schema", type_oid: 25 },
    ColumnSpec { name: "default_character_set_name", type_oid: 25 },
    ColumnSpec { name: "sql_path", type_oid: 25 },
];

const INFORMATION_SCHEMA_TABLES_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "table_catalog", type_oid: 25 },
    ColumnSpec { name: "table_schema", type_oid: 25 },
    ColumnSpec { name: "table_name", type_oid: 25 },
    ColumnSpec { name: "table_type", type_oid: 25 },
    ColumnSpec { name: "self_referencing_column_name", type_oid: 25 },
    ColumnSpec { name: "reference_generation", type_oid: 25 },
    ColumnSpec { name: "user_defined_type_catalog", type_oid: 25 },
    ColumnSpec { name: "user_defined_type_schema", type_oid: 25 },
    ColumnSpec { name: "user_defined_type_name", type_oid: 25 },
    ColumnSpec { name: "is_insertable_into", type_oid: 25 },
    ColumnSpec { name: "is_typed", type_oid: 25 },
    ColumnSpec { name: "commit_action", type_oid: 25 },
];

const INFORMATION_SCHEMA_COLUMNS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "table_catalog", type_oid: 25 },
    ColumnSpec { name: "table_schema", type_oid: 25 },
    ColumnSpec { name: "table_name", type_oid: 25 },
    ColumnSpec { name: "column_name", type_oid: 25 },
    ColumnSpec { name: "ordinal_position", type_oid: 23 },
    ColumnSpec { name: "column_default", type_oid: 25 },
    ColumnSpec { name: "is_nullable", type_oid: 25 },
    ColumnSpec { name: "data_type", type_oid: 25 },
    ColumnSpec { name: "character_maximum_length", type_oid: 23 },
    ColumnSpec { name: "character_octet_length", type_oid: 23 },
    ColumnSpec { name: "numeric_precision", type_oid: 23 },
    ColumnSpec { name: "numeric_precision_radix", type_oid: 23 },
    ColumnSpec { name: "numeric_scale", type_oid: 23 },
    ColumnSpec { name: "datetime_precision", type_oid: 23 },
    ColumnSpec { name: "interval_type", type_oid: 25 },
    ColumnSpec { name: "interval_precision", type_oid: 23 },
    ColumnSpec { name: "character_set_catalog", type_oid: 25 },
    ColumnSpec { name: "character_set_schema", type_oid: 25 },
    ColumnSpec { name: "character_set_name", type_oid: 25 },
    ColumnSpec { name: "collation_catalog", type_oid: 25 },
    ColumnSpec { name: "collation_schema", type_oid: 25 },
    ColumnSpec { name: "collation_name", type_oid: 25 },
    ColumnSpec { name: "domain_catalog", type_oid: 25 },
    ColumnSpec { name: "domain_schema", type_oid: 25 },
    ColumnSpec { name: "domain_name", type_oid: 25 },
    ColumnSpec { name: "udt_catalog", type_oid: 25 },
    ColumnSpec { name: "udt_schema", type_oid: 25 },
    ColumnSpec { name: "udt_name", type_oid: 25 },
    ColumnSpec { name: "scope_catalog", type_oid: 25 },
    ColumnSpec { name: "scope_schema", type_oid: 25 },
    ColumnSpec { name: "scope_name", type_oid: 25 },
    ColumnSpec { name: "maximum_cardinality", type_oid: 23 },
    ColumnSpec { name: "dtd_identifier", type_oid: 25 },
    ColumnSpec { name: "is_self_referencing", type_oid: 25 },
    ColumnSpec { name: "is_identity", type_oid: 25 },
    ColumnSpec { name: "identity_generation", type_oid: 25 },
    ColumnSpec { name: "identity_start", type_oid: 25 },
    ColumnSpec { name: "identity_increment", type_oid: 25 },
    ColumnSpec { name: "identity_maximum", type_oid: 25 },
    ColumnSpec { name: "identity_minimum", type_oid: 25 },
    ColumnSpec { name: "identity_cycle", type_oid: 25 },
    ColumnSpec { name: "is_generated", type_oid: 25 },
    ColumnSpec { name: "generation_expression", type_oid: 25 },
    ColumnSpec { name: "is_updatable", type_oid: 25 },
];

const INFORMATION_SCHEMA_KEY_COLUMN_USAGE_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "constraint_catalog", type_oid: 25 },
    ColumnSpec { name: "constraint_schema", type_oid: 25 },
    ColumnSpec { name: "constraint_name", type_oid: 25 },
    ColumnSpec { name: "table_catalog", type_oid: 25 },
    ColumnSpec { name: "table_schema", type_oid: 25 },
    ColumnSpec { name: "table_name", type_oid: 25 },
    ColumnSpec { name: "column_name", type_oid: 25 },
    ColumnSpec { name: "ordinal_position", type_oid: 23 },
    ColumnSpec { name: "position_in_unique_constraint", type_oid: 23 },
];

const INFORMATION_SCHEMA_TABLE_CONSTRAINTS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "constraint_catalog", type_oid: 25 },
    ColumnSpec { name: "constraint_schema", type_oid: 25 },
    ColumnSpec { name: "constraint_name", type_oid: 25 },
    ColumnSpec { name: "table_catalog", type_oid: 25 },
    ColumnSpec { name: "table_schema", type_oid: 25 },
    ColumnSpec { name: "table_name", type_oid: 25 },
    ColumnSpec { name: "constraint_type", type_oid: 25 },
    ColumnSpec { name: "is_deferrable", type_oid: 25 },
    ColumnSpec { name: "initially_deferred", type_oid: 25 },
    ColumnSpec { name: "enforced", type_oid: 25 },
    ColumnSpec { name: "nulls_distinct", type_oid: 25 },
];

const INFORMATION_SCHEMA_ROUTINES_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "specific_catalog", type_oid: 25 },
    ColumnSpec { name: "specific_schema", type_oid: 25 },
    ColumnSpec { name: "specific_name", type_oid: 25 },
    ColumnSpec { name: "routine_catalog", type_oid: 25 },
    ColumnSpec { name: "routine_schema", type_oid: 25 },
    ColumnSpec { name: "routine_name", type_oid: 25 },
    ColumnSpec { name: "routine_type", type_oid: 25 },
    ColumnSpec { name: "module_catalog", type_oid: 25 },
    ColumnSpec { name: "module_schema", type_oid: 25 },
    ColumnSpec { name: "module_name", type_oid: 25 },
    ColumnSpec { name: "udt_catalog", type_oid: 25 },
    ColumnSpec { name: "udt_schema", type_oid: 25 },
    ColumnSpec { name: "udt_name", type_oid: 25 },
    ColumnSpec { name: "data_type", type_oid: 25 },
    ColumnSpec { name: "character_maximum_length", type_oid: 23 },
    ColumnSpec { name: "character_octet_length", type_oid: 23 },
    ColumnSpec { name: "character_set_catalog", type_oid: 25 },
    ColumnSpec { name: "character_set_schema", type_oid: 25 },
    ColumnSpec { name: "character_set_name", type_oid: 25 },
    ColumnSpec { name: "collation_catalog", type_oid: 25 },
    ColumnSpec { name: "collation_schema", type_oid: 25 },
    ColumnSpec { name: "collation_name", type_oid: 25 },
    ColumnSpec { name: "numeric_precision", type_oid: 23 },
    ColumnSpec { name: "numeric_precision_radix", type_oid: 23 },
    ColumnSpec { name: "numeric_scale", type_oid: 23 },
    ColumnSpec { name: "datetime_precision", type_oid: 23 },
    ColumnSpec { name: "interval_type", type_oid: 25 },
    ColumnSpec { name: "interval_precision", type_oid: 23 },
    ColumnSpec { name: "type_udt_catalog", type_oid: 25 },
    ColumnSpec { name: "type_udt_schema", type_oid: 25 },
    ColumnSpec { name: "type_udt_name", type_oid: 25 },
    ColumnSpec { name: "scope_catalog", type_oid: 25 },
    ColumnSpec { name: "scope_schema", type_oid: 25 },
    ColumnSpec { name: "scope_name", type_oid: 25 },
    ColumnSpec { name: "maximum_cardinality", type_oid: 23 },
    ColumnSpec { name: "dtd_identifier", type_oid: 25 },
    ColumnSpec { name: "routine_body", type_oid: 25 },
    ColumnSpec { name: "routine_definition", type_oid: 25 },
    ColumnSpec { name: "external_name", type_oid: 25 },
    ColumnSpec { name: "external_language", type_oid: 25 },
    ColumnSpec { name: "parameter_style", type_oid: 25 },
    ColumnSpec { name: "is_deterministic", type_oid: 25 },
    ColumnSpec { name: "sql_data_access", type_oid: 25 },
    ColumnSpec { name: "is_null_call", type_oid: 25 },
    ColumnSpec { name: "sql_path", type_oid: 25 },
    ColumnSpec { name: "schema_level_routine", type_oid: 25 },
    ColumnSpec { name: "max_dynamic_result_sets", type_oid: 23 },
    ColumnSpec { name: "is_user_defined_cast", type_oid: 25 },
    ColumnSpec { name: "is_implicitly_invocable", type_oid: 25 },
    ColumnSpec { name: "security_type", type_oid: 25 },
    ColumnSpec { name: "to_sql_specific_catalog", type_oid: 25 },
    ColumnSpec { name: "to_sql_specific_schema", type_oid: 25 },
    ColumnSpec { name: "to_sql_specific_name", type_oid: 25 },
    ColumnSpec { name: "as_locator", type_oid: 25 },
    ColumnSpec { name: "created", type_oid: 25 },
    ColumnSpec { name: "last_altered", type_oid: 25 },
    ColumnSpec { name: "new_savepoint_level", type_oid: 25 },
    ColumnSpec { name: "is_udt_dependent", type_oid: 25 },
    ColumnSpec { name: "result_cast_from_data_type", type_oid: 25 },
    ColumnSpec { name: "result_cast_as_locator", type_oid: 25 },
    ColumnSpec { name: "result_cast_char_max_length", type_oid: 25 },
    ColumnSpec { name: "result_cast_char_octet_length", type_oid: 25 },
    ColumnSpec { name: "result_cast_char_set_catalog", type_oid: 25 },
    ColumnSpec { name: "result_cast_char_set_schema", type_oid: 25 },
    ColumnSpec { name: "result_cast_char_set_name", type_oid: 25 },
    ColumnSpec { name: "result_cast_collation_catalog", type_oid: 25 },
    ColumnSpec { name: "result_cast_collation_schema", type_oid: 25 },
    ColumnSpec { name: "result_cast_collation_name", type_oid: 25 },
    ColumnSpec { name: "result_cast_numeric_precision", type_oid: 25 },
    ColumnSpec { name: "result_cast_numeric_precision_radix", type_oid: 25 },
    ColumnSpec { name: "result_cast_numeric_scale", type_oid: 25 },
    ColumnSpec { name: "result_cast_datetime_precision", type_oid: 25 },
    ColumnSpec { name: "result_cast_interval_type", type_oid: 25 },
    ColumnSpec { name: "result_cast_interval_precision", type_oid: 25 },
    ColumnSpec { name: "result_cast_type_udt_catalog", type_oid: 25 },
    ColumnSpec { name: "result_cast_type_udt_schema", type_oid: 25 },
    ColumnSpec { name: "result_cast_type_udt_name", type_oid: 25 },
    ColumnSpec { name: "result_cast_scope_catalog", type_oid: 25 },
    ColumnSpec { name: "result_cast_scope_schema", type_oid: 25 },
    ColumnSpec { name: "result_cast_scope_name", type_oid: 25 },
    ColumnSpec { name: "result_cast_maximum_cardinality", type_oid: 25 },
    ColumnSpec { name: "result_cast_dtd_identifier", type_oid: 25 },
];

const INFORMATION_SCHEMA_VIEWS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "table_catalog", type_oid: 25 },
    ColumnSpec { name: "table_schema", type_oid: 25 },
    ColumnSpec { name: "table_name", type_oid: 25 },
    ColumnSpec { name: "view_definition", type_oid: 25 },
    ColumnSpec { name: "check_option", type_oid: 25 },
    ColumnSpec { name: "is_updatable", type_oid: 25 },
    ColumnSpec { name: "is_insertable_into", type_oid: 25 },
    ColumnSpec { name: "is_trigger_updatable", type_oid: 25 },
    ColumnSpec { name: "is_trigger_deletable", type_oid: 25 },
    ColumnSpec { name: "is_trigger_insertable_into", type_oid: 25 },
];

const INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "constraint_catalog", type_oid: 25 },
    ColumnSpec { name: "constraint_schema", type_oid: 25 },
    ColumnSpec { name: "constraint_name", type_oid: 25 },
    ColumnSpec { name: "unique_constraint_catalog", type_oid: 25 },
    ColumnSpec { name: "unique_constraint_schema", type_oid: 25 },
    ColumnSpec { name: "unique_constraint_name", type_oid: 25 },
    ColumnSpec { name: "match_option", type_oid: 25 },
    ColumnSpec { name: "update_rule", type_oid: 25 },
    ColumnSpec { name: "delete_rule", type_oid: 25 },
];

const INFORMATION_SCHEMA_CHECK_CONSTRAINTS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "constraint_catalog", type_oid: 25 },
    ColumnSpec { name: "constraint_schema", type_oid: 25 },
    ColumnSpec { name: "constraint_name", type_oid: 25 },
    ColumnSpec { name: "check_clause", type_oid: 25 },
];

const INFORMATION_SCHEMA_TRIGGERS_COLUMNS: &[ColumnSpec] = &[
    ColumnSpec { name: "trigger_catalog", type_oid: 25 },
    ColumnSpec { name: "trigger_schema", type_oid: 25 },
    ColumnSpec { name: "trigger_name", type_oid: 25 },
    ColumnSpec { name: "event_manipulation", type_oid: 25 },
    ColumnSpec { name: "event_object_catalog", type_oid: 25 },
    ColumnSpec { name: "event_object_schema", type_oid: 25 },
    ColumnSpec { name: "event_object_table", type_oid: 25 },
    ColumnSpec { name: "action_order", type_oid: 25 },
    ColumnSpec { name: "action_condition", type_oid: 25 },
    ColumnSpec { name: "action_statement", type_oid: 25 },
    ColumnSpec { name: "action_orientation", type_oid: 25 },
    ColumnSpec { name: "action_timing", type_oid: 25 },
    ColumnSpec { name: "action_reference_old_table", type_oid: 25 },
    ColumnSpec { name: "action_reference_new_table", type_oid: 25 },
    ColumnSpec { name: "action_reference_old_row", type_oid: 25 },
    ColumnSpec { name: "action_reference_new_row", type_oid: 25 },
    ColumnSpec { name: "created", type_oid: 25 },
];

fn to_field_descriptions(columns: &[ColumnSpec]) -> Vec<FieldDescription> {
    columns
        .iter()
        .enumerate()
        .map(|(i, c)| FieldDescription {
            name: c.name.to_string(),
            table_oid: 0,
            column_id: (i + 1) as i16,
            type_oid: c.type_oid,
            type_size: -1,
            type_modifier: -1,
            format: 0,
        })
        .collect()
}

pub fn column_names(columns: &[ColumnSpec]) -> Vec<String> {
    columns.iter().map(|c| c.name.to_string()).collect()
}

pub fn information_schema_schemata_columns() -> &'static [ColumnSpec] {
    INFORMATION_SCHEMA_SCHEMATA_COLUMNS
}

pub fn pg_database_columns() -> &'static [ColumnSpec] {
    PG_DATABASE_COLUMNS
}

pub fn information_schema_tables_columns() -> &'static [ColumnSpec] {
    INFORMATION_SCHEMA_TABLES_COLUMNS
}

pub fn information_schema_columns_columns() -> &'static [ColumnSpec] {
    INFORMATION_SCHEMA_COLUMNS_COLUMNS
}

pub fn information_schema_key_column_usage_columns() -> &'static [ColumnSpec] {
    INFORMATION_SCHEMA_KEY_COLUMN_USAGE_COLUMNS
}

pub fn information_schema_table_constraints_columns() -> &'static [ColumnSpec] {
    INFORMATION_SCHEMA_TABLE_CONSTRAINTS_COLUMNS
}

pub fn information_schema_routines_columns() -> &'static [ColumnSpec] {
    INFORMATION_SCHEMA_ROUTINES_COLUMNS
}

pub fn information_schema_views_columns() -> &'static [ColumnSpec] {
    INFORMATION_SCHEMA_VIEWS_COLUMNS
}

pub fn information_schema_referential_constraints_columns() -> &'static [ColumnSpec] {
    INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS_COLUMNS
}

pub fn information_schema_check_constraints_columns() -> &'static [ColumnSpec] {
    INFORMATION_SCHEMA_CHECK_CONSTRAINTS_COLUMNS
}

pub fn information_schema_triggers_columns() -> &'static [ColumnSpec] {
    INFORMATION_SCHEMA_TRIGGERS_COLUMNS
}

pub fn select_star_field_descriptions_for_catalog_query(query: &str) -> Option<Vec<FieldDescription>> {
    let q = query.to_ascii_lowercase();

    if q.contains("pg_database") {
        return Some(to_field_descriptions(PG_DATABASE_COLUMNS));
    }
    if q.contains("pg_class") {
        return Some(to_field_descriptions(PG_CLASS_COLUMNS));
    }
    if q.contains("pg_attribute") {
        return Some(to_field_descriptions(PG_ATTRIBUTE_COLUMNS));
    }
    if q.contains("pg_constraint") {
        return Some(to_field_descriptions(PG_CONSTRAINT_COLUMNS));
    }
    if q.contains("pg_depend") {
        return Some(to_field_descriptions(PG_DEPEND_COLUMNS));
    }
    if q.contains("information_schema.schemata") {
        return Some(to_field_descriptions(INFORMATION_SCHEMA_SCHEMATA_COLUMNS));
    }
    if q.contains("information_schema.tables") {
        return Some(to_field_descriptions(INFORMATION_SCHEMA_TABLES_COLUMNS));
    }
    if q.contains("information_schema.columns") {
        return Some(to_field_descriptions(INFORMATION_SCHEMA_COLUMNS_COLUMNS));
    }
    if q.contains("information_schema.key_column_usage") {
        return Some(to_field_descriptions(INFORMATION_SCHEMA_KEY_COLUMN_USAGE_COLUMNS));
    }
    if q.contains("information_schema.table_constraints") {
        return Some(to_field_descriptions(INFORMATION_SCHEMA_TABLE_CONSTRAINTS_COLUMNS));
    }
    if q.contains("information_schema.routines") {
        return Some(to_field_descriptions(INFORMATION_SCHEMA_ROUTINES_COLUMNS));
    }
    if q.contains("information_schema.views") {
        return Some(to_field_descriptions(INFORMATION_SCHEMA_VIEWS_COLUMNS));
    }
    if q.contains("information_schema.referential_constraints") {
        return Some(to_field_descriptions(INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS_COLUMNS));
    }
    if q.contains("information_schema.check_constraints") {
        return Some(to_field_descriptions(INFORMATION_SCHEMA_CHECK_CONSTRAINTS_COLUMNS));
    }
    if q.contains("information_schema.triggers") {
        return Some(to_field_descriptions(INFORMATION_SCHEMA_TRIGGERS_COLUMNS));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_star_field_descriptions_for_pg_database() {
        let fields = select_star_field_descriptions_for_catalog_query("SELECT * FROM pg_database")
            .expect("expected predefined field descriptions");
        assert_eq!(fields.len(), 18);
        assert_eq!(fields[0].name, "oid");
        assert_eq!(fields[0].type_oid, 23);
    }

    #[test]
    fn test_information_schema_column_name_sets() {
        let kcu = column_names(information_schema_key_column_usage_columns());
        let tc = column_names(information_schema_table_constraints_columns());
        let views = column_names(information_schema_views_columns());
        let rc = column_names(information_schema_referential_constraints_columns());
        assert_eq!(kcu.len(), 9);
        assert_eq!(tc.len(), 11);
        assert_eq!(views.len(), 10);
        assert_eq!(rc.len(), 9);
        assert_eq!(kcu[0], "constraint_catalog");
        assert_eq!(tc[6], "constraint_type");
        assert_eq!(views[3], "view_definition");
        assert_eq!(rc[6], "match_option");
    }
}
