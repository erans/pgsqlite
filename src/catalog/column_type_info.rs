//! PostgreSQL type string to `information_schema` column metadata.
//!
//! Moved out of `CatalogInterceptor::map_sqlite_type_to_pg_column_info` when the
//! `information_schema.columns` handler was deleted, so the views could keep
//! reporting `character_maximum_length`, `numeric_precision` and `numeric_scale`.
//!
//! Input is the declared PostgreSQL type from `__pgsqlite_schema.pg_type`
//! (`VARCHAR(50)`, `NUMERIC(10,2)`, `TIMESTAMPTZ`), falling back to the SQLite
//! declared type for databases created outside pgsqlite. Deriving from the
//! string rather than an OID is deliberate — an OID cannot carry the modifier.

/// The four `information_schema.columns` fields that depend on a column's type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgColumnInfo {
    pub data_type: String,
    pub character_maximum_length: Option<i32>,
    pub numeric_precision: Option<i32>,
    pub numeric_scale: Option<i32>,
}

impl PgColumnInfo {
    fn plain(data_type: &str) -> Self {
        Self {
            data_type: data_type.to_string(),
            character_maximum_length: None,
            numeric_precision: None,
            numeric_scale: None,
        }
    }

    fn integral(data_type: &str, precision: i32) -> Self {
        Self {
            data_type: data_type.to_string(),
            character_maximum_length: None,
            numeric_precision: Some(precision),
            numeric_scale: Some(0),
        }
    }
}

/// Split `NUMERIC(10,2)` into `("NUMERIC", ["10", "2"])`.
fn split_modifier(upper: &str) -> (&str, Vec<&str>) {
    match (upper.find('('), upper.rfind(')')) {
        (Some(open), Some(close)) if close > open => {
            let base = upper[..open].trim();
            let params = upper[open + 1..close]
                .split(',')
                .map(|p| p.trim())
                .collect();
            (base, params)
        }
        _ => (upper.trim(), Vec::new()),
    }
}

pub fn pg_column_info(pg_type: &str) -> PgColumnInfo {
    let upper = pg_type.trim().to_uppercase();

    // Array types report `ARRAY`, with the element type in udt_name in real
    // PostgreSQL. Checked before modifiers so `NUMERIC(10,2)[]` is an array.
    if upper.ends_with("[]") {
        return PgColumnInfo::plain("ARRAY");
    }

    let (base, params) = split_modifier(&upper);
    let param = |i: usize| params.get(i).and_then(|p| p.parse::<i32>().ok());

    match base {
        "VARCHAR" | "CHARACTER VARYING" => PgColumnInfo {
            data_type: "character varying".to_string(),
            character_maximum_length: param(0),
            numeric_precision: None,
            numeric_scale: None,
        },
        "CHAR" | "CHARACTER" | "BPCHAR" => PgColumnInfo {
            data_type: "character".to_string(),
            character_maximum_length: param(0),
            numeric_precision: None,
            numeric_scale: None,
        },
        "NUMERIC" | "DECIMAL" => PgColumnInfo {
            data_type: "numeric".to_string(),
            character_maximum_length: None,
            numeric_precision: param(0),
            // A precision with no scale means scale 0; no precision means
            // unconstrained, where PostgreSQL reports NULL for both.
            numeric_scale: param(1).or(param(0).map(|_| 0)),
        },

        "SMALLINT" | "INT2" | "SMALLSERIAL" => PgColumnInfo::integral("smallint", 16),
        "INTEGER" | "INT" | "INT4" | "SERIAL" => PgColumnInfo::integral("integer", 32),
        "BIGINT" | "INT8" | "BIGSERIAL" => PgColumnInfo::integral("bigint", 64),

        "REAL" | "FLOAT4" => PgColumnInfo {
            data_type: "real".to_string(),
            character_maximum_length: None,
            numeric_precision: Some(24),
            numeric_scale: None,
        },
        "DOUBLE PRECISION" | "FLOAT8" | "FLOAT" | "DOUBLE" => PgColumnInfo {
            data_type: "double precision".to_string(),
            character_maximum_length: None,
            numeric_precision: Some(53),
            numeric_scale: None,
        },

        "TEXT" => PgColumnInfo::plain("text"),
        "BYTEA" | "BLOB" => PgColumnInfo::plain("bytea"),
        "BOOLEAN" | "BOOL" => PgColumnInfo::plain("boolean"),
        "UUID" => PgColumnInfo::plain("uuid"),
        "JSON" => PgColumnInfo::plain("json"),
        "JSONB" => PgColumnInfo::plain("jsonb"),
        "MONEY" => PgColumnInfo::plain("money"),
        "INTERVAL" => PgColumnInfo::plain("interval"),

        "DATE" => PgColumnInfo::plain("date"),
        "TIME" | "TIME WITHOUT TIME ZONE" => PgColumnInfo::plain("time without time zone"),
        "TIMETZ" | "TIME WITH TIME ZONE" => PgColumnInfo::plain("time with time zone"),
        "TIMESTAMP" | "DATETIME" | "TIMESTAMP WITHOUT TIME ZONE" => {
            PgColumnInfo::plain("timestamp without time zone")
        }
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            PgColumnInfo::plain("timestamp with time zone")
        }

        // Unknown types, including ENUMs, report text. Matches the deleted
        // handler's final branch and the view's previous `ELSE 'text'`.
        _ => PgColumnInfo::plain("text"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(pg_type: &str) -> String {
        pg_column_info(pg_type).data_type
    }

    /// Types the deleted handler already got right. These must not regress.
    #[test]
    fn preserves_correct_handler_behavior() {
        assert_eq!(dt("INTEGER"), "integer");
        assert_eq!(dt("TEXT"), "text");
        assert_eq!(dt("BOOLEAN"), "boolean");
        assert_eq!(dt("UUID"), "uuid");
        assert_eq!(dt("JSONB"), "jsonb");
        assert_eq!(dt("NUMERIC(10,2)"), "numeric");
        assert_eq!(dt("VARCHAR(50)"), "character varying");
        assert_eq!(dt("TIMESTAMP"), "timestamp without time zone");
        assert_eq!(dt("DATE"), "date");
        assert_eq!(dt("BLOB"), "bytea");
    }

    /// Types the deleted handler got wrong. Measured in the spec.
    #[test]
    fn fixes_types_the_handler_reported_as_text() {
        assert_eq!(dt("SERIAL"), "integer");
        assert_eq!(dt("BIGSERIAL"), "bigint");
        assert_eq!(dt("TIMESTAMPTZ"), "timestamp with time zone");
        assert_eq!(dt("TIMESTAMP WITH TIME ZONE"), "timestamp with time zone");
        assert_eq!(dt("TIMETZ"), "time with time zone");
        assert_eq!(dt("TIME WITH TIME ZONE"), "time with time zone");
        assert_eq!(dt("TIME"), "time without time zone");
    }

    #[test]
    fn array_types_report_array() {
        assert_eq!(dt("TEXT[]"), "ARRAY");
        assert_eq!(dt("INTEGER[]"), "ARRAY");
        assert_eq!(dt("NUMERIC(10,2)[]"), "ARRAY");
    }

    #[test]
    fn character_maximum_length_comes_from_the_modifier() {
        assert_eq!(pg_column_info("VARCHAR(50)").character_maximum_length, Some(50));
        assert_eq!(pg_column_info("CHAR(8)").character_maximum_length, Some(8));
        assert_eq!(pg_column_info("VARCHAR").character_maximum_length, None);
        assert_eq!(pg_column_info("TEXT").character_maximum_length, None);
    }

    #[test]
    fn numeric_precision_and_scale_come_from_the_modifier() {
        let info = pg_column_info("NUMERIC(10,2)");
        assert_eq!(info.numeric_precision, Some(10));
        assert_eq!(info.numeric_scale, Some(2));

        let info = pg_column_info("DECIMAL(38)");
        assert_eq!(info.numeric_precision, Some(38));
        assert_eq!(info.numeric_scale, Some(0));

        let info = pg_column_info("NUMERIC");
        assert_eq!(info.numeric_precision, None);
        assert_eq!(info.numeric_scale, None);
    }

    /// PostgreSQL reports precision and scale for integer types too.
    #[test]
    fn integer_types_report_binary_precision() {
        let info = pg_column_info("INTEGER");
        assert_eq!(info.numeric_precision, Some(32));
        assert_eq!(info.numeric_scale, Some(0));

        let info = pg_column_info("BIGINT");
        assert_eq!(info.numeric_precision, Some(64));
        assert_eq!(info.numeric_scale, Some(0));
    }

    /// Text is the safe fallback: matches both the deleted handler's final
    /// branch and the view's `ELSE 'text'`. ENUM types land here (#88 non-goal).
    #[test]
    fn unknown_types_fall_back_to_text() {
        assert_eq!(dt("my_enum_type"), "text");
        assert_eq!(dt(""), "text");
    }

    #[test]
    fn case_is_insensitive() {
        assert_eq!(dt("varchar(50)"), "character varying");
        assert_eq!(dt("TimestampTZ"), "timestamp with time zone");
    }

    #[test]
    fn udfs_are_callable_from_sql() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::functions::register_all_functions(&conn).unwrap();

        let (dt, len): (String, Option<i32>) = conn
            .query_row(
                "SELECT __pgsqlite_pg_data_type('VARCHAR(50)'), __pgsqlite_char_max_length('VARCHAR(50)')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(dt, "character varying");
        assert_eq!(len, Some(50));

        let (p, s): (Option<i32>, Option<i32>) = conn
            .query_row(
                "SELECT __pgsqlite_numeric_precision('NUMERIC(10,2)'), __pgsqlite_numeric_scale('NUMERIC(10,2)')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((p, s), (Some(10), Some(2)));

        let dt: Option<String> = conn
            .query_row("SELECT __pgsqlite_pg_data_type(NULL)", [], |r| r.get(0))
            .unwrap();
        assert_eq!(dt, None);
    }
}
