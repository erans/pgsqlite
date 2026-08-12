use crate::session::db_handler::{DbHandler, DbResponse};
use uuid::Uuid;
use crate::session::SessionState;
use crate::PgSqliteError;
use crate::translator::{RegexTranslator, SchemaPrefixTranslator, TranslationMetadata};
use crate::query::projection_resolver::resolve_columns_with_legacy;
use sqlparser::ast::{Statement, TableFactor, Select, SetExpr, SelectItem, Expr, FunctionArg, FunctionArgExpr};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Span};
use tracing::{debug, info};
use super::{pg_attribute::PgAttributeHandler, pg_constraint::PgConstraintHandler, pg_depend::PgDependHandler, pg_enum::PgEnumHandler, pg_description::PgDescriptionHandler, pg_roles::PgRolesHandler, pg_user::PgUserHandler, pg_stats::PgStatsHandler, pg_sequence::PgSequenceHandler, pg_trigger::PgTriggerHandler, pg_settings::PgSettingsHandler, system_functions::SystemFunctions, where_evaluator::WhereEvaluator};
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use std::collections::HashMap;

/// Type alias for the complex Future type returned by process_expression
type ProcessExpressionFuture<'a> = Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>>;

/// Intercepts and handles queries to pg_catalog tables
pub struct CatalogInterceptor;

impl CatalogInterceptor {
    /// Check if a query is targeting pg_catalog and handle it
        pub fn is_catalog_query(query: &str) -> bool {
        let lower_query = query.to_lowercase();

        // pgsqlite's own cache-status pseudo table is served by the interceptor.
        if lower_query.contains("select * from pgsqlite_cache_status") {
            return true;
        }

        // version() is deliberately left to the SQLite function layer.
        if lower_query.trim() == "select pg_catalog.version()"
            || lower_query.trim() == "select version()"
        {
            return false;
        }

        // Check for catalog tables
        let has_catalog_tables = lower_query.contains("pg_catalog") || lower_query.contains("pg_type") ||
           lower_query.contains("pg_namespace") || lower_query.contains("pg_range") ||
           lower_query.contains("pg_tablespace") ||
           lower_query.contains("pg_class") || lower_query.contains("pg_attribute") ||
           lower_query.contains("pg_enum") ||
           lower_query.contains("pg_description") || lower_query.contains("pg_roles") ||
           lower_query.contains("pg_user") || lower_query.contains("pg_authid") ||
           lower_query.contains("pg_stats") || lower_query.contains("pg_constraint") ||
           lower_query.contains("pg_depend") || lower_query.contains("pg_sequence") ||
           lower_query.contains("pg_trigger") || lower_query.contains("pg_settings") ||
           lower_query.contains("pg_collation") ||
           lower_query.contains("pg_replication_slots") ||
           lower_query.contains("pg_shdepend") ||
           lower_query.contains("pg_statistic") ||
           lower_query.contains("information_schema") ||
           lower_query.contains("pg_stat_") || lower_query.contains("pg_database") ||
           lower_query.contains("pg_foreign_data_wrapper");

        // Check for system functions
        let has_system_functions = lower_query.contains("to_regtype") ||
           lower_query.contains("pg_get_constraintdef") || lower_query.contains("pg_table_is_visible") ||
           lower_query.contains("format_type") || lower_query.contains("pg_get_expr") ||
           lower_query.contains("pg_get_userbyid") || lower_query.contains("pg_get_indexdef") ||
           lower_query.contains("pg_size_pretty");

        has_catalog_tables || has_system_functions
    }
    async fn v38_jdbc_metadata(
        query: &str,
        db: &Arc<DbHandler>,
    ) -> Option<Result<DbResponse, PgSqliteError>> {
        let lower = query.to_lowercase();

        // ---- v39: getImportedKeys / getExportedKeys / getCrossReference ----
        // 指纹：模板里同时出现 PKTABLE_CAT 与 FKTABLE_CAT 两个别名，形状极稳。
        // 原 SQL 有两处 SQLite 死语法：`generate_series(1,32) AS pos (n)`（表别名后
        // 的列别名列表）与 `con.confkey[pos.n]`（数组下标）；且 pg_constraint 里
        // 压根没有外键行。改走 PRAGMA foreign_key_list。
        if lower.contains("pktable_cat") && lower.contains("fktable_cat") {
            let child = Self::v39_extract_eq_str(query, "fkc.relname");
            let parent = Self::v39_extract_eq_str(query, "pkc.relname");
            info!(
                "v39: intercept pgjdbc foreign keys child={child:?} parent={parent:?}"
            );
            return Some(
                Self::v39_get_foreign_keys(db, child.as_deref(), parent.as_deref()).await,
            );
        }

        // ---- getSchemas：靠 current_schemas 数组下标识别 ----
        if lower.contains("current_schemas") && lower.contains("table_schem") {
            info!("v38: intercept pgjdbc getSchemas");
            return Some(Self::v38_get_schemas(db).await);
        }

        if !lower.contains("_pg_expandarray") {
            return None;
        }
        let table = match Self::v38_extract_relname(query) {
            Some(t) => t,
            None => return None,
        };

        // getIndexInfo 模板带 pg_am/amname 与 ASC_OR_DESC；getPrimaryKeys 带 indisprimary
        if lower.contains("am.amname") || lower.contains("asc_or_desc") {
            info!("v38: intercept pgjdbc getIndexInfo for '{}'", table);
            let unique_only = lower.contains("and i.indisunique");
            return Some(Self::v38_get_index_info(db, &table, unique_only).await);
        }
        if lower.contains("indisprimary") {
            info!("v38: intercept pgjdbc getPrimaryKeys for '{}'", table);
            return Some(Self::v38_get_primary_keys(db, &table).await);
        }
        None
    }
    fn v38_extract_relname(query: &str) -> Option<String> {
        let lower = query.to_lowercase();
        let key = "ct.relname";
        let mut from = 0usize;
        while let Some(pos) = lower[from..].find(key) {
            let after = from + pos + key.len();
            let rest = &query[after..];
            let b = rest.as_bytes();
            let mut i = 0usize;
            while i < b.len() && (b[i] == b' ' || b[i] == b'=') {
                i += 1;
            }
            if rest.len() > i && rest[i..].to_lowercase().starts_with("like") {
                i += 4;
            }
            while i < b.len() && b[i] == b' ' {
                i += 1;
            }
            if i < b.len() && b[i] == b'\'' {
                let s = i + 1;
                if let Some(e) = rest[s..].find('\'') {
                    let raw = &rest[s..s + e];
                    return Some(raw.replace("\\_", "_").replace("\\%", "%"));
                }
            }
            from = after;
        }
        None
    }
    fn v38_sqlq(s: &str) -> String {
        s.replace('\'', "''")
    }
    fn v38_cell(row: &[Option<Vec<u8>>], idx: usize) -> Option<Vec<u8>> {
        row.get(idx).cloned().flatten()
    }
    fn v38_text(row: &[Option<Vec<u8>>], idx: usize) -> String {
        match Self::v38_cell(row, idx) {
            Some(v) => String::from_utf8_lossy(&v).to_string(),
            None => String::new(),
        }
    }
    async fn v38_get_primary_keys(
        db: &Arc<DbHandler>,
        table: &str,
    ) -> Result<DbResponse, PgSqliteError> {
        let sql = format!(
            "SELECT name, pk FROM pragma_table_info('{}') WHERE pk > 0 ORDER BY pk",
            Self::v38_sqlq(table)
        );
        let res = db.query(&sql).await.map_err(PgSqliteError::Sqlite)?;
        let pk_name = format!("{table}_pkey");
        let mut rows = Vec::new();
        for r in &res.rows {
            let col = Self::v38_cell(r, 0).unwrap_or_default();
            let seq = Self::v38_cell(r, 1).unwrap_or_else(|| b"1".to_vec());
            rows.push(vec![
                None,                                       // TABLE_CAT
                Some(b"public".to_vec()),                   // TABLE_SCHEM
                Some(table.as_bytes().to_vec()),            // TABLE_NAME
                Some(col),                                  // COLUMN_NAME
                Some(seq),                                  // KEY_SEQ
                Some(pk_name.as_bytes().to_vec()),          // PK_NAME
            ]);
        }
        let n = rows.len();
        Ok(DbResponse {
            columns: vec![
                "TABLE_CAT".to_string(),
                "TABLE_SCHEM".to_string(),
                "TABLE_NAME".to_string(),
                "COLUMN_NAME".to_string(),
                "KEY_SEQ".to_string(),
                "PK_NAME".to_string(),
            ],
            rows,
            rows_affected: n,
        })
    }
    async fn v38_get_index_info(
        db: &Arc<DbHandler>,
        table: &str,
        unique_only: bool,
    ) -> Result<DbResponse, PgSqliteError> {
        let list_sql = format!(
            "SELECT name, \"unique\" FROM pragma_index_list('{}')",
            Self::v38_sqlq(table)
        );
        let list = db.query(&list_sql).await.map_err(PgSqliteError::Sqlite)?;
        let mut rows = Vec::new();
        for r in &list.rows {
            let iname = Self::v38_text(r, 0);
            if iname.is_empty() {
                continue;
            }
            let uniq = Self::v38_text(r, 1) == "1";
            if unique_only && !uniq {
                continue;
            }
            let info_sql = format!(
                "SELECT seqno, name FROM pragma_index_info('{}') ORDER BY seqno",
                Self::v38_sqlq(&iname)
            );
            let info = match db.query(&info_sql).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            for ir in &info.rows {
                let seqno: i64 = Self::v38_text(ir, 0).parse().unwrap_or(0);
                let cname = Self::v38_cell(ir, 1).unwrap_or_default();
                rows.push(vec![
                    None,                                             // TABLE_CAT
                    Some(b"public".to_vec()),                         // TABLE_SCHEM
                    Some(table.as_bytes().to_vec()),                  // TABLE_NAME
                    Some(if uniq { b"f".to_vec() } else { b"t".to_vec() }), // NON_UNIQUE
                    None,                                             // INDEX_QUALIFIER
                    Some(iname.as_bytes().to_vec()),                  // INDEX_NAME
                    Some(b"3".to_vec()),                              // TYPE: 3 = btree
                    Some((seqno + 1).to_string().into_bytes()),       // ORDINAL_POSITION
                    Some(cname),                                      // COLUMN_NAME
                    Some(b"A".to_vec()),                              // ASC_OR_DESC
                    Some(b"0".to_vec()),                              // CARDINALITY
                    Some(b"0".to_vec()),                              // PAGES
                    None,                                             // FILTER_CONDITION
                ]);
            }
        }
        let n = rows.len();
        Ok(DbResponse {
            columns: vec![
                "TABLE_CAT".to_string(),
                "TABLE_SCHEM".to_string(),
                "TABLE_NAME".to_string(),
                "NON_UNIQUE".to_string(),
                "INDEX_QUALIFIER".to_string(),
                "INDEX_NAME".to_string(),
                "TYPE".to_string(),
                "ORDINAL_POSITION".to_string(),
                "COLUMN_NAME".to_string(),
                "ASC_OR_DESC".to_string(),
                "CARDINALITY".to_string(),
                "PAGES".to_string(),
                "FILTER_CONDITION".to_string(),
            ],
            rows,
            rows_affected: n,
        })
    }
    async fn v38_get_schemas(db: &Arc<DbHandler>) -> Result<DbResponse, PgSqliteError> {
        let mut names: Vec<String> = Vec::new();
        if let Ok(res) = db
            .query("SELECT nspname FROM pg_namespace WHERE nspname <> 'pg_toast' ORDER BY nspname")
            .await
        {
            for r in &res.rows {
                let v = Self::v38_text(r, 0);
                if !v.is_empty() {
                    names.push(v);
                }
            }
        }
        if names.is_empty() {
            names = vec![
                "information_schema".to_string(),
                "pg_catalog".to_string(),
                "public".to_string(),
            ];
        }
        let rows: Vec<Vec<Option<Vec<u8>>>> = names
            .iter()
            .map(|n| vec![Some(n.as_bytes().to_vec()), None])
            .collect();
        let n = rows.len();
        Ok(DbResponse {
            columns: vec!["TABLE_SCHEM".to_string(), "TABLE_CATALOG".to_string()],
            rows,
            rows_affected: n,
        })
    }
    fn v39_extract_eq_str(query: &str, key: &str) -> Option<String> {
        let lower = query.to_lowercase();
        let key_l = key.to_lowercase();
        let mut from = 0usize;
        while let Some(pos) = lower[from..].find(key_l.as_str()) {
            let after = from + pos + key_l.len();
            let rest = &query[after..];
            let b = rest.as_bytes();
            let mut i = 0usize;
            while i < b.len() && (b[i] == b' ' || b[i] == b'=') {
                i += 1;
            }
            if rest.len() > i && rest[i..].to_lowercase().starts_with("like") {
                i += 4;
            }
            while i < b.len() && b[i] == b' ' {
                i += 1;
            }
            if i < b.len() && b[i] == b'\'' {
                let s = i + 1;
                if let Some(e) = rest[s..].find('\'') {
                    let raw = &rest[s..s + e];
                    return Some(raw.replace("\\_", "_").replace("\\%", "%"));
                }
            }
            from = after;
        }
        None
    }
    fn v39_fk_rule(action: &str) -> &'static str {
        match action.trim().to_uppercase().as_str() {
            "CASCADE" => "0",     // importedKeyCascade
            "RESTRICT" => "1",    // importedKeyRestrict
            "SET NULL" => "2",    // importedKeySetNull
            "SET DEFAULT" => "4", // importedKeySetDefault
            _ => "3",             // importedKeyNoAction
        }
    }
    async fn v39_get_foreign_keys(
        db: &Arc<DbHandler>,
        child_filter: Option<&str>,
        parent_filter: Option<&str>,
    ) -> Result<DbResponse, PgSqliteError> {
        // 1) 需要扫描哪些子表
        let mut children: Vec<String> = Vec::new();
        if let Some(c) = child_filter {
            children.push(c.to_string());
        } else if let Ok(res) = db
            .query(
                "SELECT name FROM sqlite_master WHERE type = 'table' \
                 AND name NOT LIKE 'sqlite_%' AND substr(name, 1, 2) <> '__' \
                 ORDER BY name",
            )
            .await
        {
            for r in &res.rows {
                let n = Self::v38_text(r, 0);
                if !n.is_empty() {
                    children.push(n);
                }
            }
        }

        // 2) 逐表读外键。元组含义：
        //    (parent, parent_col, child, child_col, key_seq, on_update, on_delete, fk_name)
        let mut recs: Vec<(String, String, String, String, i64, String, String, String)> =
            Vec::new();
        for child in &children {
            let sql = format!(
                "SELECT id, seq, \"table\", \"from\", \"to\", on_update, on_delete \
                 FROM pragma_foreign_key_list('{}') ORDER BY id, seq",
                Self::v38_sqlq(child)
            );
            let res = match db.query(&sql).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            // 同一约束（同 id）的多列必须共用一个 FK_NAME，否则复合外键会被
            // 客户端当成多个独立约束。取 seq=0 那列作为命名基准。
            let mut first_col: std::collections::HashMap<String, String> =
                std::collections::HashMap::new();
            for r in &res.rows {
                if Self::v38_text(r, 1) == "0" {
                    first_col.insert(Self::v38_text(r, 0), Self::v38_text(r, 3));
                }
            }
            for r in &res.rows {
                let id = Self::v38_text(r, 0);
                let seq: i64 = Self::v38_text(r, 1).parse().unwrap_or(0);
                let parent = Self::v38_text(r, 2);
                let child_col = Self::v38_text(r, 3);
                let mut parent_col = Self::v38_text(r, 4);
                let upd = Self::v38_text(r, 5);
                let del = Self::v38_text(r, 6);
                if parent.is_empty() || child_col.is_empty() {
                    continue;
                }
                if let Some(p) = parent_filter {
                    if !p.eq_ignore_ascii_case(&parent) {
                        continue;
                    }
                }
                // `REFERENCES parent` 省略列名时 `to` 为 NULL，回退到父表主键第 seq 列
                if parent_col.is_empty() {
                    let pk_sql = format!(
                        "SELECT name FROM pragma_table_info('{}') WHERE pk > 0 ORDER BY pk",
                        Self::v38_sqlq(&parent)
                    );
                    if let Ok(pk) = db.query(&pk_sql).await {
                        if let Some(row) = pk.rows.get(seq as usize) {
                            parent_col = Self::v38_text(row, 0);
                        }
                    }
                    if parent_col.is_empty() {
                        parent_col = "rowid".to_string();
                    }
                }
                let base = first_col
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| child_col.clone());
                let fk_name = format!("{child}_{base}_fkey");
                recs.push((
                    parent,
                    parent_col,
                    child.clone(),
                    child_col,
                    seq,
                    upd,
                    del,
                    fk_name,
                ));
            }
        }

        // 3) 排序对齐 PG 模板：imported 按 (pk 表, 约束名, 列序)，exported 按 (fk 表, ...)
        let exported = child_filter.is_none() && parent_filter.is_some();
        if exported {
            recs.sort_by(|a, b| (&a.2, &a.7, a.4).cmp(&(&b.2, &b.7, b.4)));
        } else {
            recs.sort_by(|a, b| (&a.0, &a.7, a.4).cmp(&(&b.0, &b.7, b.4)));
        }

        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        for (parent, parent_col, child, child_col, seq, upd, del, fk_name) in recs {
            let pk_name = format!("{parent}_pkey");
            rows.push(vec![
                None,                                              // PKTABLE_CAT
                Some(b"public".to_vec()),                          // PKTABLE_SCHEM
                Some(parent.into_bytes()),                         // PKTABLE_NAME
                Some(parent_col.into_bytes()),                     // PKCOLUMN_NAME
                None,                                              // FKTABLE_CAT
                Some(b"public".to_vec()),                          // FKTABLE_SCHEM
                Some(child.into_bytes()),                          // FKTABLE_NAME
                Some(child_col.into_bytes()),                      // FKCOLUMN_NAME
                Some((seq + 1).to_string().into_bytes()),          // KEY_SEQ
                Some(Self::v39_fk_rule(&upd).as_bytes().to_vec()), // UPDATE_RULE
                Some(Self::v39_fk_rule(&del).as_bytes().to_vec()), // DELETE_RULE
                Some(fk_name.into_bytes()),                        // FK_NAME
                Some(pk_name.into_bytes()),                        // PK_NAME
                Some(b"7".to_vec()),                               // importedKeyNotDeferrable
            ]);
        }
        let n = rows.len();
        Ok(DbResponse {
            columns: vec![
                "PKTABLE_CAT".to_string(),
                "PKTABLE_SCHEM".to_string(),
                "PKTABLE_NAME".to_string(),
                "PKCOLUMN_NAME".to_string(),
                "FKTABLE_CAT".to_string(),
                "FKTABLE_SCHEM".to_string(),
                "FKTABLE_NAME".to_string(),
                "FKCOLUMN_NAME".to_string(),
                "KEY_SEQ".to_string(),
                "UPDATE_RULE".to_string(),
                "DELETE_RULE".to_string(),
                "FK_NAME".to_string(),
                "PK_NAME".to_string(),
                "DEFERRABILITY".to_string(),
            ],
            rows,
            rows_affected: n,
        })
    }
    fn normalize_projection_qualifiers(query: &mut sqlparser::ast::Query) {
        fn strip(expr: &mut Expr) {
            match expr {
                Expr::CompoundIdentifier(parts) => {
                    if let Some(last) = parts.last().cloned() {
                        *expr = Expr::Identifier(last);
                    }
                }
                Expr::Cast { expr: inner, .. } => strip(inner),
                Expr::Nested(inner) => strip(inner),
                _ => {}
            }
        }
        fn walk(body: &mut SetExpr) {
            match body {
                SetExpr::Select(select) => {
                    for item in select.projection.iter_mut() {
                        match item {
                            SelectItem::UnnamedExpr(expr) => strip(expr),
                            SelectItem::ExprWithAlias { expr, .. } => strip(expr),
                            _ => {}
                        }
                    }
                }
                SetExpr::Query(q) => walk(&mut q.body),
                SetExpr::SetOperation { left, right, .. } => {
                    walk(left);
                    walk(right);
                }
                _ => {}
            }
        }
        walk(&mut query.body);
    }
    fn projection_output_name(item: &SelectItem) -> Option<String> {
        match item {
            SelectItem::UnnamedExpr(expr) => Self::extract_projection_source_column(expr),
            SelectItem::ExprWithAlias { alias, .. } => Some(Self::alias_output_name(alias)),
            _ => None,
        }
    }
    fn align_response_to_projection(projection: &[SelectItem], response: &mut DbResponse) {
        if projection.is_empty() {
            return;
        }
        // Wildcards: the client cannot know the arity in advance, leave it alone.
        if projection.iter().any(|i| {
            matches!(i, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _))
        }) {
            return;
        }

        let mut expected: Vec<String> = Vec::with_capacity(projection.len());
        for item in projection {
            match Self::projection_output_name(item) {
                Some(name) => expected.push(name),
                // An expression we cannot name confidently — do not second-guess the handler.
                None => return,
            }
        }

        let have = response.columns.len();
        if expected.len() <= have {
            return;
        }
        // Rows must currently be well formed, otherwise bail out.
        if response.rows.iter().any(|r| r.len() != have) {
            return;
        }

        // Order-preserving merge: walk the requested names, consuming handler columns in order.
        let mut names: Vec<String> = Vec::with_capacity(expected.len());
        let mut mapping: Vec<Option<usize>> = Vec::with_capacity(expected.len());
        let mut cursor = 0usize;
        for want in &expected {
            if cursor < have && response.columns[cursor].eq_ignore_ascii_case(want) {
                names.push(response.columns[cursor].clone());
                mapping.push(Some(cursor));
                cursor += 1;
            } else {
                names.push(want.clone());
                mapping.push(None);
            }
        }
        // A handler column we could not place means the merge is unreliable.
        if cursor != have {
            return;
        }

        let new_rows: Vec<Vec<Option<Vec<u8>>>> = response
            .rows
            .iter()
            .map(|row| {
                mapping
                    .iter()
                    .map(|m| match m {
                        Some(i) => row[*i].clone(),
                        None => None,
                    })
                    .collect()
            })
            .collect();

        info!(
            "CATALOG ARITY: padded catalog response {} -> {} columns {:?}",
            have,
            names.len(),
            names
        );
        response.columns = names;
        response.rows = new_rows;
    }
    fn handle_pg_namespace_query(select: &Select) -> DbResponse {
        let all_columns = vec!["oid".to_string(), "nspname".to_string()];
        let (columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        let full_rows = vec![
            vec![
                Some("11".to_string().into_bytes()),
                Some("pg_catalog".to_string().into_bytes()),
            ],
            vec![
                Some("2200".to_string().into_bytes()),
                Some("public".to_string().into_bytes()),
            ],
        ];
        if columns.is_empty() && Self::is_count_star_projection(&select.projection) {
            return Self::count_response(full_rows.len());
        }

        let rows: Vec<Vec<Option<Vec<u8>>>> = full_rows
            .into_iter()
            .map(|full_row| {
                column_indices
                    .iter()
                    .map(|&idx| full_row[idx].clone())
                    .collect()
            })
            .collect();

        let rows_affected = rows.len();
        debug!("Returning {} rows for pg_namespace query with {} columns: {:?}", rows_affected, columns.len(), columns);
        DbResponse {
            columns,
            rows,
            rows_affected,
        }
    }
    async fn v40_get_index_list(
        db: &Arc<DbHandler>,
        table: Option<&str>,
    ) -> Result<DbResponse, PgSqliteError> {
        let cols = vec![
            "index_name".to_string(),
            "columns".to_string(),
            "is_unique".to_string(),
            "is_primary".to_string(),
            "filter_expr".to_string(),
            "index_type".to_string(),
            "nkeyatts".to_string(),
            "indkey".to_string(),
            "index_comment".to_string(),
        ];
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        if let Some(table) = table {
            let list_sql = format!(
                "SELECT name, \"unique\", origin FROM pragma_index_list('{}') ORDER BY name",
                Self::v38_sqlq(table)
            );
            if let Ok(list) = db.query(&list_sql).await {
                for r in &list.rows {
                    let iname = Self::v38_text(r, 0);
                    if iname.is_empty() {
                        continue;
                    }
                    let uniq = Self::v38_text(r, 1) == "1";
                    let is_primary = Self::v38_text(r, 2) == "pk";
                    let info_sql = format!(
                        "SELECT name FROM pragma_index_info('{}') ORDER BY seqno",
                        Self::v38_sqlq(&iname)
                    );
                    let mut col_names: Vec<String> = Vec::new();
                    let mut indkey_parts: Vec<String> = Vec::new();
                    if let Ok(info) = db.query(&info_sql).await {
                        for (i, ir) in info.rows.iter().enumerate() {
                            let cn = Self::v38_text(ir, 0);
                            if !cn.is_empty() {
                                col_names.push(cn);
                                indkey_parts.push((i + 1).to_string());
                            }
                        }
                    }
                    let columns_str = col_names.join(",");
                    let nkeyatts = col_names.len().to_string();
                    let indkey_str = indkey_parts.join(" ");
                    rows.push(vec![
                        Some(iname.as_bytes().to_vec()),
                        Some(columns_str.into_bytes()),
                        Some(if uniq { b"t".to_vec() } else { b"f".to_vec() }),
                        Some(if is_primary { b"t".to_vec() } else { b"f".to_vec() }),
                        None,
                        Some(b"btree".to_vec()),
                        Some(nkeyatts.into_bytes()),
                        Some(indkey_str.into_bytes()),
                        None,
                    ]);
                }
            }
        }
        let n = rows.len();
        Ok(DbResponse { columns: cols, rows, rows_affected: n })
    }

    // === PATCH v42: DBX 列默认值 (03_attrdef) ===
    // 用 PRAGMA table_info 的 dflt_value 合成 (attname, pg_get_expr) 两列。
    // table=None 时返回 2 列 0 行 (供 extended Describe 宣告列数)。
    async fn v42_get_attrdef(
        db: &Arc<DbHandler>,
        table: Option<&str>,
    ) -> Result<DbResponse, PgSqliteError> {
        let cols = vec![
            "attname".to_string(),
            "pg_get_expr".to_string(),
        ];
        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        if let Some(table) = table {
            let sql = format!(
                "PRAGMA table_info('{}')",
                Self::v38_sqlq(table)
            );
            if let Ok(res) = db.query(&sql).await {
                for r in &res.rows {
                    let cname = Self::v38_text(r, 1);
                    let dflt = Self::v38_text(r, 4);
                    // 仅返回有默认值的列 (对齐 PG: 无默认值则无 pg_attrdef 行)
                    if !dflt.is_empty() {
                        rows.push(vec![
                            Some(cname.into_bytes()),
                            Some(dflt.into_bytes()),
                        ]);
                    }
                }
            }
        }
        let n = rows.len();
        Ok(DbResponse { columns: cols, rows, rows_affected: n })
    }

    // 从 dbx 列默认值查询提取表名: WHERE ... c.relname = <tbl>。
    // 参数化版本被 v29i 探测替换成 c.relname = NULL, 提取不到则返回 None。
    fn v42_extract_attrdef_table(query: &str) -> Option<String> {
        let marker = "relname = '";
        if let Some(pos) = query.to_lowercase().find(marker) {
            let rest = &query[pos + marker.len()..];
            let q = 39u8 as char;
            if let Some(end) = rest.find(q) {
                let raw = &rest[..end];
                if !raw.is_empty() {
                    return Some(raw.to_string());
                }
            }
        }
        None
    }
    fn v40_extract_index_table(query: &str) -> Option<String> {
        let marker = "relname = '";
        if let Some(pos) = query.to_lowercase().find(marker) {
            let rest = &query[pos + marker.len()..];
            let q = 39u8 as char;
            if let Some(end) = rest.find(q) {
                let raw = &rest[..end];
                if !raw.is_empty() {
                    return Some(raw.to_string());
                }
            }
        }
        None
    }
    async fn handle_dbeaver_columns_query_if_match(
        query: &str,
        db: &DbHandler,
        session: Option<&Arc<SessionState>>,
    ) -> Option<Result<DbResponse, PgSqliteError>> {
        let lower = query.to_lowercase();
        // 指纹: from pg_attribute + 投影含 column_name / is_pk (DBeaver 特征)
        if !lower.contains("pg_attribute")
            || !lower.contains("as column_name")
            || !lower.contains("as is_pk")
        {
            return None;
        }

        // 抽表名: quote_ident('schema') || '.' || quote_ident('table')
        let table: String = {
            let re_concat = regex::Regex::new(
                r"quote_ident\('([^']+)'\)\s*\|\|\s*'\.'\s*\|\|\s*quote_ident\('([^']+)'\)",
            )
            .ok()?;
            if let Some(c) = re_concat.captures(query) {
                c.get(2).map(|m| m.as_str().to_string())
            } else {
                // 退化: attrelid = 'schema.table'
                let re_reg =
                    regex::Regex::new(r"attrelid\s*=\s*'((?:[^'.]+\.)?[^']+)'").ok()?;
                re_reg.captures(query).and_then(|c| {
                    c.get(1).map(|m| {
                        let s = m.as_str();
                        s.rsplit('.').next().unwrap_or(s).to_string()
                    })
                })
            }
        }?;

        let session_id = session?.id;

        let rows_data = match db.connection_manager().execute_with_session(&session_id, |conn| {
            let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
            let mut out: Vec<(i32, String, String, i32, Option<String>, i32)> = Vec::new();
            let mut q = stmt.query([])?;
            while let Some(r) = q.next()? {
                let cid: i32 = r.get(0)?;
                let name: String = r.get(1)?;
                let type_name: String = r.get(2)?;
                let notnull: i32 = r.get(3)?;
                let dflt: Option<String> = r.get(4)?;
                let pk: i32 = r.get(5)?;
                out.push((cid, name, type_name, notnull, dflt, pk));
            }
            Ok(out)
        }) {
            Ok(d) => d,
            Err(_) => {
                return Some(Ok(DbResponse {
                    columns: vec!["column_name".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                }));
            }
        };

        let columns = vec![
            "column_name".to_string(),
            "full_type".to_string(),
            "is_nullable".to_string(),
            "column_default".to_string(),
            "is_pk".to_string(),
            "column_comment".to_string(),
            "column_extra".to_string(),
            "numeric_precision".to_string(),
            "numeric_scale".to_string(),
            "character_maximum_length".to_string(),
            "enum_values".to_string(),
        ];

        let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        for (_, name, type_name, notnull, dflt, pk) in rows_data {
            let full_type = type_name.clone();
            let is_nullable = if notnull != 0 { "NO" } else { "YES" };
            let column_default = dflt.unwrap_or_default();
            let is_pk = if pk != 0 { "t" } else { "f" };
            let (num_prec, num_scale, char_max) = Self::parse_pg_type_modifiers(&type_name);
            rows.push(vec![
                Some(name.into_bytes()),
                Some(full_type.into_bytes()),
                Some(is_nullable.to_string().into_bytes()),
                if column_default.is_empty() {
                    None
                } else {
                    Some(column_default.into_bytes())
                },
                Some(is_pk.to_string().into_bytes()),
                None, // column_comment
                None, // column_extra
                num_prec,
                num_scale,
                char_max,
                None, // enum_values
            ]);
        }

        Some(Ok(DbResponse {
            columns,
            rows,
            rows_affected: 0,
        }))
    }
    pub async fn handle_information_schema_columns_query_with_session(select: &Select, db: &DbHandler, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling information_schema.columns query");

        // Define information_schema.columns columns (PostgreSQL standard)
        let all_columns = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "column_name".to_string(),
            "ordinal_position".to_string(),
            "column_default".to_string(),
            "is_nullable".to_string(),
            "data_type".to_string(),
            "character_maximum_length".to_string(),
            "character_octet_length".to_string(),
            "numeric_precision".to_string(),
            "numeric_precision_radix".to_string(),
            "numeric_scale".to_string(),
            "datetime_precision".to_string(),
            "interval_type".to_string(),
            "interval_precision".to_string(),
            "character_set_catalog".to_string(),
            "character_set_schema".to_string(),
            "character_set_name".to_string(),
            "collation_catalog".to_string(),
            "collation_schema".to_string(),
            "collation_name".to_string(),
            "domain_catalog".to_string(),
            "domain_schema".to_string(),
            "domain_name".to_string(),
            "udt_catalog".to_string(),
            "udt_schema".to_string(),
            "udt_name".to_string(),
            "scope_catalog".to_string(),
            "scope_schema".to_string(),
            "scope_name".to_string(),
            "maximum_cardinality".to_string(),
            "dtd_identifier".to_string(),
            "is_self_referencing".to_string(),
            "is_identity".to_string(),
            "identity_generation".to_string(),
            "identity_start".to_string(),
            "identity_increment".to_string(),
            "identity_maximum".to_string(),
            "identity_minimum".to_string(),
            "identity_cycle".to_string(),
            "is_generated".to_string(),
            "generation_expression".to_string(),
            "is_updatable".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Check for WHERE clause filtering
        let table_filter = if let Some(ref where_clause) = select.selection {
            Self::extract_table_name_filter(where_clause)
        } else {
            None
        };

        // Get list of tables from SQLite
        let tables_query = if let Some(table_name) = &table_filter {
            format!("SELECT name FROM sqlite_master WHERE type='table' AND name = '{}' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'", table_name)
        } else {
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'".to_string()
        };

        let tables_response = match db.connection_manager().execute_with_session(session_id, |conn| {
            let mut stmt = conn.prepare(&tables_query)?;
            let mut rows = Vec::new();
            let mut query_rows = stmt.query([])?;
            while let Some(row) = query_rows.next()? {
                let name: String = row.get(0)?;
                rows.push(vec![Some(name.into_bytes())]);
            }
            Ok(DbResponse {
                columns: vec!["name".to_string()],
                rows,
                rows_affected: 0,
            })
        }) {
            Ok(response) => response,
            Err(_) => return Ok(DbResponse {
                columns: selected_columns,
                rows: vec![],
                rows_affected: 0,
            }),
        };

        let mut rows = Vec::new();

        // Process each table
        for table_row in &tables_response.rows {
            if let Some(Some(table_name_bytes)) = table_row.first() {
                let table_name = String::from_utf8_lossy(table_name_bytes).to_string();

                // Get column information using PRAGMA table_info
                let pragma_query = format!("PRAGMA table_info({})", table_name);
                let table_info_response = match db.connection_manager().execute_with_session(session_id, |conn| {
                    let mut stmt = conn.prepare(&pragma_query)?;
                    let mut rows = Vec::new();
                    let mut query_rows = stmt.query([])?;
                    while let Some(row) = query_rows.next()? {
                        let cid: i32 = row.get(0)?;
                        let name: String = row.get(1)?;
                        let type_name: String = row.get(2)?;
                        let not_null: i32 = row.get(3)?;
                        let default_value: Option<String> = row.get(4)?;
                        let pk: i32 = row.get(5)?;

                        let row_data = vec![
                            Some(cid.to_string().into_bytes()),
                            Some(name.into_bytes()),
                            Some(type_name.into_bytes()),
                            Some(not_null.to_string().into_bytes()),
                            default_value.map(|v| v.into_bytes()),
                            Some(pk.to_string().into_bytes()),
                        ];
                        rows.push(row_data);
                    }
                    Ok(DbResponse {
                        columns: vec!["cid".to_string(), "name".to_string(), "type".to_string(), "notnull".to_string(), "dflt_value".to_string(), "pk".to_string()],
                        rows,
                        rows_affected: 0,
                    })
                }) {
                    Ok(response) => response,
                    Err(_) => continue,
                };

                // Process each column
                for (ordinal, column_row) in table_info_response.rows.iter().enumerate() {
                    if column_row.len() >= 6
                        && let (Some(Some(name_bytes)), Some(Some(type_bytes)), Some(Some(notnull_bytes)), default_opt, Some(Some(pk_bytes))) =
                            (column_row.get(1), column_row.get(2), column_row.get(3), column_row.get(4), column_row.get(5)) {

                            let column_name = String::from_utf8_lossy(name_bytes).to_string();
                            let sqlite_type = String::from_utf8_lossy(type_bytes).to_string();
                            let not_null = String::from_utf8_lossy(notnull_bytes) == "1";
                            let default_value = match default_opt {
                                Some(Some(default_bytes)) => String::from_utf8_lossy(default_bytes),
                                _ => "".into(),
                            };
                            let is_primary_key = String::from_utf8_lossy(pk_bytes) == "1";

                            // First try to get type from __pgsqlite_schema, then fall back to SQLite type
                            let pg_type = match db.connection_manager().execute_with_session(session_id, |conn| {
                                // Check if __pgsqlite_schema exists first
                                let table_exists: i32 = conn.query_row(
                                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_schema'",
                                    [],
                                    |row| row.get(0)
                                ).unwrap_or(0);

                                if table_exists > 0 {
                                    let query = "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ? AND column_name = ?";
                                    let mut stmt = conn.prepare(query)?;
                                    let result = stmt.query_row([&table_name, &column_name], |row| {
                                        row.get::<_, String>(0)
                                    });
                                    Ok(result)
                                } else {
                                    Err(rusqlite::Error::InvalidPath("__pgsqlite_schema not found".into()))
                                }
                            }) {
                                Ok(Ok(stored_type)) => stored_type,
                                _ => sqlite_type.clone(),
                            };

                            // Map type to PostgreSQL type
                            let (pg_data_type, char_max_length, numeric_precision, numeric_scale) =
                                Self::map_sqlite_type_to_pg_column_info(&pg_type);

                            // Determine nullability
                            let is_nullable = if not_null || is_primary_key { "NO" } else { "YES" };

                            // Handle default value
                            let column_default = if default_value.is_empty() || default_value == "NULL" {
                                None
                            } else {
                                Some(default_value.to_string().into_bytes())
                            };

                            let full_row: Vec<Option<Vec<u8>>> = vec![
                                Some("main".to_string().into_bytes()),                    // table_catalog
                                Some("public".to_string().into_bytes()),                 // table_schema
                                Some(table_name.clone().into_bytes()),                   // table_name
                                Some(column_name.clone().into_bytes()),                 // column_name
                                Some((ordinal + 1).to_string().into_bytes()),           // ordinal_position (1-based)
                                column_default,                                          // column_default
                                Some(is_nullable.to_string().into_bytes()),             // is_nullable
                                Some(pg_data_type.clone().into_bytes()),                // data_type
                                char_max_length.map(|v| v.to_string().into_bytes()),    // character_maximum_length
                                char_max_length.map(|v| v.to_string().into_bytes()),    // character_octet_length
                                numeric_precision.map(|v| v.to_string().into_bytes()),  // numeric_precision
                                numeric_precision.map(|_| "10".to_string().into_bytes()), // numeric_precision_radix
                                numeric_scale.map(|v| v.to_string().into_bytes()),      // numeric_scale
                                None,                                                    // datetime_precision
                                None,                                                    // interval_type
                                None,                                                    // interval_precision
                                None,                                                    // character_set_catalog
                                None,                                                    // character_set_schema
                                None,                                                    // character_set_name
                                None,                                                    // collation_catalog
                                None,                                                    // collation_schema
                                None,                                                    // collation_name
                                None,                                                    // domain_catalog
                                None,                                                    // domain_schema
                                None,                                                    // domain_name
                                Some("main".to_string().into_bytes()),                  // udt_catalog
                                Some("pg_catalog".to_string().into_bytes()),            // udt_schema
                                Some(pg_data_type.clone().into_bytes()),                // udt_name
                                None,                                                    // scope_catalog
                                None,                                                    // scope_schema
                                None,                                                    // scope_name
                                None,                                                    // maximum_cardinality
                                Some((ordinal + 1).to_string().into_bytes()),           // dtd_identifier
                                Some("NO".to_string().into_bytes()),                    // is_self_referencing
                                Some("NO".to_string().into_bytes()),                    // is_identity
                                None,                                                    // identity_generation
                                None,                                                    // identity_start
                                None,                                                    // identity_increment
                                None,                                                    // identity_maximum
                                None,                                                    // identity_minimum
                                Some("NO".to_string().into_bytes()),                    // identity_cycle
                                Some("NEVER".to_string().into_bytes()),                 // is_generated
                                None,                                                    // generation_expression
                                Some("YES".to_string().into_bytes()),                   // is_updatable
                            ];

                            // Project only the requested columns
                            let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                                .map(|&idx| full_row[idx].clone())
                                .collect();

                            rows.push(projected_row);
                        }
                }
            }
        }

        let rows_affected = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected,
        })
    }
    async fn handle_information_schema_tables_query(select: &Select, db: &DbHandler) -> DbResponse {
        debug!("Handling information_schema.tables query");

        // Get list of tables and views from SQLite
        let tables_response = match db.query("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'").await {
            Ok(response) => response,
            Err(_) => return DbResponse {
                columns: vec!["table_name".to_string()],
                rows: vec![],
                rows_affected: 0,
            },
        };

        // Define information_schema.tables columns (enhanced with all PostgreSQL standard columns)
        let all_columns = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "table_type".to_string(),
            "self_referencing_column_name".to_string(),
            "reference_generation".to_string(),
            "user_defined_type_catalog".to_string(),
            "user_defined_type_schema".to_string(),
            "user_defined_type_name".to_string(),
            "is_insertable_into".to_string(),
            "is_typed".to_string(),
            "commit_action".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Check for WHERE clause filtering
        let table_filters = if let Some(ref where_clause) = select.selection {
            Self::extract_table_name_filters(where_clause)
        } else {
            Vec::new()
        };

        // Build rows
        let mut rows = Vec::new();
        for table_row in &tables_response.rows {
            if table_row.len() >= 2
                && let (Some(Some(table_name_bytes)), Some(Some(table_type_bytes))) =
                    (table_row.first(), table_row.get(1)) {
                    let table_name = String::from_utf8_lossy(table_name_bytes).to_string();
                    let sqlite_type = String::from_utf8_lossy(table_type_bytes).to_string();

                    // Apply WHERE clause filtering if present
                    if !table_filters.is_empty() && !table_filters.contains(&table_name) {
                        continue;
                    }

                    // Map SQLite type to PostgreSQL table_type
                    let table_type = match sqlite_type.as_str() {
                        "table" => "BASE TABLE",
                        "view" => "VIEW",
                        _ => "BASE TABLE", // Default fallback
                    };

                    // Determine if table is insertable (views are not)
                    let is_insertable = if table_type == "VIEW" { "NO" } else { "YES" };

                    // Create full row with all columns
                    let full_row: Vec<Option<Vec<u8>>> = vec![
                        Some("main".to_string().into_bytes()),        // table_catalog
                        Some("public".to_string().into_bytes()),      // table_schema
                        Some(table_name.into_bytes()),                // table_name
                        Some(table_type.to_string().into_bytes()),    // table_type
                        None,                                         // self_referencing_column_name
                        None,                                         // reference_generation
                        None,                                         // user_defined_type_catalog
                        None,                                         // user_defined_type_schema
                        None,                                         // user_defined_type_name
                        Some(is_insertable.to_string().into_bytes()), // is_insertable_into
                        Some("NO".to_string().into_bytes()),          // is_typed
                        None,                                         // commit_action
                    ];

                    // Project only the requested columns
                    let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                        .map(|&idx| full_row[idx].clone())
                        .collect();

                    rows.push(projected_row);
                }
        }
        
        let rows_count = rows.len();
        DbResponse {
            columns: selected_columns,
            rows,
            rows_affected: rows_count,
        }
    }
    fn map_sqlite_type_to_pg_column_info(sqlite_type: &str) -> (String, Option<i32>, Option<i32>, Option<i32>) {
        let sqlite_type_upper = sqlite_type.to_uppercase();

        // Handle parametric types like VARCHAR(255), DECIMAL(10,2)
        if let Some(paren_pos) = sqlite_type_upper.find('(') {
            let base_type = &sqlite_type_upper[..paren_pos];
            let params_str = &sqlite_type_upper[paren_pos+1..];
            if let Some(close_paren) = params_str.find(')') {
                let params_str = &params_str[..close_paren];
                let params: Vec<&str> = params_str.split(',').map(|s| s.trim()).collect();

                match base_type {
                    "VARCHAR" | "CHAR" | "CHARACTER VARYING" => {
                        let length = params.first().and_then(|p| p.parse().ok()).unwrap_or(255);
                        return ("character varying".to_string(), Some(length), None, None);
                    },
                    "DECIMAL" | "NUMERIC" => {
                        let precision = params.first().and_then(|p| p.parse().ok()).unwrap_or(10);
                        let scale = params.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
                        return ("numeric".to_string(), None, Some(precision), Some(scale));
                    },
                    _ => {}
                }
            }
        }

        // Handle base types
        match sqlite_type_upper.as_str() {
            "INTEGER" | "INT" => ("integer".to_string(), None, Some(32), Some(0)),
            "BIGINT" => ("bigint".to_string(), None, Some(64), Some(0)),
            "SMALLINT" => ("smallint".to_string(), None, Some(16), Some(0)),
            "REAL" | "FLOAT" => ("real".to_string(), None, Some(24), None),
            "DOUBLE" | "DOUBLE PRECISION" => ("double precision".to_string(), None, Some(53), None),
            "TEXT" => ("text".to_string(), None, None, None),
            "BLOB" => ("bytea".to_string(), None, None, None),
            "BOOLEAN" | "BOOL" => ("boolean".to_string(), None, None, None),
            "DATE" => ("date".to_string(), None, None, None),
            "TIME" => ("time without time zone".to_string(), None, None, None),
            "TIMESTAMP" | "DATETIME" => ("timestamp without time zone".to_string(), None, None, None),
            "UUID" => ("uuid".to_string(), None, None, None),
            "JSON" => ("json".to_string(), None, None, None),
            "JSONB" => ("jsonb".to_string(), None, None, None),
            "VARCHAR" | "CHARACTER VARYING" => ("character varying".to_string(), None, None, None),
            "CHAR" | "CHARACTER" => ("character".to_string(), None, None, None),
            _ => {
                // Default fallback for unknown types
                if sqlite_type_upper.contains("CHAR") || sqlite_type_upper.contains("TEXT") {
                    ("text".to_string(), None, None, None)
                } else if sqlite_type_upper.contains("INT") {
                    ("integer".to_string(), None, Some(32), Some(0))
                } else if sqlite_type_upper.contains("REAL") || sqlite_type_upper.contains("FLOAT") {
                    ("real".to_string(), None, Some(24), None)
                } else {
                    ("text".to_string(), None, None, None)
                }
            }
        }
    }
    fn from_is_sqlite_resolvable(select: &sqlparser::ast::Select) -> bool {
        use sqlparser::ast::{ObjectNamePart, TableFactor};

        // === PATCH v23: information_schema objects backed by real SQLite views ===
        // The six v14 objects (tables/columns/schemata/key_column_usage/
        // table_constraints/referential_constraints) plus the seventeen v33
        // objects all have CREATE VIEW statements in the migration registry,
        // and SchemaPrefixTranslator rewrites information_schema.<x> to
        // information_schema_<x>. Keep in sync with V33_ISCHEMA_VIEWS in
        // schema_prefix_translator.rs. Objects NOT listed here (routines,
        // views, check_constraints, triggers, ...) exist only as Rust handlers
        // and must stay on the handler path — delegating them would raise
        // `no such table` and abort the whole GUI metadata transaction.
        const ISCHEMA_SQLITE_RESOLVABLE: &[&str] = &[
            // v14
            "tables",
            "columns",
            "schemata",
            "key_column_usage",
            "table_constraints",
            "referential_constraints",
            // v33
            "applicable_roles",
            "character_sets",
            "collations",
            "column_privileges",
            "column_udt_usage",
            "constraint_column_usage",
            "domain_constraints",
            "domains",
            "element_types",
            "enabled_roles",
            "information_schema_catalog_name",
            "parameters",
            "role_table_grants",
            "sequences",
            "table_privileges",
            "view_column_usage",
            "view_table_usage",
        ];

        fn factor_ok(factor: &TableFactor) -> bool {
            match factor {
                TableFactor::Table { name, .. } => {
                    let parts = &name.0;
                    if parts.len() <= 1 {
                        return true;
                    }
                    match &parts[parts.len() - 2] {
                        ObjectNamePart::Identifier(ident) => {
                            if ident.value.eq_ignore_ascii_case("pg_catalog") {
                                true
                            } else if ident.value.eq_ignore_ascii_case("information_schema") {
                                // === PATCH v23: only objects backed by a real
                                // SQLite view are safe to delegate. The v14 six and
                                // the v33 seventeen are rewritten by
                                // SchemaPrefixTranslator to information_schema_<x>.
                                if let Some(ObjectNamePart::Identifier(last)) = parts.last() {
                                    ISCHEMA_SQLITE_RESOLVABLE
                                        .iter()
                                        .any(|o| last.value.eq_ignore_ascii_case(o))
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        }
                        _ => false,
                    }
                }
                // Sub-queries / table functions are executed by the SQL engine
                // anyway; nothing for the handler to hijack.
                _ => true,
            }
        }

        for twj in &select.from {
            if !factor_ok(&twj.relation) {
                return false;
            }
            for join in &twj.joins {
                if !factor_ok(&join.relation) {
                    return false;
                }
            }
        }
        true
    }
    fn query_needs_sql_engine(
        query: &sqlparser::ast::Query,
        select: &sqlparser::ast::Select,
    ) -> bool {
        use sqlparser::ast::GroupByExpr;

        // === PATCH v18: schema-qualified names SQLite cannot resolve ===
        // SQLite has no schema namespace. `information_schema.tables` simply does
        // not exist there (the real view is `information_schema_tables`), so
        // delegating such a query would raise `no such table`, abort the
        // transaction and take the whole GUI metadata tree down with it — strictly
        // worse than the handler's (wrong but harmless) result set. Only pg_catalog
        // qualifiers are safe because SchemaPrefixTranslator strips them.
        if !Self::from_is_sqlite_resolvable(select) {
            return false;
        }

        // GROUP BY / HAVING —— handler 完全没有分组概念
        let has_group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, modifiers) => {
                !exprs.is_empty() || !modifiers.is_empty()
            }
            GroupByExpr::All(_) => true,
        };
        if has_group_by || select.having.is_some() {
            return true;
        }

        // ORDER BY —— handler 原样吐出内部顺序，排序被静默丢弃
        if query.order_by.is_some() {
            return true;
        }

        // 投影里的聚合函数 —— handler 找不到同名列，一律填 NULL
        const AGGREGATES: [&str; 14] = [
            "count(",
            "sum(",
            "avg(",
            "min(",
            "max(",
            "array_agg(",
            "string_agg(",
            "json_agg(",
            "jsonb_agg(",
            "bool_and(",
            "bool_or(",
            "every(",
            "bit_and(",
            "bit_or(",
        ];
        let projection = select
            .projection
            .iter()
            .map(|item| item.to_string().to_lowercase())
            .collect::<Vec<_>>()
            .join(" , ");
        AGGREGATES.iter().any(|f| projection.contains(f))
    }
    fn select_uses_udf_backed_system_function(select: &sqlparser::ast::Select) -> bool {
        const UDF_BACKED: [&str; 8] = [
            "format_type",
            "pg_get_expr",
            "pg_get_indexdef",
            "pg_get_constraintdef",
            "to_regtype",
            "pg_get_userbyid",
            "pg_table_is_visible",
            "pg_size_pretty",
        ];
        let rendered = select.to_string().to_lowercase();
        UDF_BACKED.iter().any(|f| rendered.contains(f))
    }
    fn is_count_star_projection(projection: &[SelectItem]) -> bool {
        if projection.len() != 1 {
            return false;
        }

        let expr = match &projection[0] {
            SelectItem::UnnamedExpr(expr) => expr,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return false,
        };

        let Expr::Function(function) = expr else {
            return false;
        };

        if Self::function_name(function).as_deref() != Some("count") {
            return false;
        }

        matches!(
            &function.args,
            sqlparser::ast::FunctionArguments::List(arg_list)
                if arg_list.args.len() == 1
                    && matches!(
                        &arg_list.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    )
        )
    }
    fn count_response(count: usize) -> DbResponse {
        DbResponse {
            columns: vec!["count".to_string()],
            rows: vec![vec![Some(count.to_string().into_bytes())]],
            rows_affected: 1,
        }
    }
    fn extract_table_name_filters(where_clause: &Expr) -> Vec<String> {
        match where_clause {
            Expr::BinaryOp { left, op, right } => {
                // Handle "table_name = 'value'"
                if let (Expr::Identifier(ident), sqlparser::ast::BinaryOperator::Eq, Expr::Value(value_with_span)) =
                    (left.as_ref(), op, right.as_ref())
                    && ident.value.to_lowercase() == "table_name"
                        && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                            return vec![value.clone()];
                        }
                // Handle "'value' = table_name" (reversed)
                if let (Expr::Value(value_with_span), sqlparser::ast::BinaryOperator::Eq, Expr::Identifier(ident)) =
                    (left.as_ref(), op, right.as_ref())
                    && ident.value.to_lowercase() == "table_name"
                        && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                            return vec![value.clone()];
                        }
                // Handle compound identifiers like "information_schema.tables.table_name = 'value'"
                if let (Expr::CompoundIdentifier(parts), sqlparser::ast::BinaryOperator::Eq, Expr::Value(value_with_span)) =
                    (left.as_ref(), op, right.as_ref())
                    && let Some(last_part) = parts.last()
                        && last_part.value.to_lowercase() == "table_name"
                            && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                                return vec![value.clone()];
                            }
            }
            Expr::InList { expr, list, negated } => {
                // Handle "table_name IN ('value1', 'value2')"
                if !negated {
                    if let Expr::Identifier(ident) = expr.as_ref()
                        && ident.value.to_lowercase() == "table_name" {
                            let mut values = Vec::new();
                            for item in list {
                                if let Expr::Value(value_with_span) = item
                                    && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                                        values.push(value.clone());
                                    }
                            }
                            return values;
                        }
                    // Handle compound identifiers in IN clause
                    if let Expr::CompoundIdentifier(parts) = expr.as_ref()
                        && let Some(last_part) = parts.last()
                            && last_part.value.to_lowercase() == "table_name" {
                                let mut values = Vec::new();
                                for item in list {
                                    if let Expr::Value(value_with_span) = item
                                        && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                                            values.push(value.clone());
                                        }
                                }
                                return values;
                            }
                }
            }
            Expr::Nested(inner) => {
                return Self::extract_table_name_filters(inner);
            }
            _ => {}
        }
        Vec::new()
    }
    fn parse_select(sql: &str) -> sqlparser::ast::Select {
        use sqlparser::ast::Statement;
        let mut ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parse sql");
        match ast.remove(0) {
            Statement::Query(q) => match *q.body {
                SetExpr::Select(select) => *select,
                _ => panic!("not a SELECT"),
            },
            _ => panic!("not a query"),
        }
    }
    fn v23_tables_count_delegates_to_sqlite() {
        assert!(CatalogInterceptor::from_is_sqlite_resolvable(&parse_select(
            "SELECT count(*) FROM information_schema.tables"
        )));
    }
    fn v23_columns_count_delegates_to_sqlite() {
        assert!(CatalogInterceptor::from_is_sqlite_resolvable(&parse_select(
            "SELECT count(*) FROM information_schema.columns"
        )));
    }
    fn v23_schemata_group_by_delegates_to_sqlite() {
        assert!(CatalogInterceptor::from_is_sqlite_resolvable(&parse_select(
            "SELECT schema_name, count(*) FROM information_schema.schemata GROUP BY schema_name"
        )));
    }
    fn v23_key_column_usage_delegates_to_sqlite() {
        assert!(CatalogInterceptor::from_is_sqlite_resolvable(&parse_select(
            "SELECT count(*) FROM information_schema.key_column_usage"
        )));
    }
    fn v23_handler_only_routines_stays_on_handler() {
        assert!(!CatalogInterceptor::from_is_sqlite_resolvable(&parse_select(
            "SELECT count(*) FROM information_schema.routines"
        )));
    }
    fn v23_pg_catalog_still_delegates() {
        assert!(CatalogInterceptor::from_is_sqlite_resolvable(&parse_select(
            "SELECT count(*) FROM pg_catalog.pg_class"
        )));
    }
    fn v23_bare_table_name_still_delegates() {
        assert!(CatalogInterceptor::from_is_sqlite_resolvable(&parse_select(
            "SELECT count(*) FROM pg_class"
        )));
    }

pub async fn intercept_query(query: &str, db: Arc<DbHandler>, session: Option<Arc<SessionState>>) -> Option<Result<DbResponse, PgSqliteError>> {
        debug!("INTERCEPT_QUERY: {}", query);
        // Quick check to avoid parsing if not a catalog query
        let lower_query = query.to_lowercase();
        debug!("INTERCEPT: lower_query = {}", lower_query);
        
        // Check for cache status query
        if lower_query.contains("select * from pgsqlite_cache_status") {
            let (columns, rows) = crate::cache::format_cache_status_as_table();
            let rows_affected = rows.len();
            let response = DbResponse {
                columns,
                rows,
                rows_affected,
            };
            return Some(Ok(response));
        }
        
        // Special case: pg_catalog.version() should be handled by SQLite function, not catalog interceptor
        if lower_query.trim() == "select pg_catalog.version()" || 
           lower_query.trim() == "select version()" {
            return None;
        }
        // === v38: pgjdbc DatabaseMetaData 固定模板拦截 ===
        if let Some(r) = Self::v38_jdbc_metadata(query, &db).await {
            println!("INTERCEPT: v38 jdbc-metadata handled");
            return Some(r);
        }

        // === PATCH v40: DBeaver/DBX 字段列表查询 (01_columns) 专属短路 ===
        if let Some(r) = Self::handle_dbeaver_columns_query_if_match(query, &db, session.as_ref()).await {
            println!("INTERCEPT: DBeaver columns (01_columns) handled");
            return Some(r);
        }

        // === PATCH v40: dbx / DBeaver 取索引列表 (02_indexes) 专属短路 ===
        if lower_query.contains("pg_index") && lower_query.contains("array_agg") && !lower_query.contains("_pg_expandarray") {
            let tbl = Self::v40_extract_index_table(query);
            println!("INTERCEPT: dbx index list for table={:?}", tbl);
            return Some(Self::v40_get_index_list(&db, tbl.as_deref()).await);
        }

        // === PATCH v42: DBX 列默认值查询 (03_attrdef) 专属短路 ===
        if lower_query.contains("pg_attrdef")
            && lower_query.contains("pg_get_expr")
            && !lower_query.contains("as column_name")
        {
            let tbl = Self::v42_extract_attrdef_table(query);
            println!("INTERCEPT: dbx attrdef for table={:?}", tbl);
            return Some(Self::v42_get_attrdef(&db, tbl.as_deref()).await);
        }

        // Check for catalog tables
        let has_catalog_tables = lower_query.contains("pg_catalog") || lower_query.contains("pg_type") ||
           lower_query.contains("pg_namespace") || lower_query.contains("pg_range") ||
           lower_query.contains("pg_tablespace") ||
           lower_query.contains("pg_class") || lower_query.contains("pg_attribute") ||
           lower_query.contains("pg_enum") ||
           lower_query.contains("pg_description") || lower_query.contains("pg_roles") ||
           lower_query.contains("pg_user") || lower_query.contains("pg_authid") ||
           lower_query.contains("pg_stats") || lower_query.contains("pg_constraint") ||
           lower_query.contains("pg_depend") || lower_query.contains("pg_sequence") ||
           lower_query.contains("pg_trigger") || lower_query.contains("pg_settings") ||
           lower_query.contains("pg_collation") ||
           lower_query.contains("pg_replication_slots") ||
           lower_query.contains("pg_shdepend") ||
           lower_query.contains("pg_statistic") ||
           lower_query.contains("information_schema") ||
           lower_query.contains("pg_stat_") || lower_query.contains("pg_database") ||
           lower_query.contains("pg_foreign_data_wrapper");

        // Check for system functions
        let has_system_functions = lower_query.contains("to_regtype") ||
           lower_query.contains("pg_get_constraintdef") || lower_query.contains("pg_table_is_visible") ||
           lower_query.contains("format_type") || lower_query.contains("pg_get_expr") ||
           lower_query.contains("pg_get_userbyid") || lower_query.contains("pg_get_indexdef") ||
           lower_query.contains("pg_size_pretty");

        debug!("INTERCEPT: has_catalog_tables = {}, has_system_functions = {}", has_catalog_tables, has_system_functions);

        if !has_catalog_tables && !has_system_functions {
            debug!("INTERCEPT: Returning None (no catalog tables or system functions)");
            return None;
        }
        
        debug!("Intercepting catalog query: {}", query);
        debug!("INTERCEPT: After debug, about to check LIMIT 0");

        // Special handling for LIMIT 0 queries used for metadata
        if query.contains("LIMIT 0") {
            debug!("INTERCEPT: Found LIMIT 0, returning None");
            // Skipping LIMIT 0 catalog query
            return None;
        }
        debug!("INTERCEPT: No LIMIT 0, continuing");
        
        // First, remove schema prefixes from catalog tables
        debug!("INTERCEPT: About to call SchemaPrefixTranslator");
        let schema_translated = SchemaPrefixTranslator::translate_query(query);
        debug!("INTERCEPT: schema_translated = '{}'", schema_translated);

        // Then, try to translate regex operators if present
        debug!("INTERCEPT: About to call RegexTranslator");
        let query_to_parse = match RegexTranslator::translate_query(&schema_translated) {
            Ok(translated) => {
                if translated != query {
                    debug!("INTERCEPT: RegexTranslator changed query to: '{}'", translated);
                } else {
                    debug!("INTERCEPT: RegexTranslator made no changes");
                }
                translated
            }
            Err(e) => {
                debug!("INTERCEPT: RegexTranslator failed: {:?}", e);
                // Failed to translate regex operators
                query.to_string()
            }
        };
        debug!("INTERCEPT: query_to_parse = '{}'", query_to_parse);

        // Parse the query (keep JSON path placeholders for now)
        let dialect = PostgreSqlDialect {};
        // Parsing query for system functions
        match Parser::parse_sql(&dialect, &query_to_parse) {
            Ok(mut statements) => {
                if statements.len() == 1
                    && let Statement::Query(query_stmt) = &mut statements[0] {
                        // First check if query contains system functions that need processing
                        let contains_functions = Self::query_contains_system_functions(query_stmt);
                        // Query contains system functions
                        if contains_functions {
                            // Clone the query and process system functions
                            match Self::process_system_functions_in_query(query_stmt.clone(), db.clone()).await {
                                Ok(processed_query) => {
                                    // Convert the processed query back to SQL and execute
                                    let mut processed_sql = processed_query.to_string();
                                    // Processed system functions
                                    
                                    // Also translate regex operators
                                    match RegexTranslator::translate_query(&processed_sql) {
                                        Ok(translated) => {
                                            processed_sql = translated;
                                            debug!("Translated regex operators: {}", processed_sql);
                                        }
                                        Err(_) => {
                                            // Failed to translate regex operators
                                        }
                                    }
                                    
                                    // Update the query_to_parse with the processed SQL and continue
                                    // This ensures that catalog queries are handled properly after system function processing
                                    debug!("System functions processed, continuing with catalog handling");
                                    
                                    // Check if the query contains catalog tables
                                    let contains_catalog_tables = processed_sql.to_lowercase().contains("pg_") || 
                                                                 processed_sql.to_lowercase().contains("information_schema");
                                    
                                    if !contains_catalog_tables {
                                        // This is a standalone system function query, execute it directly
                                        debug!("Executing standalone system function query: {}", processed_sql);
                                        match db.query(&processed_sql).await {
                                            Ok(response) => return Some(Ok(response)),
                                            Err(e) => return Some(Err(PgSqliteError::Sqlite(e))),
                                        }
                                    } else {
                                        // Re-parse the processed query to continue with catalog handling
                                        match Parser::parse_sql(&dialect, &processed_sql) {
                                            Ok(mut new_statements) => {
                                                if new_statements.len() == 1
                                                    && let Statement::Query(new_query) = &mut new_statements[0] {
                                                        // Replace the current query with the processed one
                                                        *query_stmt = new_query.clone();
                                                        // Continue to the catalog handling below
                                                    }
                                            }
                                            Err(e) => {
                                                // Failed to re-parse processed query
                                                return Some(Err(PgSqliteError::Protocol(format!("Failed to parse processed query: {e}"))));
                                            }
                                        }
                                    }
                                }
                                Err(_) => {
                                    // Error processing system functions
                                    // Continue with normal catalog handling
                                }
                            }
                        }
                        
                        // Normal catalog table handling
                        debug!("INTERCEPT: About to call handle_catalog_query");
                        if let Some(response) = Self::handle_catalog_query(query_stmt, db.clone(), session.clone()).await {
                            debug!("INTERCEPT: handle_catalog_query returned Some(response), columns: {}, rows: {}", response.columns.len(), response.rows.len());
                            return Some(Ok(response));
                        }
                        debug!("INTERCEPT: handle_catalog_query returned None");
                    }
                
                // If we translated the query but it's not a special catalog query,
                // execute the translated query directly
                if query_to_parse != query {
                    match db.query(&query_to_parse).await {
                        Ok(response) => return Some(Ok(response)),
                        Err(e) => return Some(Err(PgSqliteError::Sqlite(e))),
                    }
                }
            }
            Err(_) => return None,
        }

        None
    }

    /// Catalog tables that exist as real SQLite views/tables (populated by migrations and
    /// constraint_populator) and can therefore be queried with plain SQL once we return `None`
    /// to let the query fall through. `pg_stats` and `pg_tablespace` are deliberately excluded:
    /// per migrations v19 and v24 (src/migration/registry.rs), both are handled entirely by the
    /// catalog interceptor and have no backing SQLite view/table, so routing a JOIN into them to
    /// raw SQL would fail with "no such table".
    fn is_sqlite_backed_catalog(name: &str) -> bool {
        name.contains("pg_constraint") || name.contains("pg_index") ||
            name.contains("pg_depend") || name.contains("pg_proc") ||
            name.contains("pg_description") || name.contains("pg_roles") ||
            name.contains("pg_user")
    }

    async fn handle_catalog_query(query: &sqlparser::ast::Query, db: Arc<DbHandler>, session: Option<Arc<SessionState>>) -> Option<DbResponse> {
        debug!("handle_catalog_query called");
        debug!("HANDLE_CATALOG_QUERY: called with query");
        // Check if this is a SELECT from pg_catalog tables
        if let SetExpr::Select(select) = &*query.body {
            debug!("HANDLE_CATALOG_QUERY: Is SELECT query, from.len()={}, has_joins={}",
                     select.from.len(),
                     !select.from.is_empty() && !select.from[0].joins.is_empty());
            debug!("Is SELECT query, from.len()={}, has_joins={}",
                     select.from.len(),
                     !select.from.is_empty() && !select.from[0].joins.is_empty());
            // Check if this is a JOIN query involving catalog tables
            if !select.from.is_empty() && !select.from[0].joins.is_empty() {
                debug!("Detected as JOIN query");
                debug!("HANDLE_CATALOG_QUERY: Detected as JOIN query");

                // Check if this is a JOIN between information_schema tables
                if let TableFactor::Table { name: main_table, .. } = &select.from[0].relation {
                    let main_table_name = main_table.to_string().to_lowercase();
                    debug!("HANDLE_CATALOG_QUERY: Main table name: '{}'", main_table_name);

                    // Check if main table and all JOINs are information_schema tables
                    let is_information_schema_join = main_table_name.contains("information_schema") &&
                        select.from[0].joins.iter().all(|j| {
                            if let TableFactor::Table { name: join_table, .. } = &j.relation {
                                let join_table_name = join_table.to_string().to_lowercase();
                                debug!("HANDLE_CATALOG_QUERY: Join table name: '{}'", join_table_name);
                                join_table_name.contains("information_schema")
                            } else {
                                false
                            }
                        });

                    debug!("HANDLE_CATALOG_QUERY: is_information_schema_join = {}", is_information_schema_join);

                    if is_information_schema_join {
                        debug!("Detected information_schema JOIN query - translating and executing");
                        debug!("HANDLE_CATALOG_QUERY: Detected information_schema JOIN query");

                        // Information_schema tables exist as views with underscores, not dots
                        // e.g., information_schema_table_constraints instead of information_schema.table_constraints
                        // We need to translate the query to use the correct view names
                        if let Some(ref session) = session {
                            let session_id = session.id;
                            let mut query_str = query.to_string();

                            // Replace information_schema.table_name with information_schema_table_name
                            query_str = query_str.replace("information_schema.table_constraints", "information_schema_table_constraints");
                            query_str = query_str.replace("information_schema.key_column_usage", "information_schema_key_column_usage");
                            query_str = query_str.replace("information_schema.referential_constraints", "information_schema_referential_constraints");
                            query_str = query_str.replace("information_schema.columns", "information_schema_columns");
                            query_str = query_str.replace("information_schema.tables", "information_schema_tables");
                            query_str = query_str.replace("information_schema.schemata", "information_schema_schemata");

                            debug!("HANDLE_CATALOG_QUERY: Translated query: {}", query_str);

                            match db.connection_manager().execute_with_session(&session_id, |conn| {
                                debug!("Executing translated information_schema JOIN query: {}", query_str);

                                // Execute the translated query
                                let mut stmt = conn.prepare(&query_str)?;
                                let column_count = stmt.column_count();
                                let columns: Vec<String> = resolve_columns_with_legacy(
                                    &stmt,
                                    &query_str,
                                    &HashMap::new(),
                                    &TranslationMetadata::new(),
                                    false,
                                )?.into_iter().map(|m| m.wire_name).collect();

                                let rows_result: rusqlite::Result<Vec<Vec<Option<Vec<u8>>>>> = stmt.query_map([], |row| {
                                    let mut values = Vec::new();
                                    for i in 0..column_count {
                                        // Try to get as string, handle NULL values
                                        let value: rusqlite::Result<Option<String>> = row.get(i);
                                        match value {
                                            Ok(Some(s)) => values.push(Some(s.into_bytes())),
                                            Ok(None) => values.push(None),
                                            Err(_) => values.push(None),
                                        }
                                    }
                                    Ok(values)
                                })?.collect();

                                match rows_result {
                                    Ok(rows) => {
                                        let rows_affected = rows.len();
                                        Ok(DbResponse {
                                            columns,
                                            rows,
                                            rows_affected,
                                        })
                                    }
                                    Err(e) => {
                                        debug!("Failed to execute translated JOIN query: {}", e);
                                        Err(e)
                                    }
                                }
                            }) {
                                Ok(response) => {
                                    debug!("Successfully executed translated JOIN query, returning {} rows with {} columns",
                                           response.rows_affected, response.columns.len());
                                    debug!("HANDLE_CATALOG_QUERY: Successfully executed translated JOIN, {} rows, {} columns",
                                            response.rows_affected, response.columns.len());
                                    return Some(response);
                                }
                                Err(e) => {
                                    debug!("Failed to execute translated JOIN: {}", e);
                                    debug!("HANDLE_CATALOG_QUERY: Failed to execute translated JOIN: {}", e);
                                    // Fall through to try other methods
                                }
                            }
                        }
                    }
                }

                // Check if the query contains system functions that need special handling
                let query_str = query.to_string();
                let contains_system_functions = query_str.contains("pg_table_is_visible") ||
                                              query_str.contains("pg_get_constraintdef") ||
                                              query_str.contains("format_type") ||
                                              query_str.contains("pg_get_expr");

                if contains_system_functions {
                    // This query contains system functions that need special handling
                    debug!("JOIN query contains system functions, handling specially");
                    
                    // For SQLAlchemy table existence checks, we can handle this specially
                    // The query pattern is checking if a table exists by joining pg_class and pg_namespace
                    // and using pg_table_is_visible
                    if query_str.contains("pg_table_is_visible") && query_str.contains("pg_class.relname") {
                        // Extract the table name being checked
                        let table_name_pattern = regex::Regex::new(r"relname\s*=\s*'([^']+)'").unwrap();
                        if let Some(captures) = table_name_pattern.captures(&query_str)
                            && let Some(table_name) = captures.get(1) {
                                let table_name_str = table_name.as_str();
                                debug!("Checking existence of table: {}", table_name_str);
                                
                                // Check if the table exists in SQLite
                                let check_query = format!(
                                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{table_name_str}' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'"
                                );
                                
                                match db.query(&check_query).await {
                                    Ok(response) => {
                                        // If we found the table, return a result with the table name
                                        if !response.rows.is_empty() {
                                            debug!("Table {} exists", table_name_str);
                                            let db_response = DbResponse {
                                                columns: vec!["relname".to_string()],
                                                rows: vec![vec![Some(table_name_str.as_bytes().to_vec())]],
                                                rows_affected: 1,
                                            };
                                            return Some(db_response);
                                        } else {
                                            debug!("Table {} does not exist", table_name_str);
                                            let db_response = DbResponse {
                                                columns: vec!["relname".to_string()],
                                                rows: vec![],
                                                rows_affected: 0,
                                            };
                                            return Some(db_response);
                                        }
                                    }
                                    Err(_) => {
                                        // Error checking table existence
                                    }
                                }
                            }
                    }
                    
                    // For other system functions, fall through to default handling
                    debug!("Unable to handle this specific system function query pattern");
                } else {
                    debug!("In JOIN query else block (no system functions)");
                    // Check if this is a pg_attribute JOIN query that we should handle
                    if let TableFactor::Table { name, .. } = &select.from[0].relation {
                        let table_name = name.to_string().to_lowercase();
                        debug!("Checking JOIN query with main table: '{}'", table_name);

                        // Handle pg_attribute JOIN queries specially to avoid connection context issues
                        let has_catalog_joins = select.from[0].joins.iter().any(|j| {
                            if let TableFactor::Table { name, .. } = &j.relation {
                                let join_table = name.to_string().to_lowercase();
                                debug!("  Found JOIN table: '{}'", join_table);
                                join_table.contains("pg_attribute") || join_table.contains("pg_type")
                            } else {
                                false
                            }
                        });

                        if (table_name.contains("pg_class") || table_name.contains("pg_attribute")) && has_catalog_joins {
                            debug!("Intercepting pg_attribute JOIN query for special handling");
                            // Execute the query using session connection to ensure table visibility
                            if let Some(ref session) = session {
                                if let Some(db_handler) = session.get_db_handler().await {
                                    let session_id = session.id;
                                    let query_str = query.to_string();


                                    match db_handler.with_session_connection(&session_id, |conn| {
                                        debug!("Executing catalog JOIN query with session connection: {}", query_str);

                                        // Execute the query directly with the session's connection
                                        let mut stmt = conn.prepare(&query_str)?;
                                        let column_count = stmt.column_count();
                                        let columns: Vec<String> = resolve_columns_with_legacy(
                                            &stmt,
                                            &query_str,
                                            &HashMap::new(),
                                            &TranslationMetadata::new(),
                                            false,
                                        )?.into_iter().map(|m| m.wire_name).collect();

                                        let rows_result: rusqlite::Result<Vec<Vec<Option<Vec<u8>>>>> = stmt.query_map([], |row| {
                                            let mut values = Vec::new();
                                            for i in 0..column_count {
                                                let value: Option<String> = row.get(i).ok();
                                                values.push(value.map(|s| s.into_bytes()));
                                            }
                                            Ok(values)
                                        })?.collect();

                                        match rows_result {
                                            Ok(rows) => {
                                                let rows_affected = rows.len();
                                                Ok(DbResponse {
                                                    columns,
                                                    rows,
                                                    rows_affected,
                                                })
                                            }
                                            Err(e) => {
                                                debug!("Failed to execute catalog JOIN query: {}", e);
                                                Err(e)
                                            }
                                        }
                                    }).await {
                                        Ok(response) => {
                                            debug!("Successfully executed catalog JOIN query, returning {} rows", response.rows_affected);
                                            return Some(response);
                                        }
                                        Err(e) => {
                                            debug!("Failed to execute catalog JOIN with session connection: {}", e);
                                            // Fall through to try other methods
                                        }
                                    }
                                }
                            }
                        }

                        // pg_class is now served by a real SQLite view (see pg_class migration).
                        // JOINs from pg_class into other SQLite-backed catalog tables (pg_constraint,
                        // pg_index, pg_depend, ...) must be executed as real SQL so the join predicate
                        // is actually evaluated, instead of being routed to a single-table handler that
                        // can't see columns from the other side of the join. pg_stats and pg_tablespace
                        // are excluded — see is_sqlite_backed_catalog.
                        let has_view_backed_catalog_joins = select.from[0].joins.iter().any(|j| {
                            if let TableFactor::Table { name, .. } = &j.relation {
                                let join_table = name.to_string().to_lowercase();
                                Self::is_sqlite_backed_catalog(&join_table)
                            } else {
                                false
                            }
                        });

                        if table_name.contains("pg_class") && has_view_backed_catalog_joins {
                            debug!("Passing pg_class JOIN against SQLite-backed catalog table to SQLite views");
                            return None;
                        }

                        // For other catalog table JOINs, still return None to let SQLite handle
                        if table_name.contains("pg_") && Self::is_sqlite_backed_catalog(&table_name) {
                            debug!("Passing other catalog JOIN query to SQLite views");
                            return None;
                        }
                    }
                    
                    // Keep special handling for pg_type JOINs since they need custom logic
                    if let TableFactor::Table { name, .. } = &select.from[0].relation {
                        let table_name = name.to_string().to_lowercase();
                        if table_name.contains("pg_type") || table_name.contains("pg_catalog.pg_type") {
                            // This is a pg_type JOIN query - handle it specially
                            let response = Self::handle_pg_type_join_query(select);
                            return Some(response);
                        }
                    }
                }
            }
            
            // For simple queries, check each table
            for table_ref in &select.from {
                // Check main table
                if let Some(response) = Self::check_table_factor(&table_ref.relation, select, db.clone(), session.clone()).await {
                    return response.ok();
                }
                
                // Check joined tables
                for join in &table_ref.joins {
                    if let Some(response) = Self::check_table_factor(&join.relation, select, db.clone(), session.clone()).await {
                        return response.ok();
                    }
                }
            }
        }
        
        None
    }
    
    async fn check_table_factor(table_factor: &TableFactor, select: &Select, db: Arc<DbHandler>, session: Option<Arc<SessionState>>) -> Option<Result<DbResponse, PgSqliteError>> {
        if let TableFactor::Table { name, .. } = table_factor {
            let table_name = name.to_string().to_lowercase();
            debug!("CHECK_TABLE_FACTOR: Processing table name: '{}'", table_name);
            debug!("check_table_factor: Processing table name: {}", table_name);
            
            // Handle pg_type queries
            if table_name.contains("pg_type") || table_name.contains("pg_catalog.pg_type") {
                return Some(Ok(Self::handle_pg_type_query(select, db.clone(), session.clone()).await));
            }

            // Handle pg_range queries (usually empty)
            if table_name.contains("pg_range") || table_name.contains("pg_catalog.pg_range") {
                return Some(Ok(Self::handle_pg_range_query(select)));
            }

            // Handle pg_tablespace queries
            if table_name.contains("pg_tablespace") || table_name.contains("pg_catalog.pg_tablespace") {
                return Some(Ok(Self::handle_pg_tablespace_query(select)));
            }

            // Handle pg_collation queries
            if table_name.contains("pg_collation") || table_name.contains("pg_catalog.pg_collation") {
                return Some(Ok(Self::handle_pg_collation_query(select)));
            }

            // Handle pg_replication_slots queries (always empty - SQLite has no replication)
            if table_name.contains("pg_replication_slots") || table_name.contains("pg_catalog.pg_replication_slots") {
                return Some(Ok(Self::handle_pg_replication_slots_query(select)));
            }

            // Handle pg_shdepend queries (always empty - SQLite has no shared dependencies)
            if table_name.contains("pg_shdepend") || table_name.contains("pg_catalog.pg_shdepend") {
                return Some(Ok(Self::handle_pg_shdepend_query(select)));
            }

            // Handle pg_statistic queries (always empty - internal stats table)
            if table_name.contains("pg_statistic") || table_name.contains("pg_catalog.pg_statistic") {
                return Some(Ok(Self::handle_pg_statistic_query(select)));
            }

            // Handle pg_attribute queries
            if table_name.contains("pg_attribute") || table_name.contains("pg_catalog.pg_attribute") {
                info!("Routing to PgAttributeHandler for table: {}", table_name);
                return match PgAttributeHandler::handle_query(select, &db).await {
                    Ok(response) => {
                        debug!("PgAttributeHandler returned {} rows", response.rows.len());
                        Some(Ok(response))
                    },
                    Err(_) => {
                        // PgAttributeHandler error
                        None
                    },
                };
            }
            
            // Handle pg_enum queries
            if table_name.contains("pg_enum") || table_name.contains("pg_catalog.pg_enum") {
                return Some(PgEnumHandler::handle_query(select, &db).await.map_err(PgSqliteError::Protocol));
            }

            // Handle pg_constraint queries
            if table_name.contains("pg_constraint") || table_name.contains("pg_catalog.pg_constraint") {
                info!("Routing to PgConstraintHandler for table: {}", table_name);
                return match PgConstraintHandler::handle_query(select, &db).await {
                    Ok(response) => {
                        debug!("PgConstraintHandler returned {} rows", response.rows.len());
                        Some(Ok(response))
                    },
                    Err(_) => {
                        // PgConstraintHandler error
                        None
                    },
                };
            }


            // Handle pg_description queries
            if table_name.contains("pg_description") || table_name.contains("pg_catalog.pg_description") {
                info!("Routing to PgDescriptionHandler for table: {}", table_name);
                return match PgDescriptionHandler::handle_query(select, &db).await {
                    Ok(response) => {
                        debug!("PgDescriptionHandler returned {} rows", response.rows.len());
                        Some(Ok(response))
                    },
                    Err(_) => {
                        // PgDescriptionHandler error
                        None
                    },
                };
            }
            // Handle pg_roles queries
            if table_name.contains("pg_roles") || table_name.contains("pg_catalog.pg_roles") {
                info!("Routing to PgRolesHandler for table: {}", table_name);
                return match PgRolesHandler::handle_query(select, &db).await {
                    Ok(response) => {
                        debug!("PgRolesHandler returned {} rows", response.rows.len());
                        Some(Ok(response))
                    },
                    Err(_) => {
                        // PgRolesHandler error
                        None
                    },
                };
            }
            // Handle pg_user queries
            if table_name.contains("pg_user") || table_name.contains("pg_catalog.pg_user") {
                info!("Routing to PgUserHandler for table: {}", table_name);
                return match PgUserHandler::handle_query(select, &db).await {
                    Ok(response) => {
                        debug!("PgUserHandler returned {} rows", response.rows.len());
                        Some(Ok(response))
                    },
                    Err(_) => {
                        // PgUserHandler error
                        None
                    },
                };
            }
            // Handle pg_stats queries
            if table_name.contains("pg_stats") || table_name.contains("pg_catalog.pg_stats") {
                info!("Routing to PgStatsHandler for table: {}", table_name);
                debug!("PgStatsHandler input select: {:?}", select);
                return match PgStatsHandler::handle_query(select, &db).await {
                    Ok(response) => {
                        debug!("PgStatsHandler returned {} rows", response.rows.len());
                        Some(Ok(response))
                    },
                    Err(_) => {
                        // PgStatsHandler error
                        None
                    },
                };
            }

            // Handle pg_sequence queries
            if table_name.contains("pg_sequence") || table_name.contains("pg_catalog.pg_sequence") {
                info!("Routing to PgSequenceHandler for table: {}", table_name);
                return match PgSequenceHandler::handle_query(select, &db).await {
                    Ok(response) => {
                        debug!("PgSequenceHandler returned {} rows", response.rows.len());
                        Some(Ok(response))
                    },
                    Err(_) => {
                        None
                    },
                };
            }

            if table_name.contains("pg_trigger") || table_name.contains("pg_catalog.pg_trigger") {
                info!("Routing to PgTriggerHandler for table: {}", table_name);
                return match PgTriggerHandler::handle_query(select, &db).await {
                    Ok(response) => {
                        debug!("PgTriggerHandler returned {} rows", response.rows.len());
                        Some(Ok(response))
                    },
                    Err(_) => {
                        None
                    },
                };
            }

            if table_name.contains("pg_settings") || table_name.contains("pg_catalog.pg_settings") {
                info!("Routing to PgSettingsHandler for table: {}", table_name);
                return match PgSettingsHandler::handle_query(select) {
                    Ok(response) => {
                        debug!("PgSettingsHandler returned {} rows", response.rows.len());
                        Some(Ok(response))
                    },
                    Err(_) => {
                        None
                    },
                };
            }

            // Handle pg_depend queries
            if table_name.contains("pg_depend") || table_name.contains("pg_catalog.pg_depend") {
                info!("Routing to PgDependHandler for table: {}", table_name);
                return match PgDependHandler::handle_query(select, &db).await {
                    Ok(response) => {
                        debug!("PgDependHandler returned {} rows", response.rows.len());
                        Some(Ok(response))
                    },
                    Err(_) => {
                        // PgDependHandler error
                        None
                    },
                };
            }

            // Handle information_schema.schemata queries
            if table_name.contains("information_schema.schemata") {
                return Some(Ok(Self::handle_information_schema_schemata_query(select, &db).await));
            }

            // Handle information_schema.key_column_usage queries
            if table_name.contains("information_schema.key_column_usage") {
                if let Some(ref session_state) = session {
                    return Some(Self::handle_information_schema_key_column_usage_query(select, &db, &session_state.id).await);
                } else {
                    return None;
                }
            }

            // Handle information_schema.table_constraints queries
            if table_name.contains("information_schema.table_constraints") {
                if let Some(ref session_state) = session {
                    return Some(Self::handle_information_schema_table_constraints_query(select, &db, &session_state.id).await);
                } else {
                    return None;
                }
            }

            // Handle information_schema.referential_constraints queries
            if table_name.contains("information_schema.referential_constraints") {
                if let Some(ref session_state) = session {
                    return Some(Self::handle_information_schema_referential_constraints_query_with_session(select, &db, &session_state.id).await);
                } else {
                    return None;
                }
            }

            // Handle information_schema.routines queries
            if table_name.contains("information_schema.routines") {
                return Some(Self::handle_information_schema_routines_query(select, &db).await);
            }

            // Handle information_schema.views queries
            if table_name.contains("information_schema.views") {
                return Some(Self::handle_information_schema_views_query(select, &db).await);
            }

            // Handle pg_database queries
            if table_name.contains("pg_database") || table_name.contains("pg_catalog.pg_database") {
                return Some(Ok(Self::handle_pg_database_query(select, &db).await));
            }

            // Handle pg_constraint queries
            if table_name.contains("pg_constraint") || table_name.contains("pg_catalog.pg_constraint") {
                return Some(crate::catalog::pg_constraint::PgConstraintHandler::handle_query(select, &db).await);
            }

            // Note: pg_index is a SQLite view that will be executed normally
            // It doesn't need special interception since it exists in the database
        }
        debug!("INTERCEPT: Reached end of intercept_query, returning None");
        None
    }

    async fn handle_pg_type_query(select: &Select, db: Arc<DbHandler>, session: Option<Arc<SessionState>>) -> DbResponse {
        // Extract which columns are being selected
        let mut columns = Vec::new();
        let mut column_indices = Vec::new();
        
        debug!("Processing pg_type query with {} projections", select.projection.len());
        
        for (i, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    let col_name = parts.last().unwrap().value.to_lowercase();
                    debug!("  Column {}: {}", i, col_name);
                    columns.push(col_name.clone());
                    column_indices.push(i);
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_lowercase();
                    debug!("  Column {}: {}", i, col_name);
                    columns.push(col_name.clone());
                    column_indices.push(i);
                }
                SelectItem::UnnamedExpr(Expr::Cast { expr, .. }) => {
                    // Handle CAST expressions like CAST(oid AS TEXT)
                    match expr.as_ref() {
                        Expr::Identifier(ident) => {
                            let col_name = ident.value.to_lowercase();
                            debug!("  Column {} (CAST): {}", i, col_name);
                            columns.push(col_name.clone());
                            column_indices.push(i);
                        }
                        Expr::CompoundIdentifier(parts) => {
                            let col_name = parts.last().unwrap().value.to_lowercase();
                            debug!("  Column {} (CAST): {}", i, col_name);
                            columns.push(col_name.clone());
                            column_indices.push(i);
                        }
                        _ => {
                            debug!("  Column {}: unknown CAST expression", i);
                        }
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    // For aliased expressions, we still need the source column for data lookup
                    let source_col = match expr {
                        Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
                        Expr::CompoundIdentifier(parts) => Some(parts.last().unwrap().value.to_lowercase()),
                        Expr::Cast { expr, .. } => {
                            match expr.as_ref() {
                                Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
                                Expr::CompoundIdentifier(parts) => Some(parts.last().unwrap().value.to_lowercase()),
                                _ => None,
                            }
                        }
                        _ => None,
                    };
                    
                    if let Some(col) = source_col {
                        debug!("  Column {} (aliased as {}): {}", i, alias, col);
                        columns.push(col);
                        column_indices.push(i);
                    } else {
                        debug!("  Column {}: unknown aliased expression", i);
                    }
                }
                SelectItem::Wildcard(_) => {
                    // Handle SELECT * queries - return all columns
                    debug!("  Wildcard selection - returning all columns");
                    columns = vec![
                        "oid".to_string(),
                        "typname".to_string(),
                        "typtype".to_string(),
                        "typelem".to_string(),
                        "typbasetype".to_string(),
                        "typnamespace".to_string(),
                        "typrelid".to_string(),
                    ];
                    break;
                }
                _ => {
                    debug!("  Column {}: unknown projection type", i);
                }
            }
        }
        
        // If no columns were detected, default to common columns for pg_type
        if columns.is_empty() && !select.projection.is_empty() {
            debug!("No columns detected from projections, using default pg_type columns");
            columns = vec!["oid".to_string(), "typname".to_string()];
        }

        // Check if there's a WHERE clause filtering by OID or typtype
        let mut filter_oid = None;
        let mut has_placeholder = false;
        let mut filter_typtype = None;
        
        if let Some(selection) = &select.selection {
            Self::extract_filters(selection, &mut filter_oid, &mut has_placeholder, &mut filter_typtype);
        }
        
        debug!("Filters - OID: {:?}, typtype: {:?}, has_placeholder: {}", filter_oid, filter_typtype, has_placeholder);
        
        // If query has a placeholder, we need to handle it differently
        // Don't return empty result as tokio-postgres needs the type info
        debug!("Query has placeholder: {}, filter_oid: {:?}", has_placeholder, filter_oid);

        // Build response based on columns requested
        let mut rows = Vec::new();
        
        // Special case: if filter_oid is -1 (our sentinel for NULL), return empty result
        if filter_oid == Some(-1) {
            debug!("NULL OID filter detected - returning empty result set");
            let rows_affected = 0;
            info!("pg_type query with NULL filter: returning 0 rows");
            return DbResponse {
                columns,
                rows,
                rows_affected,
            };
        }
        
        // Define our basic types - matching all types from type_mapper.rs
        let types = vec![
            // Basic types
            (16, "bool", "b", 0, 0, 11, 0),        // bool
            (17, "bytea", "b", 0, 0, 11, 0),       // bytea
            (20, "int8", "b", 0, 0, 11, 0),        // bigint
            (21, "int2", "b", 0, 0, 11, 0),        // smallint
            (23, "int4", "b", 0, 0, 11, 0),        // integer
            (25, "text", "b", 0, 0, 11, 0),        // text
            (114, "json", "b", 0, 0, 11, 0),       // json
            (700, "float4", "b", 0, 0, 11, 0),     // real
            (701, "float8", "b", 0, 0, 11, 0),     // double precision
            (1042, "char", "b", 0, 0, 11, 0),      // char
            (1043, "varchar", "b", 0, 0, 11, 0),   // varchar
            (1082, "date", "b", 0, 0, 11, 0),      // date
            (1083, "time", "b", 0, 0, 11, 0),      // time
            (1114, "timestamp", "b", 0, 0, 11, 0), // timestamp
            (1184, "timestamptz", "b", 0, 0, 11, 0), // timestamptz
            (1700, "numeric", "b", 0, 0, 11, 0),   // numeric
            (2950, "uuid", "b", 0, 0, 11, 0),      // uuid
            (3802, "jsonb", "b", 0, 0, 11, 0),     // jsonb
            // Array types
            (1000, "_bool", "b", 16, 0, 11, 0),    // bool array
            (1001, "_bytea", "b", 17, 0, 11, 0),   // bytea array
            (1005, "_int2", "b", 21, 0, 11, 0),    // int2 array
            (1007, "_int4", "b", 23, 0, 11, 0),    // int4 array
            (1009, "_text", "b", 25, 0, 11, 0),    // text array
            (1014, "_char", "b", 1042, 0, 11, 0),  // char array
            (1015, "_varchar", "b", 1043, 0, 11, 0), // varchar array
            (1016, "_int8", "b", 20, 0, 11, 0),    // int8 array
            (1021, "_float4", "b", 700, 0, 11, 0), // float4 array
            (1022, "_float8", "b", 701, 0, 11, 0), // float8 array
            (1115, "_timestamp", "b", 1114, 0, 11, 0), // timestamp array
            (1182, "_date", "b", 1082, 0, 11, 0),  // date array
            (1183, "_time", "b", 1083, 0, 11, 0),  // time array
            (1185, "_timestamptz", "b", 1184, 0, 11, 0), // timestamptz array
            (1231, "_numeric", "b", 1700, 0, 11, 0), // numeric array
            (2951, "_uuid", "b", 2950, 0, 11, 0),  // uuid array
            (199, "_json", "b", 114, 0, 11, 0),    // json array
            (3807, "_jsonb", "b", 3802, 0, 11, 0), // jsonb array
        ];

        for (oid, typname, typtype, typelem, typbasetype, _typnamespace, typrelid) in types {
            // Apply OID filter if specified
            if let Some(filter) = filter_oid
                && oid != filter {
                    continue;
                }
            
            // Apply typtype filter if specified
            if let Some(ref filter) = filter_typtype
                && typtype != filter {
                    continue;
                }

            let mut row = Vec::new();
            for col in &columns {
                let value = match col.as_str() {
                    "oid" => Some(oid.to_string().into_bytes()),
                    "typname" => Some(typname.to_string().into_bytes()),
                    "typtype" => Some(typtype.to_string().into_bytes()),
                    "typelem" => Some(typelem.to_string().into_bytes()),
                    "typbasetype" => Some(typbasetype.to_string().into_bytes()),
                    "typnamespace" => Some(_typnamespace.to_string().into_bytes()),
                    "typrelid" => Some(typrelid.to_string().into_bytes()),
                    "nspname" => Some("pg_catalog".to_string().into_bytes()),
                    "rngsubtype" => None, // NULL for non-range types
                    "typarray" => {
                        // Find the array type OID for this base type
                        let array_oid = match oid {
                            16 => 1000,   // bool -> _bool
                            17 => 1001,   // bytea -> _bytea
                            20 => 1016,   // int8 -> _int8
                            21 => 1005,   // int2 -> _int2
                            23 => 1007,   // int4 -> _int4
                            25 => 1009,   // text -> _text
                            700 => 1021,  // float4 -> _float4
                            701 => 1022,  // float8 -> _float8
                            1042 => 1014, // char -> _char
                            1043 => 1015, // varchar -> _varchar
                            1082 => 1182, // date -> _date
                            1083 => 1183, // time -> _time
                            1114 => 1115, // timestamp -> _timestamp
                            1184 => 1185, // timestamptz -> _timestamptz
                            1700 => 1231, // numeric -> _numeric
                            2950 => 2951, // uuid -> _uuid
                            114 => 199,   // json -> _json
                            3802 => 3807, // jsonb -> _jsonb
                            _ => 0,       // No array type
                        };
                        Some(array_oid.to_string().into_bytes())
                    }
                    "typdelim" => Some(",".to_string().into_bytes()), // Default delimiter
                    _ => None,
                };
                row.push(value);
            }
            
            if !row.is_empty() {
                rows.push(row);
            }
        }
        
        // Add ENUM types from metadata only if typtype filter allows it
        if filter_typtype.is_none() || filter_typtype.as_ref() == Some(&"e".to_string()) {
            // Use session connection if available, otherwise fall back to get_mut_connection
            let enum_types_result = if let Some(ref session) = session {
                db.with_session_connection(&session.id, |conn| {
                    crate::metadata::EnumMetadata::get_all_enum_types(conn)
                        .map_err(|e| rusqlite::Error::SqliteFailure(
                            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                            Some(format!("Failed to get enum types: {e}"))
                        ))
                }).await
            } else {
                db.get_mut_connection()
                    .and_then(|conn| crate::metadata::EnumMetadata::get_all_enum_types(&conn))
                    .map_err(PgSqliteError::Sqlite)
            };
            
            if let Ok(enum_types) = enum_types_result {
                debug!("Found {} enum types in metadata", enum_types.len());
                for enum_type in enum_types {
                        debug!("Processing enum type: {} (OID: {})", enum_type.type_name, enum_type.type_oid);
                        // Apply OID filter if specified
                        if let Some(filter) = filter_oid
                            && enum_type.type_oid != filter {
                                continue;
                            }
                        
                        let mut row = Vec::new();
                        for col in &columns {
                            let value = match col.as_str() {
                                "oid" => Some(enum_type.type_oid.to_string().into_bytes()),
                                "typname" => Some(enum_type.type_name.clone().into_bytes()),
                                "typtype" => Some("e".to_string().into_bytes()), // 'e' for enum
                                "typelem" => Some("0".to_string().into_bytes()),
                                "typbasetype" => Some("0".to_string().into_bytes()),
                                "typnamespace" => Some(enum_type.namespace_oid.to_string().into_bytes()),
                                "typrelid" => Some("0".to_string().into_bytes()),
                                "nspname" => Some("public".to_string().into_bytes()),
                                "rngsubtype" => None, // NULL for non-range types
                                "typarray" => Some("0".to_string().into_bytes()), // ENUMs don't have array types
                                "typdelim" => Some(",".to_string().into_bytes()), // Default delimiter
                                _ => None,
                            };
                            row.push(value);
                        }
                        
                        if !row.is_empty() {
                            rows.push(row);
                        }
                    }
            }
        }

        let rows_affected = rows.len();
        info!("pg_type query: filter_oid={:?}, filter_typtype={:?}, has_placeholder={}", filter_oid, filter_typtype, has_placeholder);
        info!("Returning {} rows for pg_type query with {} columns: {:?}", rows_affected, columns.len(), columns);
        DbResponse {
            columns,
            rows,
            rows_affected,
        }
    }

    fn handle_pg_range_query(_select: &Select) -> DbResponse {
        // pg_range is typically empty for basic types
        let columns = vec!["rngtypid".to_string(), "rngsubtype".to_string()];
        let rows = vec![];
        let rows_affected = rows.len();

        DbResponse {
            columns,
            rows,
            rows_affected,
        }
    }

    pub fn handle_pg_tablespace_query(_select: &Select) -> DbResponse {
        // Return standard PostgreSQL tablespaces
        let columns = vec![
            "oid".to_string(),
            "spcname".to_string(),
            "spcowner".to_string(),
            "spcacl".to_string(),
            "spcoptions".to_string(),
        ];

        let rows = vec![
            // pg_default tablespace (oid 1663)
            vec![
                Some("1663".to_string().into_bytes()),
                Some("pg_default".to_string().into_bytes()),
                Some("10".to_string().into_bytes()), // Default superuser oid
                Some("".to_string().into_bytes()),   // No ACL (NULL)
                Some("".to_string().into_bytes()),   // No options (NULL)
            ],
            // pg_global tablespace (oid 1664)
            vec![
                Some("1664".to_string().into_bytes()),
                Some("pg_global".to_string().into_bytes()),
                Some("10".to_string().into_bytes()), // Default superuser oid
                Some("".to_string().into_bytes()),   // No ACL (NULL)
                Some("".to_string().into_bytes()),   // No options (NULL)
            ],
        ];

        let rows_affected = rows.len();
        debug!("Returning {} rows for pg_tablespace query with {} columns: {:?}", rows_affected, columns.len(), columns);

        DbResponse {
            columns,
            rows,
            rows_affected,
        }
    }

    pub fn handle_pg_collation_query(select: &Select) -> DbResponse {
        // Define pg_collation columns (PostgreSQL standard)
        let all_columns = vec![
            "oid".to_string(),
            "collname".to_string(),
            "collnamespace".to_string(),
            "collowner".to_string(),
            "collprovider".to_string(),
            "collisdeterministic".to_string(),
            "collencoding".to_string(),
            "collcollate".to_string(),
            "collctype".to_string(),
            "colliculocale".to_string(),
            "collicurules".to_string(),
            "collversion".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Define standard collations
        let collations = vec![
            ("100", "default", "11", "10", "d", "t", "-1", "", "", "", "", ""),
            ("950", "C", "11", "10", "c", "t", "-1", "C", "C", "", "", ""),
            ("951", "POSIX", "11", "10", "c", "t", "-1", "POSIX", "POSIX", "", "", ""),
        ];

        // Check for WHERE clause filtering by collname
        let name_filter = if let Some(ref where_clause) = select.selection {
            Self::extract_collation_name_filter(where_clause)
        } else {
            None
        };

        let mut rows = Vec::new();
        for (oid, collname, collnamespace, collowner, collprovider, collisdeterministic,
             collencoding, collcollate, collctype, colliculocale, collicurules, collversion) in collations {

            // Apply name filter if present
            if let Some(ref filter) = name_filter {
                if collname != filter {
                    continue;
                }
            }

            let full_row: Vec<Option<Vec<u8>>> = vec![
                Some(oid.to_string().into_bytes()),
                Some(collname.to_string().into_bytes()),
                Some(collnamespace.to_string().into_bytes()),
                Some(collowner.to_string().into_bytes()),
                Some(collprovider.to_string().into_bytes()),
                Some(collisdeterministic.to_string().into_bytes()),
                Some(collencoding.to_string().into_bytes()),
                if collcollate.is_empty() { None } else { Some(collcollate.to_string().into_bytes()) },
                if collctype.is_empty() { None } else { Some(collctype.to_string().into_bytes()) },
                if colliculocale.is_empty() { None } else { Some(colliculocale.to_string().into_bytes()) },
                if collicurules.is_empty() { None } else { Some(collicurules.to_string().into_bytes()) },
                if collversion.is_empty() { None } else { Some(collversion.to_string().into_bytes()) },
            ];

            let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                .map(|&idx| full_row[idx].clone())
                .collect();
            rows.push(projected_row);
        }

        let rows_affected = rows.len();
        DbResponse {
            columns: selected_columns,
            rows,
            rows_affected,
        }
    }

    fn extract_collation_name_filter(where_clause: &Expr) -> Option<String> {
        match where_clause {
            Expr::BinaryOp { left, op, right } => {
                if let (Expr::Identifier(ident), sqlparser::ast::BinaryOperator::Eq, Expr::Value(value_with_span)) =
                    (left.as_ref(), op, right.as_ref())
                    && ident.value.to_lowercase() == "collname"
                        && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                            return Some(value.clone());
                        }
            }
            _ => {}
        }
        None
    }

    pub fn handle_pg_replication_slots_query(select: &Select) -> DbResponse {
        let all_columns = vec![
            "slot_name".to_string(),
            "plugin".to_string(),
            "slot_type".to_string(),
            "datoid".to_string(),
            "database".to_string(),
            "temporary".to_string(),
            "active".to_string(),
            "active_pid".to_string(),
            "xmin".to_string(),
            "catalog_xmin".to_string(),
            "restart_lsn".to_string(),
            "confirmed_flush_lsn".to_string(),
            "wal_status".to_string(),
            "safe_wal_size".to_string(),
            "two_phase".to_string(),
            "conflicting".to_string(),
        ];

        let (selected_columns, _) = Self::extract_selected_columns(select, &all_columns);

        // Always return empty - SQLite has no replication
        DbResponse {
            columns: selected_columns,
            rows: vec![],
            rows_affected: 0,
        }
    }

    pub fn handle_pg_shdepend_query(select: &Select) -> DbResponse {
        let all_columns = vec![
            "dbid".to_string(),
            "classid".to_string(),
            "objid".to_string(),
            "objsubid".to_string(),
            "refclassid".to_string(),
            "refobjid".to_string(),
            "deptype".to_string(),
        ];

        let (selected_columns, _) = Self::extract_selected_columns(select, &all_columns);

        // Always return empty - SQLite has no shared dependencies
        DbResponse {
            columns: selected_columns,
            rows: vec![],
            rows_affected: 0,
        }
    }

    pub fn handle_pg_statistic_query(select: &Select) -> DbResponse {
        let all_columns = vec![
            "starelid".to_string(),
            "staattnum".to_string(),
            "stainherit".to_string(),
            "stanullfrac".to_string(),
            "stawidth".to_string(),
            "stadistinct".to_string(),
            "stakind1".to_string(),
            "stakind2".to_string(),
            "stakind3".to_string(),
            "stakind4".to_string(),
            "stakind5".to_string(),
            "staop1".to_string(),
            "staop2".to_string(),
            "staop3".to_string(),
            "staop4".to_string(),
            "staop5".to_string(),
            "stacoll1".to_string(),
            "stacoll2".to_string(),
            "stacoll3".to_string(),
            "stacoll4".to_string(),
            "stacoll5".to_string(),
            "stanumbers1".to_string(),
            "stanumbers2".to_string(),
            "stanumbers3".to_string(),
            "stanumbers4".to_string(),
            "stanumbers5".to_string(),
            "stavalues1".to_string(),
            "stavalues2".to_string(),
            "stavalues3".to_string(),
            "stavalues4".to_string(),
            "stavalues5".to_string(),
        ];

        let (selected_columns, _) = Self::extract_selected_columns(select, &all_columns);

        // Always return empty - internal stats table, use pg_stats view instead
        DbResponse {
            columns: selected_columns,
            rows: vec![],
            rows_affected: 0,
        }
    }

    fn handle_pg_type_join_query(select: &Select) -> DbResponse {
        // Handle the complex JOIN query that tokio-postgres uses
        // Extract which columns are being selected
        let mut columns = Vec::new();
        
        debug!("Processing pg_type JOIN query with {} projections", select.projection.len());
        
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    let col_name = parts.last().unwrap().value.to_lowercase();
                    debug!("  Column: {}", col_name);
                    columns.push(col_name);
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_lowercase();
                    debug!("  Column: {}", col_name);
                    columns.push(col_name);
                }
                _ => {
                    debug!("  Unknown projection type");
                }
            }
        }

        // Check if there's a WHERE clause filtering by OID
        let mut filter_oid = None;
        
        if let Some(selection) = &select.selection
            && let Expr::BinaryOp { left, op, right } = selection
                && matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                    let is_oid_column = if let Expr::CompoundIdentifier(left_parts) = left.as_ref() {
                        left_parts.last().unwrap().value.to_lowercase() == "oid"
                    } else if let Expr::Identifier(ident) = left.as_ref() {
                        ident.value.to_lowercase() == "oid"
                    } else {
                        false
                    };
                    
                    if is_oid_column {
                        // Check if right side is a number or placeholder
                        if let Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) = right.as_ref() {
                            filter_oid = n.parse::<i32>().ok();
                        } else if let Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Placeholder(_), .. }) = right.as_ref() {
                            // For placeholders in JOIN queries, we return all types
                            // tokio-postgres will filter client-side
                            filter_oid = None;
                        }
                    }
                }
        
        // Build response with all requested columns
        let mut rows = Vec::new();
        
        // Define our types with proper values for JOIN query
        let types = vec![
            // Basic types
            (16, "bool", "b", 0, 0, 11, 0),
            (17, "bytea", "b", 0, 0, 11, 0),
            (20, "int8", "b", 0, 0, 11, 0),
            (21, "int2", "b", 0, 0, 11, 0),
            (23, "int4", "b", 0, 0, 11, 0),
            (25, "text", "b", 0, 0, 11, 0),
            (114, "json", "b", 0, 0, 11, 0),
            (700, "float4", "b", 0, 0, 11, 0),
            (701, "float8", "b", 0, 0, 11, 0),
            (1042, "char", "b", 0, 0, 11, 0),
            (1043, "varchar", "b", 0, 0, 11, 0),
            (1082, "date", "b", 0, 0, 11, 0),
            (1083, "time", "b", 0, 0, 11, 0),
            (1114, "timestamp", "b", 0, 0, 11, 0),
            (1184, "timestamptz", "b", 0, 0, 11, 0),
            (1700, "numeric", "b", 0, 0, 11, 0),
            (2950, "uuid", "b", 0, 0, 11, 0),
            (3802, "jsonb", "b", 0, 0, 11, 0),
            // Array types - typtype is still 'b' for arrays in PostgreSQL
            (1000, "_bool", "b", 16, 0, 11, 0),
            (1001, "_bytea", "b", 17, 0, 11, 0),
            (1005, "_int2", "b", 21, 0, 11, 0),
            (1007, "_int4", "b", 23, 0, 11, 0),
            (1009, "_text", "b", 25, 0, 11, 0),
            (1014, "_char", "b", 1042, 0, 11, 0),
            (1015, "_varchar", "b", 1043, 0, 11, 0),
            (1016, "_int8", "b", 20, 0, 11, 0),
            (1021, "_float4", "b", 700, 0, 11, 0),
            (1022, "_float8", "b", 701, 0, 11, 0),
            (1115, "_timestamp", "b", 1114, 0, 11, 0),
            (1182, "_date", "b", 1082, 0, 11, 0),
            (1183, "_time", "b", 1083, 0, 11, 0),
            (1185, "_timestamptz", "b", 1184, 0, 11, 0),
            (1231, "_numeric", "b", 1700, 0, 11, 0),
            (2951, "_uuid", "b", 2950, 0, 11, 0),
            (199, "_json", "b", 114, 0, 11, 0),
            (3807, "_jsonb", "b", 3802, 0, 11, 0),
        ];

        for (oid, typname, typtype, typelem, typbasetype, _typnamespace, typrelid) in types {
            // Apply filter if specified
            if let Some(filter) = filter_oid
                && oid != filter {
                    continue;
                }

            let mut row = Vec::new();
            for col in &columns {
                let value = match col.as_str() {
                    "oid" => Some(oid.to_string().into_bytes()),
                    "typname" => Some(typname.to_string().into_bytes()),
                    "typtype" => Some(typtype.to_string().into_bytes()),
                    "typelem" => Some(typelem.to_string().into_bytes()),
                    "rngsubtype" => None, // NULL for non-range types
                    "typbasetype" => Some(typbasetype.to_string().into_bytes()),
                    "nspname" => Some("pg_catalog".to_string().into_bytes()),
                    "typrelid" => Some(typrelid.to_string().into_bytes()),
                    "typarray" => {
                        // Find the array type OID for this base type
                        let array_oid = match oid {
                            16 => 1000,   // bool -> _bool
                            17 => 1001,   // bytea -> _bytea
                            20 => 1016,   // int8 -> _int8
                            21 => 1005,   // int2 -> _int2
                            23 => 1007,   // int4 -> _int4
                            25 => 1009,   // text -> _text
                            700 => 1021,  // float4 -> _float4
                            701 => 1022,  // float8 -> _float8
                            1042 => 1014, // char -> _char
                            1043 => 1015, // varchar -> _varchar
                            1082 => 1182, // date -> _date
                            1083 => 1183, // time -> _time
                            1114 => 1115, // timestamp -> _timestamp
                            1184 => 1185, // timestamptz -> _timestamptz
                            1700 => 1231, // numeric -> _numeric
                            2950 => 2951, // uuid -> _uuid
                            114 => 199,   // json -> _json
                            3802 => 3807, // jsonb -> _jsonb
                            _ => 0,       // No array type
                        };
                        Some(array_oid.to_string().into_bytes())
                    }
                    "typdelim" => Some(",".to_string().into_bytes()), // Default delimiter
                    _ => None,
                };
                row.push(value);
            }
            
            if !row.is_empty() {
                rows.push(row);
            }
        }

        let rows_affected = rows.len();
        debug!("Returning {} rows for pg_type JOIN query", rows_affected);
        DbResponse {
            columns,
            rows,
            rows_affected,
        }
    }

    /// Check if a query contains system function calls
    pub fn query_contains_system_functions(query: &sqlparser::ast::Query) -> bool {
        if let SetExpr::Select(select) = &*query.body {
            // Check projections
            for item in &select.projection {
                if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item
                    && Self::expression_contains_system_function(expr) {
                        return true;
                    }
            }
            
            // Check WHERE clause
            if let Some(selection) = &select.selection
                && Self::expression_contains_system_function(selection) {
                    return true;
                }
        }
        false
    }

    /// Check if an expression contains system function calls
    fn expression_contains_system_function(expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                debug!("Found function in expression: {}", func_name);
                // Check if it's a known system function
                matches!(func_name.as_str(), 
                    "pg_get_constraintdef" | "pg_table_is_visible" | "format_type" |
                    "pg_get_expr" | "pg_get_userbyid" | "pg_get_indexdef" | "to_regtype" |
                    "pg_catalog.pg_get_constraintdef" | "pg_catalog.pg_table_is_visible" |
                    "pg_catalog.format_type" | "pg_catalog.pg_get_expr" |
                    "pg_catalog.pg_get_userbyid" | "pg_catalog.pg_get_indexdef" | "pg_catalog.to_regtype"
                )
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::expression_contains_system_function(left) || 
                Self::expression_contains_system_function(right)
            }
            Expr::UnaryOp { expr, .. } => Self::expression_contains_system_function(expr),
            Expr::Cast { expr, .. } => Self::expression_contains_system_function(expr),
            Expr::Case { operand, conditions, else_result, .. } => {
                operand.as_ref().is_some_and(|e| Self::expression_contains_system_function(e)) ||
                conditions.iter().any(|when| Self::expression_contains_system_function(&when.condition) || 
                                           Self::expression_contains_system_function(&when.result)) ||
                else_result.as_ref().is_some_and(|e| Self::expression_contains_system_function(e))
            }
            Expr::InList { expr, list, .. } => {
                Self::expression_contains_system_function(expr) ||
                list.iter().any(Self::expression_contains_system_function)
            }
            Expr::InSubquery { expr, subquery: _, .. } => Self::expression_contains_system_function(expr),
            Expr::Between { expr, low, high, .. } => {
                Self::expression_contains_system_function(expr) ||
                Self::expression_contains_system_function(low) ||
                Self::expression_contains_system_function(high)
            }
            _ => false,
        }
    }

    /// Process system functions in a query by replacing them with their results
    pub async fn process_system_functions_in_query(
        mut query: Box<sqlparser::ast::Query>,
        db: Arc<DbHandler>,
    ) -> Result<Box<sqlparser::ast::Query>, Box<dyn std::error::Error + Send + Sync>> {
        
        if let SetExpr::Select(select) = &mut *query.body {
            // Process projections
            for item in &mut select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        Self::process_expression(expr, db.clone()).await?;
                    }
                    SelectItem::ExprWithAlias { expr, .. } => {
                        Self::process_expression(expr, db.clone()).await?;
                    }
                    _ => {}
                }
            }
            
            // Process WHERE clause
            if let Some(selection) = &mut select.selection {
                Self::process_expression(selection, db.clone()).await?;
            }
        }
        
        Ok(query)
    }

    /// Extract filter conditions from WHERE clause
    fn extract_filters(expr: &Expr, filter_oid: &mut Option<i32>, has_placeholder: &mut bool, filter_typtype: &mut Option<String>) {
        if let Expr::BinaryOp { left, op, right } = expr {
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                // Check for OID filter
                let is_oid_column = if let Expr::CompoundIdentifier(left_parts) = left.as_ref() {
                    left_parts.last().unwrap().value.to_lowercase() == "oid"
                } else if let Expr::Identifier(ident) = left.as_ref() {
                    ident.value.to_lowercase() == "oid"
                } else {
                    false
                };
                
                if is_oid_column {
                    // Check if right side is a number (not a placeholder)
                    match right.as_ref() {
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) => {
                            *filter_oid = n.parse::<i32>().ok();
                            debug!("Extracted numeric OID filter: {:?}", filter_oid);
                        }
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::SingleQuotedString(s), .. }) => {
                            // Handle quoted numeric strings (from parameter substitution)
                            if s.to_uppercase() == "NULL" {
                                // Handle 'NULL' string literal as NULL filter
                                *filter_oid = Some(-1); // Use -1 as a sentinel value for NULL
                                debug!("Found 'NULL' string literal - treating as NULL filter");
                            } else {
                                *filter_oid = s.parse::<i32>().ok();
                                debug!("Extracted string OID filter: {:?}", filter_oid);
                            }
                        }
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Placeholder(_), .. }) => {
                            *has_placeholder = true;
                            debug!("Found placeholder for OID filter");
                        }
                        Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Null, .. }) => {
                            // NULL filter means no rows should be returned
                            *filter_oid = Some(-1); // Use -1 as a sentinel value for NULL
                            debug!("Found NULL OID filter - no rows will match");
                        }
                        Expr::Function(func) if func.name.to_string().to_lowercase() == "to_regtype" => {
                            // This is a to_regtype function call that hasn't been processed yet
                            debug!("Found to_regtype function in OID filter - needs processing");
                            *has_placeholder = true; // Treat it like a placeholder for now
                        }
                        _ => {
                            debug!("Unknown expression type for OID filter: {:?}", right);
                        }
                    }
                }
                
                // Check for typtype filter
                let is_typtype_column = if let Expr::CompoundIdentifier(left_parts) = left.as_ref() {
                    left_parts.last().unwrap().value.to_lowercase() == "typtype"
                } else if let Expr::Identifier(ident) = left.as_ref() {
                    ident.value.to_lowercase() == "typtype"
                } else {
                    false
                };
                
                if is_typtype_column
                    && let Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::SingleQuotedString(s), .. }) = right.as_ref() {
                        *filter_typtype = Some(s.clone());
                    }
            } else if matches!(op, sqlparser::ast::BinaryOperator::And) {
                // Recursively check both sides of AND
                Self::extract_filters(left, filter_oid, has_placeholder, filter_typtype);
                Self::extract_filters(right, filter_oid, has_placeholder, filter_typtype);
            }
        }
    }

    /// Process an expression and replace system function calls with their results
    fn process_expression<'a>(
        expr: &'a mut Expr,
        db: Arc<DbHandler>,
    ) -> ProcessExpressionFuture<'a> {
        Box::pin(async move {
        match expr {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();
                let base_name = if let Some(pos) = func_name.rfind('.') {
                    &func_name[pos + 1..]
                } else {
                    &func_name
                };
                
                // Extract arguments
                let mut args = Vec::new();
                if let sqlparser::ast::FunctionArguments::List(func_arg_list) = &func.args {
                    for arg in &func_arg_list.args {
                        match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => args.push(e.clone()),
                            FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } => args.push(e.clone()),
                            _ => {}
                        }
                    }
                }
                
                // Process the function call
                if let Some(result) = SystemFunctions::process_function_call(base_name, &args, db).await? {
                    // Replace the function call with its result
                    *expr = Expr::Value(sqlparser::ast::ValueWithSpan { 
                        value: sqlparser::ast::Value::SingleQuotedString(result),
                        span: Span {
                            start: Location { line: 1, column: 1 },
                            end: Location { line: 1, column: 1 }
                        }
                    });
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::process_expression(left, db.clone()).await?;
                Self::process_expression(right, db.clone()).await?;
            }
            Expr::UnaryOp { expr: inner_expr, .. } => {
                Self::process_expression(inner_expr, db.clone()).await?;
            }
            Expr::Cast { expr: inner_expr, .. } => {
                Self::process_expression(inner_expr, db.clone()).await?;
            }
            Expr::Case { operand, conditions, else_result, .. } => {
                if let Some(op) = operand {
                    Self::process_expression(op, db.clone()).await?;
                }
                for when in conditions.iter_mut() {
                    Self::process_expression(&mut when.condition, db.clone()).await?;
                    Self::process_expression(&mut when.result, db.clone()).await?;
                }
                if let Some(else_res) = else_result {
                    Self::process_expression(else_res, db.clone()).await?;
                }
            }
            Expr::InList { expr: inner_expr, list, .. } => {
                Self::process_expression(inner_expr, db.clone()).await?;
                for item in list {
                    Self::process_expression(item, db.clone()).await?;
                }
            }
            Expr::Between { expr: inner_expr, low, high, .. } => {
                Self::process_expression(inner_expr, db.clone()).await?;
                Self::process_expression(low, db.clone()).await?;
                Self::process_expression(high, db.clone()).await?;
            }
            _ => {}
        }
        Ok(())
        })
    }

    /// Extract selected output columns and source indices from a SELECT query.
    fn extract_selected_columns(select: &Select, all_columns: &[String]) -> (Vec<String>, Vec<usize>) {
        if select.projection.len() == 1
            && matches!(
                &select.projection[0],
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            ) {
                // SELECT * - return all columns
                return (all_columns.to_vec(), (0..all_columns.len()).collect::<Vec<_>>());
            }

        // Extract specific columns
        let mut cols = Vec::new();
        let mut indices = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if let Some(col_name) = Self::extract_projection_source_column(expr)
                        && let Some(idx) = all_columns.iter().position(|c| c == &col_name)
                    {
                        cols.push(all_columns[idx].clone());
                        indices.push(idx);
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Some(col_name) = Self::extract_projection_source_column(expr)
                        && let Some(idx) = all_columns.iter().position(|c| c == &col_name)
                    {
                        cols.push(Self::alias_output_name(alias));
                        indices.push(idx);
                    }
                }
                SelectItem::Wildcard(_) => {
                    cols.extend_from_slice(all_columns);
                    indices.extend(0..all_columns.len());
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    cols.extend_from_slice(all_columns);
                    indices.extend(0..all_columns.len());
                }
            }
        }
        (cols, indices)
    }

    fn extract_projection_source_column(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
            Expr::CompoundIdentifier(parts) => {
                parts.last().map(|ident| ident.value.to_lowercase())
            }
            Expr::Cast { expr, .. } => Self::extract_projection_source_column(expr),
            Expr::Nested(expr) => Self::extract_projection_source_column(expr),
            Expr::Function(f) => Self::function_name(f),
            _ => None,
        }
    }

    fn alias_output_name(alias: &sqlparser::ast::Ident) -> String {
        if alias.quote_style.is_some() {
            alias.value.clone()
        } else {
            alias.value.to_lowercase()
        }
    }

    fn function_name(function: &sqlparser::ast::Function) -> Option<String> {
        function
            .name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.to_lowercase())
    }

    async fn handle_information_schema_schemata_query(select: &Select, _db: &DbHandler) -> DbResponse {
        debug!("Handling information_schema.schemata query");

        // Define information_schema.schemata columns
        let all_columns = vec![
            "catalog_name".to_string(),
            "schema_name".to_string(),
            "schema_owner".to_string(),
            "default_character_set_catalog".to_string(),
            "default_character_set_schema".to_string(),
            "default_character_set_name".to_string(),
            "sql_path".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Define available schemas
        let schemas = vec![
            ("main", "public", "postgres"),      // Default public schema
            ("main", "pg_catalog", "postgres"),  // System catalog schema
            ("main", "information_schema", "postgres"), // Information schema
        ];

        let mut rows = Vec::new();
        for (catalog, schema, owner) in schemas {
            let full_row: Vec<Option<Vec<u8>>> = vec![
                Some(catalog.to_string().into_bytes()),     // catalog_name
                Some(schema.to_string().into_bytes()),      // schema_name
                Some(owner.to_string().into_bytes()),       // schema_owner
                None,                                       // default_character_set_catalog
                None,                                       // default_character_set_schema
                None,                                       // default_character_set_name
                None,                                       // sql_path
            ];

            let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                .map(|&idx| full_row[idx].clone())
                .collect();

            rows.push(projected_row);
        }

        let rows_affected = rows.len();
        DbResponse {
            columns: selected_columns,
            rows,
            rows_affected,
        }
    }

    pub async fn handle_information_schema_key_column_usage_query(select: &Select, db: &DbHandler, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling information_schema.key_column_usage query");

        // Define information_schema.key_column_usage columns (PostgreSQL standard)
        let all_columns = vec![
            "constraint_catalog".to_string(),
            "constraint_schema".to_string(),
            "constraint_name".to_string(),
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "column_name".to_string(),
            "ordinal_position".to_string(),
            "position_in_unique_constraint".to_string(),
        ];

        // Determine which columns are being selected
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Extract table filter from WHERE clause if present
        let table_filter = if let Some(ref where_clause) = select.selection {
            Self::extract_table_name_filter(where_clause)
        } else {
            None
        };

        let mut rows = Vec::new();

        // Get list of user tables from sqlite_master
        let tables_query = if let Some(table_name) = &table_filter {
            format!("SELECT name FROM sqlite_master WHERE type='table' AND name='{}' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%' AND name NOT LIKE 'pg_%'", table_name)
        } else {
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%' AND name NOT LIKE 'pg_%'".to_string()
        };

        let tables_response = match db.connection_manager().execute_with_session(session_id, |conn| {
            let mut stmt = conn.prepare(&tables_query)?;
            let mut tables = Vec::new();
            let mut query_rows = stmt.query([])?;
            while let Some(row) = query_rows.next()? {
                let table_name: String = row.get(0)?;
                tables.push(table_name);
            }
            Ok(tables)
        }) {
            Ok(tables) => tables,
            Err(_) => return Ok(DbResponse {
                columns: selected_columns,
                rows: vec![],
                rows_affected: 0,
            }),
        };

        // For each table, extract constraint information using SQLite pragmas
        for table_name in tables_response {
            // 1. Get primary key information from PRAGMA table_info
            let table_info_response = match db.connection_manager().execute_with_session(session_id, |conn| {
                let pragma_query = format!("PRAGMA table_info({})", table_name);
                let mut stmt = conn.prepare(&pragma_query)?;
                let mut rows = Vec::new();
                let mut query_rows = stmt.query([])?;
                while let Some(row) = query_rows.next()? {
                    let name: String = row.get(1)?;
                    let pk: i32 = row.get(5)?;
                    if pk > 0 {
                        rows.push((name, pk));
                    }
                }
                Ok(rows)
            }) {
                Ok(response) => response,
                Err(_) => continue,
            };

            // Add primary key constraints
            for (column_name, pk_order) in table_info_response {
                let constraint_name = format!("{}_pkey", table_name);
                let full_row: Vec<Option<Vec<u8>>> = vec![
                    Some("main".to_string().into_bytes()),                 // constraint_catalog
                    Some("public".to_string().into_bytes()),              // constraint_schema
                    Some(constraint_name.into_bytes()),                   // constraint_name
                    Some("main".to_string().into_bytes()),                // table_catalog
                    Some("public".to_string().into_bytes()),              // table_schema
                    Some(table_name.clone().into_bytes()),                // table_name
                    Some(column_name.into_bytes()),                       // column_name
                    Some(pk_order.to_string().into_bytes()),              // ordinal_position
                    None,                                                 // position_in_unique_constraint
                ];

                // Project only the requested columns
                let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                    .map(|&idx| full_row[idx].clone())
                    .collect();
                rows.push(projected_row);
            }

            // 2. Get foreign key information from PRAGMA foreign_key_list
            let fk_response = match db.connection_manager().execute_with_session(session_id, |conn| {
                let pragma_query = format!("PRAGMA foreign_key_list({})", table_name);
                let mut stmt = conn.prepare(&pragma_query)?;
                let mut fks = Vec::new();
                let mut query_rows = stmt.query([])?;
                while let Some(row) = query_rows.next()? {
                    let id: i32 = row.get(0)?;
                    let seq: i32 = row.get(1)?;
                    let from_col: String = row.get(3)?;
                    fks.push((id, seq, from_col));
                }
                Ok(fks)
            }) {
                Ok(response) => response,
                Err(_) => continue,
            };

            // Add foreign key constraints
            for (_id, seq, from_col) in fk_response {
                let constraint_name = format!("{}_{}_fkey", table_name, from_col);
                let full_row: Vec<Option<Vec<u8>>> = vec![
                    Some("main".to_string().into_bytes()),                 // constraint_catalog
                    Some("public".to_string().into_bytes()),              // constraint_schema
                    Some(constraint_name.into_bytes()),                   // constraint_name
                    Some("main".to_string().into_bytes()),                // table_catalog
                    Some("public".to_string().into_bytes()),              // table_schema
                    Some(table_name.clone().into_bytes()),                // table_name
                    Some(from_col.into_bytes()),                          // column_name
                    Some((seq + 1).to_string().into_bytes()),             // ordinal_position
                    None,                                                 // position_in_unique_constraint
                ];

                // Project only the requested columns
                let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                    .map(|&idx| full_row[idx].clone())
                    .collect();
                rows.push(projected_row);
            }
        }

        let rows_affected = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected,
        })
    }

    pub async fn handle_information_schema_table_constraints_query(select: &Select, db: &DbHandler, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling information_schema.table_constraints query");

        // Define information_schema.table_constraints columns (PostgreSQL standard)
        let all_columns = vec![
            "constraint_catalog".to_string(),
            "constraint_schema".to_string(),
            "constraint_name".to_string(),
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "constraint_type".to_string(),
            "is_deferrable".to_string(),
            "initially_deferred".to_string(),
            "enforced".to_string(),
            "nulls_distinct".to_string(),
        ];

        // Determine which columns are being selected
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Extract table filter from WHERE clause if present
        let table_filter = if let Some(ref where_clause) = select.selection {
            Self::extract_table_name_filter(where_clause)
        } else {
            None
        };

        let mut rows = Vec::new();

        // Get list of user tables from sqlite_master
        let tables_query = if let Some(table_name) = &table_filter {
            format!("SELECT name FROM sqlite_master WHERE type='table' AND name='{}' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%' AND name NOT LIKE 'pg_%'", table_name)
        } else {
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%' AND name NOT LIKE 'pg_%'".to_string()
        };

        let tables_response = match db.connection_manager().execute_with_session(session_id, |conn| {
            let mut stmt = conn.prepare(&tables_query)?;
            let mut tables = Vec::new();
            let mut query_rows = stmt.query([])?;
            while let Some(row) = query_rows.next()? {
                let table_name: String = row.get(0)?;
                tables.push(table_name);
            }
            Ok(tables)
        }) {
            Ok(tables) => tables,
            Err(_) => return Ok(DbResponse {
                columns: selected_columns,
                rows: vec![],
                rows_affected: 0,
            }),
        };

        // For each table, extract constraint information using SQLite pragmas
        for table_name in tables_response {
            // 1. Get primary key information from PRAGMA table_info
            let table_info_response = match db.connection_manager().execute_with_session(session_id, |conn| {
                let pragma_query = format!("PRAGMA table_info({})", table_name);
                let mut stmt = conn.prepare(&pragma_query)?;
                let mut has_pk = false;
                let mut query_rows = stmt.query([])?;
                while let Some(row) = query_rows.next()? {
                    let pk: i32 = row.get(5)?;
                    if pk > 0 {
                        has_pk = true;
                        break;
                    }
                }
                Ok(has_pk)
            }) {
                Ok(response) => response,
                Err(_) => continue,
            };

            // Add primary key constraint if table has one
            if table_info_response {
                let constraint_name = format!("{}_pkey", table_name);
                let full_row: Vec<Option<Vec<u8>>> = vec![
                    Some("main".to_string().into_bytes()),                // constraint_catalog
                    Some("public".to_string().into_bytes()),              // constraint_schema
                    Some(constraint_name.into_bytes()),                   // constraint_name
                    Some("main".to_string().into_bytes()),                // table_catalog
                    Some("public".to_string().into_bytes()),              // table_schema
                    Some(table_name.clone().into_bytes()),                // table_name
                    Some("PRIMARY KEY".to_string().into_bytes()),         // constraint_type
                    Some("NO".to_string().into_bytes()),                  // is_deferrable
                    Some("NO".to_string().into_bytes()),                  // initially_deferred
                    Some("YES".to_string().into_bytes()),                 // enforced
                    Some("YES".to_string().into_bytes()),                 // nulls_distinct
                ];

                // Project only the requested columns
                let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                    .map(|&idx| full_row[idx].clone())
                    .collect();
                rows.push(projected_row);
            }

            // 2. Get foreign key information from PRAGMA foreign_key_list
            let fk_response = match db.connection_manager().execute_with_session(session_id, |conn| {
                let pragma_query = format!("PRAGMA foreign_key_list({})", table_name);
                let mut stmt = conn.prepare(&pragma_query)?;
                let mut fks = Vec::new();
                let mut query_rows = stmt.query([])?;
                while let Some(row) = query_rows.next()? {
                    let id: i32 = row.get(0)?;
                    let from_col: String = row.get(3)?;
                    if !fks.iter().any(|(fk_id, _)| *fk_id == id) {
                        fks.push((id, from_col));
                    }
                }
                Ok(fks)
            }) {
                Ok(response) => response,
                Err(_) => continue,
            };

            // Add foreign key constraints
            for (_id, from_col) in fk_response {
                let constraint_name = format!("{}_{}_fkey", table_name, from_col);
                let full_row: Vec<Option<Vec<u8>>> = vec![
                    Some("main".to_string().into_bytes()),                // constraint_catalog
                    Some("public".to_string().into_bytes()),              // constraint_schema
                    Some(constraint_name.into_bytes()),                   // constraint_name
                    Some("main".to_string().into_bytes()),                // table_catalog
                    Some("public".to_string().into_bytes()),              // table_schema
                    Some(table_name.clone().into_bytes()),                // table_name
                    Some("FOREIGN KEY".to_string().into_bytes()),         // constraint_type
                    Some("NO".to_string().into_bytes()),                  // is_deferrable
                    Some("NO".to_string().into_bytes()),                  // initially_deferred
                    Some("YES".to_string().into_bytes()),                 // enforced
                    Some("YES".to_string().into_bytes()),                 // nulls_distinct
                ];

                // Project only the requested columns
                let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                    .map(|&idx| full_row[idx].clone())
                    .collect();
                rows.push(projected_row);
            }
        }

        let rows_affected = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected,
        })
    }

    async fn handle_pg_database_query(select: &Select, _db: &DbHandler) -> DbResponse {
        debug!("Handling pg_database query");

        // Define pg_database columns (PostgreSQL 17 compatible)
        let all_columns = vec![
            "oid".to_string(),
            "datname".to_string(),
            "datdba".to_string(),
            "encoding".to_string(),
            "datlocprovider".to_string(),
            "datistemplate".to_string(),
            "datallowconn".to_string(),
            "dathasloginevt".to_string(),
            "datconnlimit".to_string(),
            "datfrozenxid".to_string(),
            "datminmxid".to_string(),
            "dattablespace".to_string(),
            "datcollate".to_string(),
            "datctype".to_string(),
            "datlocale".to_string(),
            "daticurules".to_string(),
            "datcollversion".to_string(),
            "datacl".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Single database entry representing the current SQLite database
        let full_row: Vec<Option<Vec<u8>>> = vec![
            Some("1".to_string().into_bytes()),                        // oid
            Some("main".to_string().into_bytes()),                     // datname - the key field!
            Some("10".to_string().into_bytes()),                       // datdba (owner)
            Some("6".to_string().into_bytes()),                        // encoding (UTF8)
            Some("d".to_string().into_bytes()),                        // datlocprovider (default)
            Some("f".to_string().into_bytes()),                        // datistemplate (false) - PostgreSQL uses 'f'/'t' for bool
            Some("t".to_string().into_bytes()),                        // datallowconn (true) - PostgreSQL uses 'f'/'t' for bool
            Some("f".to_string().into_bytes()),                        // dathasloginevt (false) - PostgreSQL uses 'f'/'t' for bool
            Some("-1".to_string().into_bytes()),                       // datconnlimit (no limit)
            Some("1".to_string().into_bytes()),                        // datfrozenxid
            Some("1".to_string().into_bytes()),                        // datminmxid
            Some("1663".to_string().into_bytes()),                     // dattablespace (default)
            Some("en_US.UTF-8".to_string().into_bytes()),              // datcollate
            Some("en_US.UTF-8".to_string().into_bytes()),              // datctype
            None,                                                      // datlocale
            None,                                                      // daticurules
            None,                                                      // datcollversion
            None,                                                      // datacl
        ];

        // Project only requested columns
        let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
            .map(|&idx| full_row[idx].clone())
            .collect();

        DbResponse {
            columns: selected_columns,
            rows: vec![projected_row],
            rows_affected: 1,
        }
    }

    /// Extract table name from WHERE clause like "table_name = 'some_table'"
    fn extract_table_name_filter(where_clause: &Expr) -> Option<String> {
        match where_clause {
            Expr::BinaryOp { left, op, right } => {
                // Handle "table_name = 'value'"
                if let (Expr::Identifier(ident), sqlparser::ast::BinaryOperator::Eq, Expr::Value(value_with_span)) =
                    (left.as_ref(), op, right.as_ref())
                    && ident.value.to_lowercase() == "table_name"
                        && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                            return Some(value.clone());
                        }
                // Handle "'value' = table_name" (reversed)
                if let (Expr::Value(value_with_span), sqlparser::ast::BinaryOperator::Eq, Expr::Identifier(ident)) =
                    (left.as_ref(), op, right.as_ref())
                    && ident.value.to_lowercase() == "table_name"
                        && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                            return Some(value.clone());
                        }
                // Handle compound identifiers like "information_schema.columns.table_name = 'value'"
                if let (Expr::CompoundIdentifier(parts), sqlparser::ast::BinaryOperator::Eq, Expr::Value(value_with_span)) =
                    (left.as_ref(), op, right.as_ref())
                    && let Some(last_part) = parts.last()
                        && last_part.value.to_lowercase() == "table_name"
                            && let sqlparser::ast::Value::SingleQuotedString(value) = &value_with_span.value {
                                return Some(value.clone());
                            }
            }
            Expr::Nested(inner) => {
                return Self::extract_table_name_filter(inner);
            }
            _ => {}
        }
        None
    }

    pub async fn handle_information_schema_routines_query(select: &Select, _db: &DbHandler) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling information_schema.routines query");

        // Define information_schema.routines columns (PostgreSQL standard)
        let all_columns = vec![
            "specific_catalog".to_string(),
            "specific_schema".to_string(),
            "specific_name".to_string(),
            "routine_catalog".to_string(),
            "routine_schema".to_string(),
            "routine_name".to_string(),
            "routine_type".to_string(),
            "module_catalog".to_string(),
            "module_schema".to_string(),
            "module_name".to_string(),
            "udt_catalog".to_string(),
            "udt_schema".to_string(),
            "udt_name".to_string(),
            "data_type".to_string(),
            "character_maximum_length".to_string(),
            "character_octet_length".to_string(),
            "character_set_catalog".to_string(),
            "character_set_schema".to_string(),
            "character_set_name".to_string(),
            "collation_catalog".to_string(),
            "collation_schema".to_string(),
            "collation_name".to_string(),
            "numeric_precision".to_string(),
            "numeric_precision_radix".to_string(),
            "numeric_scale".to_string(),
            "datetime_precision".to_string(),
            "interval_type".to_string(),
            "interval_precision".to_string(),
            "type_udt_catalog".to_string(),
            "type_udt_schema".to_string(),
            "type_udt_name".to_string(),
            "scope_catalog".to_string(),
            "scope_schema".to_string(),
            "scope_name".to_string(),
            "maximum_cardinality".to_string(),
            "dtd_identifier".to_string(),
            "routine_body".to_string(),
            "routine_definition".to_string(),
            "external_name".to_string(),
            "external_language".to_string(),
            "parameter_style".to_string(),
            "is_deterministic".to_string(),
            "sql_data_access".to_string(),
            "is_null_call".to_string(),
            "sql_path".to_string(),
            "schema_level_routine".to_string(),
            "max_dynamic_result_sets".to_string(),
            "is_user_defined_cast".to_string(),
            "is_implicitly_invocable".to_string(),
            "security_type".to_string(),
            "to_sql_specific_catalog".to_string(),
            "to_sql_specific_schema".to_string(),
            "to_sql_specific_name".to_string(),
            "as_locator".to_string(),
            "created".to_string(),
            "last_altered".to_string(),
            "new_savepoint_level".to_string(),
            "is_udt_dependent".to_string(),
            "result_cast_from_data_type".to_string(),
            "result_cast_as_locator".to_string(),
            "result_cast_char_max_length".to_string(),
            "result_cast_char_octet_length".to_string(),
            "result_cast_char_set_catalog".to_string(),
            "result_cast_char_set_schema".to_string(),
            "result_cast_char_set_name".to_string(),
            "result_cast_collation_catalog".to_string(),
            "result_cast_collation_schema".to_string(),
            "result_cast_collation_name".to_string(),
            "result_cast_numeric_precision".to_string(),
            "result_cast_numeric_precision_radix".to_string(),
            "result_cast_numeric_scale".to_string(),
            "result_cast_datetime_precision".to_string(),
            "result_cast_interval_type".to_string(),
            "result_cast_interval_precision".to_string(),
            "result_cast_type_udt_catalog".to_string(),
            "result_cast_type_udt_schema".to_string(),
            "result_cast_type_udt_name".to_string(),
            "result_cast_scope_catalog".to_string(),
            "result_cast_scope_schema".to_string(),
            "result_cast_scope_name".to_string(),
            "result_cast_maximum_cardinality".to_string(),
            "result_cast_dtd_identifier".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Get system functions from pg_proc style data
        let functions = Self::get_system_functions_for_routines();

        // Apply WHERE clause filtering if present
        let filtered_functions = if let Some(where_clause) = &select.selection {
            Self::apply_routines_where_filter(&functions, where_clause)?
        } else {
            functions
        };

        // Build response rows
        let mut rows = Vec::new();
        for func_data in filtered_functions {
            let mut row = Vec::new();
            for &col_idx in &column_indices {
                if col_idx < all_columns.len() {
                    let column_name = &all_columns[col_idx];
                    let value = func_data.get(column_name).cloned().unwrap_or_else(|| b"".to_vec());
                    row.push(Some(value));
                } else {
                    row.push(Some(b"".to_vec()));
                }
            }
            rows.push(row);
        }

        let rows_count = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected: rows_count,
        })
    }

    fn get_system_functions_for_routines() -> Vec<HashMap<String, Vec<u8>>> {
        let mut functions = Vec::new();

        // Built-in SQL functions with enhanced metadata for information_schema.routines
        let function_data = vec![
            // String functions
            ("length", "FUNCTION", "text", "integer", "SQL", "CONTAINS_SQL"),
            ("lower", "FUNCTION", "text", "text", "SQL", "CONTAINS_SQL"),
            ("upper", "FUNCTION", "text", "text", "SQL", "CONTAINS_SQL"),
            ("substr", "FUNCTION", "text", "text", "SQL", "CONTAINS_SQL"),
            ("replace", "FUNCTION", "text", "text", "SQL", "CONTAINS_SQL"),
            ("trim", "FUNCTION", "text", "text", "SQL", "CONTAINS_SQL"),
            ("ltrim", "FUNCTION", "text", "text", "SQL", "CONTAINS_SQL"),
            ("rtrim", "FUNCTION", "text", "text", "SQL", "CONTAINS_SQL"),

            // Math functions
            ("abs", "FUNCTION", "numeric", "numeric", "SQL", "CONTAINS_SQL"),
            ("round", "FUNCTION", "numeric", "numeric", "SQL", "CONTAINS_SQL"),
            ("ceil", "FUNCTION", "numeric", "numeric", "SQL", "CONTAINS_SQL"),
            ("floor", "FUNCTION", "numeric", "numeric", "SQL", "CONTAINS_SQL"),
            ("sqrt", "FUNCTION", "numeric", "numeric", "SQL", "CONTAINS_SQL"),
            ("power", "FUNCTION", "numeric", "numeric", "SQL", "CONTAINS_SQL"),

            // Aggregate functions
            ("count", "FUNCTION", "bigint", "bigint", "SQL", "CONTAINS_SQL"),
            ("sum", "FUNCTION", "numeric", "numeric", "SQL", "CONTAINS_SQL"),
            ("avg", "FUNCTION", "numeric", "numeric", "SQL", "CONTAINS_SQL"),
            ("max", "FUNCTION", "anyelement", "anyelement", "SQL", "CONTAINS_SQL"),
            ("min", "FUNCTION", "anyelement", "anyelement", "SQL", "CONTAINS_SQL"),

            // Date/time functions
            ("now", "FUNCTION", "timestamp with time zone", "timestamp with time zone", "SQL", "CONTAINS_SQL"),
            ("current_timestamp", "FUNCTION", "timestamp with time zone", "timestamp with time zone", "SQL", "CONTAINS_SQL"),
            ("current_date", "FUNCTION", "date", "date", "SQL", "CONTAINS_SQL"),
            ("current_time", "FUNCTION", "time with time zone", "time with time zone", "SQL", "CONTAINS_SQL"),
            ("date_trunc", "FUNCTION", "timestamp with time zone", "timestamp with time zone", "SQL", "CONTAINS_SQL"),
            ("extract", "FUNCTION", "numeric", "double precision", "SQL", "CONTAINS_SQL"),

            // JSON functions
            ("json_agg", "FUNCTION", "json", "json", "SQL", "CONTAINS_SQL"),
            ("jsonb_agg", "FUNCTION", "jsonb", "jsonb", "SQL", "CONTAINS_SQL"),
            ("json_object_agg", "FUNCTION", "json", "json", "SQL", "CONTAINS_SQL"),
            ("jsonb_object_agg", "FUNCTION", "jsonb", "jsonb", "SQL", "CONTAINS_SQL"),
            ("json_extract", "FUNCTION", "json", "json", "SQL", "CONTAINS_SQL"),
            ("jsonb_extract", "FUNCTION", "jsonb", "jsonb", "SQL", "CONTAINS_SQL"),

            // Array functions
            ("array_agg", "FUNCTION", "anyarray", "anyarray", "SQL", "CONTAINS_SQL"),
            ("unnest", "FUNCTION", "anyelement", "setof anyelement", "SQL", "CONTAINS_SQL"),
            ("array_length", "FUNCTION", "integer", "integer", "SQL", "CONTAINS_SQL"),

            // UUID functions
            ("uuid_generate_v4", "FUNCTION", "uuid", "uuid", "SQL", "CONTAINS_SQL"),

            // System functions
            ("version", "FUNCTION", "text", "text", "SQL", "CONTAINS_SQL"),
            ("current_user", "FUNCTION", "name", "name", "SQL", "CONTAINS_SQL"),
            ("session_user", "FUNCTION", "name", "name", "SQL", "CONTAINS_SQL"),
            ("user", "FUNCTION", "name", "name", "SQL", "CONTAINS_SQL"),

            // Full-text search functions
            ("to_tsvector", "FUNCTION", "tsvector", "tsvector", "SQL", "CONTAINS_SQL"),
            ("to_tsquery", "FUNCTION", "tsquery", "tsquery", "SQL", "CONTAINS_SQL"),
            ("plainto_tsquery", "FUNCTION", "tsquery", "tsquery", "SQL", "CONTAINS_SQL"),
            ("ts_rank", "FUNCTION", "real", "real", "SQL", "CONTAINS_SQL"),
        ];

        for (name, routine_type, _param_type, return_type, language, data_access) in function_data {
            let mut func = HashMap::new();

            // Core identification
            func.insert("specific_catalog".to_string(), b"main".to_vec());
            func.insert("specific_schema".to_string(), b"pg_catalog".to_vec());
            func.insert("specific_name".to_string(), format!("{}_main_pg_catalog", name).into_bytes());
            func.insert("routine_catalog".to_string(), b"main".to_vec());
            func.insert("routine_schema".to_string(), b"pg_catalog".to_vec());
            func.insert("routine_name".to_string(), name.as_bytes().to_vec());
            func.insert("routine_type".to_string(), routine_type.as_bytes().to_vec());

            // Module information (NULL for built-in functions)
            func.insert("module_catalog".to_string(), b"".to_vec());
            func.insert("module_schema".to_string(), b"".to_vec());
            func.insert("module_name".to_string(), b"".to_vec());

            // Return type information
            func.insert("udt_catalog".to_string(), b"main".to_vec());
            func.insert("udt_schema".to_string(), b"pg_catalog".to_vec());
            func.insert("udt_name".to_string(), return_type.as_bytes().to_vec());
            func.insert("data_type".to_string(), return_type.as_bytes().to_vec());

            // Character/String type attributes (mostly NULL for our functions)
            func.insert("character_maximum_length".to_string(), b"".to_vec());
            func.insert("character_octet_length".to_string(), b"".to_vec());
            func.insert("character_set_catalog".to_string(), b"".to_vec());
            func.insert("character_set_schema".to_string(), b"".to_vec());
            func.insert("character_set_name".to_string(), b"".to_vec());
            func.insert("collation_catalog".to_string(), b"".to_vec());
            func.insert("collation_schema".to_string(), b"".to_vec());
            func.insert("collation_name".to_string(), b"".to_vec());

            // Numeric type attributes (mostly NULL for our functions)
            func.insert("numeric_precision".to_string(), b"".to_vec());
            func.insert("numeric_precision_radix".to_string(), b"".to_vec());
            func.insert("numeric_scale".to_string(), b"".to_vec());

            // DateTime attributes (mostly NULL)
            func.insert("datetime_precision".to_string(), b"".to_vec());
            func.insert("interval_type".to_string(), b"".to_vec());
            func.insert("interval_precision".to_string(), b"".to_vec());

            // Type information
            func.insert("type_udt_catalog".to_string(), b"main".to_vec());
            func.insert("type_udt_schema".to_string(), b"pg_catalog".to_vec());
            func.insert("type_udt_name".to_string(), return_type.as_bytes().to_vec());

            // Scope information (mostly NULL)
            func.insert("scope_catalog".to_string(), b"".to_vec());
            func.insert("scope_schema".to_string(), b"".to_vec());
            func.insert("scope_name".to_string(), b"".to_vec());

            // Cardinality and identifier
            func.insert("maximum_cardinality".to_string(), b"".to_vec());
            func.insert("dtd_identifier".to_string(), b"1".to_vec());

            // Function body and definition
            func.insert("routine_body".to_string(), b"EXTERNAL".to_vec());
            func.insert("routine_definition".to_string(), b"".to_vec());
            func.insert("external_name".to_string(), name.as_bytes().to_vec());
            func.insert("external_language".to_string(), language.as_bytes().to_vec());

            // Function characteristics
            func.insert("parameter_style".to_string(), b"SQL".to_vec());
            func.insert("is_deterministic".to_string(), b"NO".to_vec());
            func.insert("sql_data_access".to_string(), data_access.as_bytes().to_vec());
            func.insert("is_null_call".to_string(), b"YES".to_vec());
            func.insert("sql_path".to_string(), b"".to_vec());
            func.insert("schema_level_routine".to_string(), b"YES".to_vec());
            func.insert("max_dynamic_result_sets".to_string(), b"0".to_vec());
            func.insert("is_user_defined_cast".to_string(), b"NO".to_vec());
            func.insert("is_implicitly_invocable".to_string(), b"NO".to_vec());
            func.insert("security_type".to_string(), b"INVOKER".to_vec());

            // SQL-specific information (mostly NULL)
            func.insert("to_sql_specific_catalog".to_string(), b"".to_vec());
            func.insert("to_sql_specific_schema".to_string(), b"".to_vec());
            func.insert("to_sql_specific_name".to_string(), b"".to_vec());
            func.insert("as_locator".to_string(), b"NO".to_vec());

            // Timestamps (NULL for built-in functions)
            func.insert("created".to_string(), b"".to_vec());
            func.insert("last_altered".to_string(), b"".to_vec());

            // Additional attributes (mostly NULL)
            func.insert("new_savepoint_level".to_string(), b"".to_vec());
            func.insert("is_udt_dependent".to_string(), b"NO".to_vec());

            // Result cast information (all NULL for our simple functions)
            for prefix in &["result_cast_"] {
                for suffix in &["from_data_type", "as_locator", "char_max_length", "char_octet_length",
                              "char_set_catalog", "char_set_schema", "char_set_name", "collation_catalog",
                              "collation_schema", "collation_name", "numeric_precision", "numeric_precision_radix",
                              "numeric_scale", "datetime_precision", "interval_type", "interval_precision",
                              "type_udt_catalog", "type_udt_schema", "type_udt_name", "scope_catalog",
                              "scope_schema", "scope_name", "maximum_cardinality", "dtd_identifier"] {
                    func.insert(format!("{}{}", prefix, suffix), b"".to_vec());
                }
            }

            functions.push(func);
        }

        functions
    }

    fn apply_routines_where_filter(
        routines: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for routine in routines {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in routine {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(routine.clone());
            }
        }

        Ok(filtered)
    }

    pub async fn handle_information_schema_views_query(select: &Select, db: &DbHandler) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling information_schema.views query");

        // Define information_schema.views columns (PostgreSQL standard)
        let all_columns = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "view_definition".to_string(),
            "check_option".to_string(),
            "is_updatable".to_string(),
            "is_insertable_into".to_string(),
            "is_trigger_updatable".to_string(),
            "is_trigger_deletable".to_string(),
            "is_trigger_insertable_into".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Get views from SQLite
        let views = Self::get_sqlite_views(db).await?;

        // Apply WHERE clause filtering if present
        let filtered_views = if let Some(where_clause) = &select.selection {
            Self::apply_views_where_filter(&views, where_clause)?
        } else {
            views
        };

        // Build response rows
        let mut rows = Vec::new();
        for view_data in filtered_views {
            let mut row = Vec::new();
            for &col_idx in &column_indices {
                if col_idx < all_columns.len() {
                    let column_name = &all_columns[col_idx];
                    let value = view_data.get(column_name).cloned().unwrap_or_else(|| b"".to_vec());
                    row.push(Some(value));
                } else {
                    row.push(Some(b"".to_vec()));
                }
            }
            rows.push(row);
        }

        let rows_count = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected: rows_count,
        })
    }

    async fn get_sqlite_views(db: &DbHandler) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut views = Vec::new();

        // Use get_mut_connection to avoid recursion
        match db.get_mut_connection() {
            Ok(conn) => {
                let query = "SELECT name, sql FROM sqlite_master WHERE type='view' AND name NOT LIKE '__pgsqlite_%' AND name NOT LIKE 'pg_%' AND name NOT LIKE 'information_schema_%'";

                let mut stmt = conn.prepare(query).map_err(PgSqliteError::Sqlite)?;
                let rows = stmt.query_map([], |row| {
                    let name: String = row.get(0)?;
                    let sql: String = row.get(1)?;
                    Ok((name, sql))
                }).map_err(PgSqliteError::Sqlite)?;

                debug!("get_sqlite_views direct query");
                for view_info in rows.flatten() {
                    let (view_name, view_sql) = view_info;
                    debug!("Found view: {}", view_name);

                    let mut view = HashMap::new();

                    // Basic view information
                    view.insert("table_catalog".to_string(), b"main".to_vec());
                    view.insert("table_schema".to_string(), b"public".to_vec());
                    view.insert("view_name".to_string(), view_name.as_bytes().to_vec());
                    view.insert("table_name".to_string(), view_name.as_bytes().to_vec());

                    // View definition - extract from CREATE VIEW statement
                    let view_definition = Self::extract_view_definition(&view_sql);
                    view.insert("view_definition".to_string(), view_definition.as_bytes().to_vec());

                    // Standard PostgreSQL defaults for SQLite views
                    view.insert("check_option".to_string(), b"NONE".to_vec());
                    view.insert("is_updatable".to_string(), b"NO".to_vec());
                    view.insert("is_insertable_into".to_string(), b"NO".to_vec());
                    view.insert("is_trigger_updatable".to_string(), b"NO".to_vec());
                    view.insert("is_trigger_deletable".to_string(), b"NO".to_vec());
                    view.insert("is_trigger_insertable_into".to_string(), b"NO".to_vec());

                    views.push(view);
                }
            }
            Err(e) => {
                debug!("No database connection available: {:?}", e);
            }
        }

        Ok(views)
    }

    pub async fn handle_information_schema_referential_constraints_query(select: &Select, db: &DbHandler) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling information_schema.referential_constraints query");
        // Define information_schema.referential_constraints columns (PostgreSQL standard)
        let all_columns = vec![
            "constraint_catalog".to_string(),
            "constraint_schema".to_string(),
            "constraint_name".to_string(),
            "unique_constraint_catalog".to_string(),
            "unique_constraint_schema".to_string(),
            "unique_constraint_name".to_string(),
            "match_option".to_string(),
            "update_rule".to_string(),
            "delete_rule".to_string(),
        ];
        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);
        // Get referential constraints from pg_constraint
        let constraints = Self::get_referential_constraints(db).await?;
        // Apply WHERE clause filtering if present
        let filtered_constraints = if let Some(where_clause) = &select.selection {
            Self::apply_referential_constraints_where_filter(&constraints, where_clause)?
        } else {
            constraints
        };
        // Build response rows
        let mut rows = Vec::new();
        for constraint_data in filtered_constraints {
            let mut row = Vec::new();
            for &col_idx in &column_indices {
                if col_idx < all_columns.len() {
                    let column_name = &all_columns[col_idx];
                    let value = constraint_data.get(column_name).cloned().unwrap_or_else(|| b"".to_vec());
                    row.push(Some(value));
                } else {
                    row.push(Some(b"".to_vec()));
                }
            }
            rows.push(row);
        }
        let rows_count = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected: rows_count,
        })
    }

    async fn get_referential_constraints(db: &DbHandler) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut constraints = Vec::new();
        // Query pg_constraint for foreign key constraints only
        let query = "SELECT conname, confrelid FROM pg_constraint WHERE contype = 'f'";
        let constraints_response: Result<DbResponse, PgSqliteError> = match db.get_mut_connection() {
            Ok(conn) => {
                let mut stmt = conn.prepare(query)?;
                let mut rows = Vec::new();
                let mut query_rows = stmt.query([])?;
                while let Some(row) = query_rows.next()? {
                    let constraint_name: String = row.get(0)?;
                    let referenced_table_oid: String = row.get(1)?;
                    rows.push(vec![Some(constraint_name.into_bytes()), Some(referenced_table_oid.into_bytes())]);
                }
                Ok(DbResponse {
                    columns: vec!["conname".to_string(), "confrelid".to_string()],
                    rows,
                    rows_affected: 0,
                })
            }
            Err(e) => {
                debug!("Failed to get referential constraints: {:?}", e);
                return Ok(constraints);
            }
        };

        // Process each foreign key constraint
        for constraint_row in &constraints_response?.rows {
            if constraint_row.len() >= 2
                && let (Some(Some(name_bytes)), Some(Some(ref_oid_bytes))) =
                    (constraint_row.first(), constraint_row.get(1)) {
                    let constraint_name = String::from_utf8_lossy(name_bytes).to_string();
                    let referenced_table_oid = String::from_utf8_lossy(ref_oid_bytes).to_string();

                    debug!("Processing referential constraint: {}", constraint_name);
                    let mut constraint = HashMap::new();

                    // Basic constraint information
                    constraint.insert("constraint_catalog".to_string(), b"main".to_vec());
                    constraint.insert("constraint_schema".to_string(), b"public".to_vec());
                    constraint.insert("constraint_name".to_string(), constraint_name.as_bytes().to_vec());

                    // Referenced constraint information - try to find the primary key constraint
                    // of the referenced table
                    let referenced_constraint_name = Self::find_referenced_constraint_name(db, &referenced_table_oid).await
                        .unwrap_or_else(|_| format!("{}_pkey", Self::get_table_name_from_oid(&referenced_table_oid)));

                    constraint.insert("unique_constraint_catalog".to_string(), b"main".to_vec());
                    constraint.insert("unique_constraint_schema".to_string(), b"public".to_vec());
                    constraint.insert("unique_constraint_name".to_string(), referenced_constraint_name.as_bytes().to_vec());

                    // SQLite foreign key defaults (SQLite doesn't store these explicitly)
                    constraint.insert("match_option".to_string(), b"NONE".to_vec());
                    constraint.insert("update_rule".to_string(), b"NO ACTION".to_vec());
                    constraint.insert("delete_rule".to_string(), b"NO ACTION".to_vec());

                    constraints.push(constraint);
                }
        }
        Ok(constraints)
    }

    async fn find_referenced_constraint_name(db: &DbHandler, table_oid: &str) -> Result<String, PgSqliteError> {
        // Try to find the primary key constraint for the referenced table
        let query = "SELECT conname FROM pg_constraint WHERE conrelid = ?1 AND contype = 'p'";
        match db.get_mut_connection() {
            Ok(conn) => {
                let mut stmt = conn.prepare(query)?;
                match stmt.query_row([table_oid], |row| row.get::<_, String>(0)) {
                    Ok(constraint_name) => Ok(constraint_name),
                    Err(_) => {
                        // Fallback: generate a primary key constraint name
                        let table_name = Self::get_table_name_from_oid(table_oid);
                        Ok(format!("{}_pkey", table_name))
                    }
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    fn get_table_name_from_oid(oid: &str) -> String {
        // This is a simplified approach - in practice we'd query pg_class
        // For now, extract table name from common OID patterns
        if let Ok(oid_num) = oid.parse::<u64>() {
            format!("table_{}", oid_num % 1000)
        } else {
            "unknown_table".to_string()
        }
    }

    fn apply_referential_constraints_where_filter(
        constraints: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for constraint in constraints {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in constraint {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(constraint.clone());
            }
        }

        Ok(filtered)
    }

    pub async fn handle_information_schema_referential_constraints_query_with_session(select: &Select, db: &DbHandler, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        // Define information_schema.referential_constraints columns (PostgreSQL standard)
        let all_columns = vec![
            "constraint_catalog".to_string(),
            "constraint_schema".to_string(),
            "constraint_name".to_string(),
            "unique_constraint_catalog".to_string(),
            "unique_constraint_schema".to_string(),
            "unique_constraint_name".to_string(),
            "match_option".to_string(),
            "update_rule".to_string(),
            "delete_rule".to_string(),
        ];
        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);
        // Get referential constraints using session connection
        let constraints = Self::get_referential_constraints_with_session(db, session_id).await?;
        // Apply WHERE clause filtering if present
        let filtered_constraints = if let Some(where_clause) = &select.selection {
            Self::apply_referential_constraints_where_filter(&constraints, where_clause)?
        } else {
            constraints
        };
        // Build response rows
        let mut rows = Vec::new();
        for constraint_data in filtered_constraints {
            let mut row = Vec::new();
            for &col_idx in &column_indices {
                if col_idx < all_columns.len() {
                    let column_name = &all_columns[col_idx];
                    let value = constraint_data.get(column_name).cloned().unwrap_or_else(|| b"".to_vec());
                    row.push(Some(value));
                } else {
                    row.push(Some(b"".to_vec()));
                }
            }
            rows.push(row);
        }
        let rows_count = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected: rows_count,
        })
    }

    async fn get_referential_constraints_with_session(db: &DbHandler, session_id: &Uuid) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut constraints = Vec::new();
        // Use session connection to see constraints created in this session
        let constraints_response = match db.connection_manager().execute_with_session(session_id, |conn| {
            let query = "SELECT conname, confrelid FROM pg_constraint WHERE contype = 'f'";
            let mut stmt = conn.prepare(query)?;
            let mut rows = Vec::new();
            let mut query_rows = stmt.query([])?;
            while let Some(row) = query_rows.next()? {
                let constraint_name: String = row.get(0)?;
                let referenced_table_oid: String = row.get(1)?;  // OIDs are stored as TEXT in pg_constraint
                rows.push(vec![Some(constraint_name.into_bytes()), Some(referenced_table_oid.into_bytes())]);
            }
            Ok(DbResponse {
                columns: vec!["conname".to_string(), "confrelid".to_string()],
                rows,
                rows_affected: 0,
            })
        }) {
            Ok(response) => response,
            Err(e) => {
                debug!("Failed to get referential constraints from session: {:?}", e);
                return Ok(constraints);
            }
        };

        // Process each foreign key constraint
        for constraint_row in &constraints_response.rows {
            if constraint_row.len() >= 2
                && let (Some(Some(name_bytes)), Some(Some(ref_oid_bytes))) =
                    (constraint_row.first(), constraint_row.get(1)) {
                    let constraint_name = String::from_utf8_lossy(name_bytes).to_string();
                    let referenced_table_oid = String::from_utf8_lossy(ref_oid_bytes).to_string();

                    debug!("Processing referential constraint with session: {}", constraint_name);
                    let mut constraint = HashMap::new();

                    // Basic constraint information
                    constraint.insert("constraint_catalog".to_string(), b"main".to_vec());
                    constraint.insert("constraint_schema".to_string(), b"public".to_vec());
                    constraint.insert("constraint_name".to_string(), constraint_name.as_bytes().to_vec());

                    // Referenced constraint information - try to find the primary key constraint
                    // of the referenced table using session connection
                    let referenced_constraint_name = Self::find_referenced_constraint_name_with_session(db, session_id, &referenced_table_oid).await
                        .unwrap_or_else(|_| format!("{}_pkey", Self::get_table_name_from_oid(&referenced_table_oid)));

                    constraint.insert("unique_constraint_catalog".to_string(), b"main".to_vec());
                    constraint.insert("unique_constraint_schema".to_string(), b"public".to_vec());
                    constraint.insert("unique_constraint_name".to_string(), referenced_constraint_name.as_bytes().to_vec());

                    // SQLite foreign key defaults (SQLite doesn't store these explicitly)
                    constraint.insert("match_option".to_string(), b"NONE".to_vec());
                    constraint.insert("update_rule".to_string(), b"NO ACTION".to_vec());
                    constraint.insert("delete_rule".to_string(), b"NO ACTION".to_vec());

                    constraints.push(constraint);
                }
        }
        Ok(constraints)
    }

    async fn find_referenced_constraint_name_with_session(db: &DbHandler, session_id: &Uuid, table_oid: &str) -> Result<String, PgSqliteError> {
        // Try to find the primary key constraint for the referenced table using session connection
        match db.connection_manager().execute_with_session(session_id, |conn| {
            let query = "SELECT conname FROM pg_constraint WHERE conrelid = ?1 AND contype = 'p'";
            let mut stmt = conn.prepare(query)?;
            match stmt.query_row([table_oid], |row| row.get::<_, String>(0)) {
                Ok(constraint_name) => Ok(constraint_name),
                Err(_) => {
                    // Fallback: generate a primary key constraint name
                    let table_name = Self::get_table_name_from_oid(table_oid);
                    Ok(format!("{}_pkey", table_name))
                }
            }
        }) {
            Ok(constraint_name) => Ok(constraint_name),
            Err(e) => Err(e),
        }
    }

    pub async fn handle_information_schema_views_query_with_session(select: &Select, db: &DbHandler, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling information_schema.views query with session");
        // Define information_schema.views columns (PostgreSQL standard)
        let all_columns = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "view_definition".to_string(),
            "check_option".to_string(),
            "is_updatable".to_string(),
            "is_insertable_into".to_string(),
            "is_trigger_updatable".to_string(),
            "is_trigger_deletable".to_string(),
            "is_trigger_insertable_into".to_string(),
        ];
        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);
        // Get views from SQLite using session connection
        let views = Self::get_sqlite_views_with_session(db, session_id).await?;
        // Apply WHERE clause filtering if present
        let filtered_views = if let Some(where_clause) = &select.selection {
            Self::apply_views_where_filter(&views, where_clause)?
        } else {
            views
        };
        // Build response rows
        let mut rows = Vec::new();
        for view_data in filtered_views {
            let mut row = Vec::new();
            for &col_idx in &column_indices {
                if col_idx < all_columns.len() {
                    let column_name = &all_columns[col_idx];
                    let value = view_data.get(column_name).cloned().unwrap_or_else(|| b"".to_vec());
                    row.push(Some(value));
                } else {
                    row.push(Some(b"".to_vec()));
                }
            }
            rows.push(row);
        }
        let rows_count = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected: rows_count,
        })
    }

    async fn get_sqlite_views_with_session(db: &DbHandler, session_id: &Uuid) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut views = Vec::new();
        // Use session connection to see views created in this session
        let views_response = match db.connection_manager().execute_with_session(session_id, |conn| {
            let query = "SELECT name, sql FROM sqlite_master WHERE type='view' AND name NOT LIKE '__pgsqlite_%' AND name NOT LIKE 'pg_%' AND name NOT LIKE 'information_schema_%'";
            let mut stmt = conn.prepare(query)?;
            let mut rows = Vec::new();
            let mut query_rows = stmt.query([])?;
            while let Some(row) = query_rows.next()? {
                let name: String = row.get(0)?;
                let sql: String = row.get(1)?;
                rows.push(vec![Some(name.into_bytes()), Some(sql.into_bytes())]);
            }
            Ok(DbResponse {
                columns: vec!["name".to_string(), "sql".to_string()],
                rows,
                rows_affected: 0,
            })
        }) {
            Ok(response) => response,
            Err(e) => {
                debug!("Failed to get views from session: {:?}", e);
                return Ok(views);
            }
        };

        // Process each view
        for view_row in &views_response.rows {
            if view_row.len() >= 2
                && let (Some(Some(name_bytes)), Some(Some(sql_bytes))) =
                    (view_row.first(), view_row.get(1)) {
                    let view_name = String::from_utf8_lossy(name_bytes).to_string();
                    let view_sql = String::from_utf8_lossy(sql_bytes).to_string();

                    debug!("Found view with session: {}", view_name);
                    let mut view = HashMap::new();
                    // Basic view information
                    view.insert("table_catalog".to_string(), b"main".to_vec());
                    view.insert("table_schema".to_string(), b"public".to_vec());
                    view.insert("view_name".to_string(), view_name.as_bytes().to_vec());
                    view.insert("table_name".to_string(), view_name.as_bytes().to_vec());
                    // View definition - extract from CREATE VIEW statement
                    let view_definition = Self::extract_view_definition(&view_sql);
                    view.insert("view_definition".to_string(), view_definition.as_bytes().to_vec());
                    // Standard PostgreSQL defaults for SQLite views
                    view.insert("check_option".to_string(), b"NONE".to_vec());
                    view.insert("is_updatable".to_string(), b"NO".to_vec());
                    view.insert("is_insertable_into".to_string(), b"NO".to_vec());
                    view.insert("is_trigger_updatable".to_string(), b"NO".to_vec());
                    view.insert("is_trigger_deletable".to_string(), b"NO".to_vec());
                    view.insert("is_trigger_insertable_into".to_string(), b"NO".to_vec());
                    views.push(view);
                }
        }
        Ok(views)
    }

    fn extract_view_definition(sql: &str) -> String {
        // Extract the SELECT part from "CREATE VIEW name AS SELECT ..."
        // Be more careful to find the correct AS keyword (after CREATE VIEW viewname)
        let upper_sql = sql.to_uppercase();

        // Look for CREATE VIEW pattern first
        if let Some(create_view_pos) = upper_sql.find("CREATE VIEW") {
            // Find the AS that comes after CREATE VIEW and the view name, but before any SELECT
            let after_create_view = &upper_sql[create_view_pos + 11..]; // Skip "CREATE VIEW"

            // Find the AS keyword that should come after the view name but before SELECT
            // Look for patterns that indicate the main AS clause (not column aliases)
            let mut as_candidates = Vec::new();

            // Collect all possible AS positions
            if let Some(pos) = after_create_view.find(" AS ") {
                as_candidates.push((pos, 4));
            }
            if let Some(pos) = after_create_view.find("\nAS ") {
                as_candidates.push((pos, 4));
            }
            if let Some(pos) = after_create_view.find(" AS\n") {
                as_candidates.push((pos, 4));
            }
            if let Some(pos) = after_create_view.find("\nAS\n") {
                as_candidates.push((pos, 4));
            }

            // Take the first AS that appears (should be the one after the view name)
            if let Some(&(as_pos, skip_len)) = as_candidates.iter().min() {
                let actual_pos = create_view_pos + 11 + as_pos + skip_len;
                let select_part = &sql[actual_pos..];
                return select_part.trim().to_string();
            } else if let Some(as_pos) = after_create_view.find("AS") {
                // Generic AS fallback
                let actual_pos = create_view_pos + 11 + as_pos + 2;
                let select_part = &sql[actual_pos..];
                return select_part.trim().to_string();
            }
        }

        // Fallback - return the trimmed SQL
        sql.trim().to_string()
    }

    fn apply_views_where_filter(
        views: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for view in views {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in view {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(view.clone());
            }
        }

        Ok(filtered)
    }

    pub async fn handle_information_schema_check_constraints_query_with_session(select: &Select, db: &DbHandler, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        // Define information_schema.check_constraints columns (PostgreSQL standard)
        let all_columns = vec![
            "constraint_catalog".to_string(),
            "constraint_schema".to_string(),
            "constraint_name".to_string(),
            "check_clause".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Get check constraints using session connection
        let constraints = Self::get_check_constraints_with_session(db, session_id).await?;

        // Apply WHERE clause filtering if present
        let filtered_constraints = if let Some(where_clause) = &select.selection {
            Self::apply_check_constraints_where_filter(&constraints, where_clause)?
        } else {
            constraints
        };

        // Build response rows
        let mut rows = Vec::new();
        for constraint_data in filtered_constraints {
            let mut row = Vec::new();
            for &col_idx in &column_indices {
                if col_idx < all_columns.len() {
                    let column_name = &all_columns[col_idx];
                    let value = constraint_data.get(column_name).cloned().unwrap_or_else(|| b"".to_vec());
                    row.push(Some(value));
                } else {
                    row.push(Some(b"".to_vec()));
                }
            }
            rows.push(row);
        }

        let rows_count = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected: rows_count,
        })
    }

    async fn get_check_constraints_with_session(db: &DbHandler, session_id: &Uuid) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut constraints = Vec::new();

        // Use session connection to see constraints created in this session
        let constraints_response = match db.connection_manager().execute_with_session(session_id, |conn| {
            let query = "SELECT conname, consrc FROM pg_constraint WHERE contype = 'c'";
            let mut stmt = conn.prepare(query)?;
            let mut rows = Vec::new();
            let mut query_rows = stmt.query([])?;
            while let Some(row) = query_rows.next()? {
                let constraint_name: String = row.get(0)?;
                let check_clause: String = row.get(1)?;
                rows.push(vec![Some(constraint_name.into_bytes()), Some(check_clause.into_bytes())]);
            }
            Ok(DbResponse {
                columns: vec!["conname".to_string(), "consrc".to_string()],
                rows,
                rows_affected: 0,
            })
        }) {
            Ok(response) => response,
            Err(e) => {
                debug!("Failed to get check constraints from session: {:?}", e);
                return Ok(constraints);
            }
        };

        // Process each check constraint
        for constraint_row in &constraints_response.rows {
            if constraint_row.len() >= 2
                && let (Some(Some(name_bytes)), Some(Some(clause_bytes))) =
                    (constraint_row.first(), constraint_row.get(1)) {
                    let constraint_name = String::from_utf8_lossy(name_bytes).to_string();
                    let check_clause = String::from_utf8_lossy(clause_bytes).to_string();

                    let mut constraint = HashMap::new();

                    // Standard information_schema.check_constraints columns
                    constraint.insert("constraint_catalog".to_string(), b"main".to_vec());
                    constraint.insert("constraint_schema".to_string(), b"public".to_vec());
                    constraint.insert("constraint_name".to_string(), constraint_name.as_bytes().to_vec());
                    constraint.insert("check_clause".to_string(), check_clause.as_bytes().to_vec());

                    constraints.push(constraint);
                }
        }

        Ok(constraints)
    }

    fn apply_check_constraints_where_filter(
        constraints: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for constraint in constraints {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in constraint {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(constraint.clone());
            }
        }

        Ok(filtered)
    }

    /// Handle information_schema.triggers queries with session-aware connection
    pub async fn handle_information_schema_triggers_query_with_session(select: &Select, db: &DbHandler, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        // Define information_schema.triggers columns (PostgreSQL standard)
        let all_columns = vec![
            "trigger_catalog".to_string(),
            "trigger_schema".to_string(),
            "trigger_name".to_string(),
            "event_manipulation".to_string(),
            "event_object_catalog".to_string(),
            "event_object_schema".to_string(),
            "event_object_table".to_string(),
            "action_order".to_string(),
            "action_condition".to_string(),
            "action_statement".to_string(),
            "action_orientation".to_string(),
            "action_timing".to_string(),
            "action_reference_old_table".to_string(),
            "action_reference_new_table".to_string(),
            "action_reference_old_row".to_string(),
            "action_reference_new_row".to_string(),
            "created".to_string(),
        ];

        // Extract selected columns
        let (selected_columns, column_indices) = Self::extract_selected_columns(select, &all_columns);

        // Get triggers using session connection
        let triggers = Self::get_triggers_with_session(db, session_id).await?;

        // Apply WHERE clause filtering if present
        let filtered_triggers = if let Some(where_clause) = &select.selection {
            Self::apply_triggers_where_filter(&triggers, where_clause)?
        } else {
            triggers
        };

        // Build response rows with selected columns
        let mut result_rows = Vec::new();
        for trigger in &filtered_triggers {
            let mut row = Vec::new();
            for &col_idx in &column_indices {
                if let Some(column_name) = all_columns.get(col_idx) {
                    let value = trigger.get(column_name)
                        .cloned()
                        .unwrap_or_else(|| "".to_string().into_bytes());
                    row.push(Some(value));
                } else {
                    row.push(Some("".to_string().into_bytes()));
                }
            }
            result_rows.push(row);
        }

        let rows_affected = result_rows.len();
        debug!("Returning {} rows for information_schema.triggers query with {} columns: {:?}",
               rows_affected, selected_columns.len(), selected_columns);

        Ok(DbResponse {
            columns: selected_columns,
            rows: result_rows,
            rows_affected,
        })
    }

    /// Get triggers from SQLite using session connection
    async fn get_triggers_with_session(db: &DbHandler, session_id: &Uuid) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut triggers = Vec::new();

        // Query SQLite's sqlite_master for trigger information using direct connection access
        let triggers_data = match db.with_session_connection(session_id, |conn| {
            let mut stmt = conn.prepare("SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' ORDER BY name")?;
            let mut rows = Vec::new();
            let mut prepared_rows = stmt.query([])?;

            while let Some(row) = prepared_rows.next()? {
                let name: String = row.get(0)?;
                let tbl_name: String = row.get(1)?;
                let sql: String = row.get(2)?;
                rows.push((name, tbl_name, sql));
            }

            Ok::<Vec<(String, String, String)>, rusqlite::Error>(rows)
        }).await {
            Ok(data) => data,
            Err(e) => {
                debug!("Failed to get triggers from session: {:?}", e);
                return Ok(triggers);
            }
        };

        // Process each trigger
        for (trigger_name, table_name, trigger_sql) in &triggers_data {

            // Parse trigger SQL to extract details
            let (timing, event, orientation) = Self::parse_trigger_sql(trigger_sql);

            let mut trigger = HashMap::new();
            trigger.insert("trigger_catalog".to_string(), "main".to_string().into_bytes());
            trigger.insert("trigger_schema".to_string(), "public".to_string().into_bytes());
            trigger.insert("trigger_name".to_string(), trigger_name.clone().into_bytes());
            trigger.insert("event_manipulation".to_string(), event.into_bytes());
            trigger.insert("event_object_catalog".to_string(), "main".to_string().into_bytes());
            trigger.insert("event_object_schema".to_string(), "public".to_string().into_bytes());
            trigger.insert("event_object_table".to_string(), table_name.clone().into_bytes());
            trigger.insert("action_order".to_string(), "1".to_string().into_bytes());
            trigger.insert("action_condition".to_string(), "".to_string().into_bytes()); // SQLite doesn't expose WHEN conditions separately
            trigger.insert("action_statement".to_string(), trigger_sql.clone().into_bytes());
            trigger.insert("action_orientation".to_string(), orientation.into_bytes());
            trigger.insert("action_timing".to_string(), timing.into_bytes());
            trigger.insert("action_reference_old_table".to_string(), "".to_string().into_bytes());
            trigger.insert("action_reference_new_table".to_string(), "".to_string().into_bytes());
            trigger.insert("action_reference_old_row".to_string(), "".to_string().into_bytes());
            trigger.insert("action_reference_new_row".to_string(), "".to_string().into_bytes());
            trigger.insert("created".to_string(), "".to_string().into_bytes());

            triggers.push(trigger);
        }

        Ok(triggers)
    }

    /// Parse SQLite trigger SQL to extract timing, event, and orientation
    fn parse_trigger_sql(sql: &str) -> (String, String, String) {
        let sql_upper = sql.to_uppercase();

        // Parse timing (BEFORE, AFTER, INSTEAD OF)
        let timing = if sql_upper.contains("BEFORE") {
            "BEFORE".to_string()
        } else if sql_upper.contains("AFTER") {
            "AFTER".to_string()
        } else if sql_upper.contains("INSTEAD OF") {
            "INSTEAD OF".to_string()
        } else {
            "BEFORE".to_string() // Default
        };

        // Parse event (INSERT, UPDATE, DELETE) by looking at the trigger definition structure
        // SQLite trigger syntax: CREATE TRIGGER name [BEFORE|AFTER|INSTEAD OF] [INSERT|UPDATE|DELETE] ON table
        let event = if let Some(on_pos) = sql_upper.find(" ON ") {
            // Look before " ON " for the trigger event
            let before_on = &sql_upper[..on_pos];

            // Look for the event words in the trigger definition
            if before_on.contains(" DELETE") || before_on.ends_with("DELETE") {
                "DELETE".to_string()
            } else if before_on.contains(" UPDATE") || before_on.ends_with("UPDATE") {
                "UPDATE".to_string()
            } else {
                "INSERT".to_string() // Default (includes INSERT or unknown)
            }
        } else {
            // Fallback to original logic if no " ON " found
            if sql_upper.contains("DELETE") {
                "DELETE".to_string()
            } else if sql_upper.contains("UPDATE") {
                "UPDATE".to_string()
            } else {
                "INSERT".to_string() // Default (includes INSERT or unknown)
            }
        };

        // SQLite triggers are always ROW-level
        let orientation = "ROW".to_string();

        (timing, event, orientation)
    }

    /// Apply WHERE clause filtering to triggers
    fn apply_triggers_where_filter(
        triggers: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for trigger in triggers {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in trigger {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(trigger.clone());
            }
        }

        Ok(filtered)
    }
}
