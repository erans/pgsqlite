use clap::Parser;
use pgsqlite::config::Config;
use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

fn text_cell(row: &[Option<Vec<u8>>], index: usize) -> String {
    String::from_utf8(row[index].clone().expect("cell should not be NULL")).unwrap()
}

/// Regression test for the `--in-memory` fix in `src/main.rs` (issue #87 fix round).
///
/// `src/main.rs` opens `file:pgsqlite_mem?mode=memory&cache=shared` (not bare
/// `:memory:`) so that migrations run on one SQLite connection stay visible to the
/// *other* SQLite connections opened for each client session. `DbHandler` keeps a
/// `_memory_keepalive` connection alive for exactly this reason
/// (`src/session/db_handler.rs`): a shared-cache memory database is destroyed the
/// moment its last connection closes, so without a keepalive connection, or without
/// the shared-cache URI, a second session's connection would open its own private,
/// empty in-memory database and never see the migrated `pg_class`/`pg_namespace`
/// views or any table created by another session.
///
/// This test opens a `DbHandler` on the URI that `--in-memory` actually resolves to
/// -- obtained from `Config::resolve_db_path()`, the same call `src/main.rs` makes, so
/// reverting that decision to a bare `:memory:` breaks this test rather than leaving it
/// green. It creates a table on one (temporary, memory-mode) connection via
/// `DbHandler::execute`, then queries `pg_class` from a second, independently created
/// session connection and asserts the table shows up. Before the shared-cache fix this
/// hard-errored with "no such table: pg_class" because the second connection's database
/// was empty.
#[tokio::test]
async fn test_in_memory_pg_class_visible_across_sessions() {
    // Exactly what `pgsqlite --in-memory` resolves its database path to.
    let db_path = Config::parse_from(["pgsqlite", "--in-memory"]).resolve_db_path();
    assert_ne!(
        db_path, ":memory:",
        "--in-memory must not resolve to a bare :memory:; that gives every connection \
         its own private database and hides the migrated catalog views from sessions"
    );

    let db_handler = Arc::new(
        DbHandler::new(&db_path)
            .expect("DbHandler::new should succeed on the shared-cache in-memory URI"),
    );

    // Create a user table. For memory databases, DbHandler::execute opens its own
    // temporary session connection, runs the statement, and tears the connection
    // down again -- so by the time this returns, no connection this test controls
    // is the one that created the table.
    db_handler
        .execute("CREATE TABLE regression_users (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .expect("CREATE TABLE should succeed");

    // Open a brand-new session connection, independent of the one used above, and
    // query pg_class through it -- this is the exact scenario a second psql/ORM
    // connection to `pgsqlite --in-memory` exercises.
    let session_id = Uuid::new_v4();
    db_handler
        .create_session_connection(session_id)
        .await
        .expect("creating a second session connection should succeed");

    let response = db_handler
        .query_with_session(
            "SELECT relname FROM pg_class WHERE relname = 'regression_users'",
            &session_id,
        )
        .await
        .expect(
            "querying pg_class from a second session must not hard-error; if this fails, \
             the shared-cache in-memory URI regressed back to per-connection private databases",
        );

    assert_eq!(
        response.rows.len(),
        1,
        "expected the table created on another session to be visible via pg_class"
    );
    assert_eq!(text_cell(&response.rows[0], 0), "regression_users");

    db_handler.remove_session_connection(&session_id);
}
