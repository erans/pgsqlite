use pgsqlite::session::db_handler::DbHandler;
use pgsqlite::catalog::pg_trigger::PgTriggerHandler;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tempfile::TempDir;

#[tokio::test]
async fn test_pg_trigger_basic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_pg_trigger.db");
    let db = DbHandler::new(db_path.to_str().unwrap()).unwrap();

    db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
    .await
    .unwrap();

    db.execute("CREATE TRIGGER test_trigger AFTER INSERT ON test_table BEGIN SELECT 1; END")
    .await
    .unwrap();

    let sql = "SELECT tgname, tgrelid, tgtype, tgenabled FROM pg_trigger";
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    let statement = &ast[0];

    if let sqlparser::ast::Statement::Query(query) = statement {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            let result = PgTriggerHandler::handle_query(select, &db).await.unwrap();

            assert_eq!(result.rows.len(), 1);

            let tgname = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
            assert_eq!(tgname, "test_trigger");

            let tgrelid_str = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
            let tgrelid: u32 = tgrelid_str.parse().unwrap();
            assert!(tgrelid >= 16384);

            let tgtype_str = String::from_utf8(result.rows[0][2].as_ref().unwrap().clone()).unwrap();
            let tgtype: i16 = tgtype_str.parse().unwrap();
            assert!(tgtype > 0);

            let tgenabled = String::from_utf8(result.rows[0][3].as_ref().unwrap().clone()).unwrap();
            assert_eq!(tgenabled, "O");
        }
    }
}

#[tokio::test]
async fn test_pg_trigger_before_update() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_pg_trigger_before.db");
    let db = DbHandler::new(db_path.to_str().unwrap()).unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    .await
    .unwrap();

    db.execute("CREATE TRIGGER audit_trigger BEFORE UPDATE ON users BEGIN SELECT 1; END")
    .await
    .unwrap();

    let sql = "SELECT tgname, tgtype FROM pg_trigger WHERE tgname = 'audit_trigger'";
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    let statement = &ast[0];

    if let sqlparser::ast::Statement::Query(query) = statement {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            let result = PgTriggerHandler::handle_query(select, &db).await.unwrap();

            assert_eq!(result.rows.len(), 1);

            let tgname = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
            assert_eq!(tgname, "audit_trigger");

            let tgtype_str = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
            let tgtype: i16 = tgtype_str.parse().unwrap();

            assert_eq!(tgtype & 1, 1);
            assert_eq!(tgtype & 2, 2);
            assert_eq!(tgtype & 16, 16);
        }
    }
}

#[tokio::test]
async fn test_pg_trigger_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_pg_trigger_delete.db");
    let db = DbHandler::new(db_path.to_str().unwrap()).unwrap();

    db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, price REAL)")
    .await
    .unwrap();

    db.execute("CREATE TRIGGER cleanup_trigger AFTER DELETE ON products BEGIN SELECT 1; END")
    .await
    .unwrap();

    let sql = "SELECT tgname, tgtype FROM pg_trigger WHERE tgname = 'cleanup_trigger'";
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    let statement = &ast[0];

    if let sqlparser::ast::Statement::Query(query) = statement {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            let result = PgTriggerHandler::handle_query(select, &db).await.unwrap();

            assert_eq!(result.rows.len(), 1);

            let tgtype_str = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
            let tgtype: i16 = tgtype_str.parse().unwrap();

            assert_eq!(tgtype & 1, 1);
            assert_eq!(tgtype & 8, 8);
        }
    }
}

#[tokio::test]
async fn test_pg_trigger_empty() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_pg_trigger_empty.db");
    let db = DbHandler::new(db_path.to_str().unwrap()).unwrap();

    db.execute("CREATE TABLE empty_table (id INTEGER PRIMARY KEY)")
    .await
    .unwrap();

    let sql = "SELECT tgname FROM pg_trigger";
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    let statement = &ast[0];

    if let sqlparser::ast::Statement::Query(query) = statement {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            let result = PgTriggerHandler::handle_query(select, &db).await.unwrap();
            assert_eq!(result.rows.len(), 0);
        }
    }
}

#[tokio::test]
async fn test_pg_trigger_multiple() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_pg_trigger_multiple.db");
    let db = DbHandler::new(db_path.to_str().unwrap()).unwrap();

    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)")
    .await
    .unwrap();

    db.execute("CREATE TRIGGER before_insert_trigger BEFORE INSERT ON orders BEGIN SELECT 1; END")
    .await
    .unwrap();

    db.execute("CREATE TRIGGER after_insert_trigger AFTER INSERT ON orders BEGIN SELECT 2; END")
    .await
    .unwrap();

    let sql = "SELECT tgname FROM pg_trigger ORDER BY tgname";
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
    let statement = &ast[0];

    if let sqlparser::ast::Statement::Query(query) = statement {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            let result = PgTriggerHandler::handle_query(select, &db).await.unwrap();

            assert_eq!(result.rows.len(), 2);

            let name1 = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
            let name2 = String::from_utf8(result.rows[1][0].as_ref().unwrap().clone()).unwrap();

            assert_eq!(name1, "after_insert_trigger");
            assert_eq!(name2, "before_insert_trigger");
        }
    }
}
