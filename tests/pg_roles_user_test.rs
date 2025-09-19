use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;

#[tokio::test]
async fn test_pg_roles_view_exists() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_roles.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Test basic pg_roles view query
    let result = db_handler.query("SELECT COUNT(*) FROM pg_roles").await.unwrap();
    assert!(!result.rows.is_empty(), "Should get a count result");

    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = String::from_utf8(count_bytes.clone()).unwrap();
    let count: i32 = count_str.parse().unwrap();

    // Should have 3 default roles: postgres, public, pgsqlite_user
    assert_eq!(count, 3, "Should have 3 default roles, got {}", count);
    println!("✅ pg_roles view contains {} roles (expected 3)", count);

    // Test column structure
    let result = db_handler.query("SELECT oid, rolname, rolsuper, rolcanlogin FROM pg_roles LIMIT 1").await.unwrap();
    assert_eq!(result.columns.len(), 4, "Should have 4 columns");
    assert_eq!(result.columns, vec!["oid", "rolname", "rolsuper", "rolcanlogin"]);
    println!("✅ pg_roles has correct column structure");

    println!("🎉 pg_roles SQLite view test passed!");
}

#[tokio::test]
async fn test_pg_user_view_exists() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_user.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Test basic pg_user view query
    let result = db_handler.query("SELECT COUNT(*) FROM pg_user").await.unwrap();
    assert!(!result.rows.is_empty(), "Should get a count result");

    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = String::from_utf8(count_bytes.clone()).unwrap();
    let count: i32 = count_str.parse().unwrap();

    // Should have 2 default users: postgres, pgsqlite_user (public is not a user)
    assert_eq!(count, 2, "Should have 2 default users, got {}", count);
    println!("✅ pg_user view contains {} users (expected 2)", count);

    // Test column structure
    let result = db_handler.query("SELECT usename, usesysid, usesuper, usecreatedb FROM pg_user LIMIT 1").await.unwrap();
    assert_eq!(result.columns.len(), 4, "Should have 4 columns");
    assert_eq!(result.columns, vec!["usename", "usesysid", "usesuper", "usecreatedb"]);
    println!("✅ pg_user has correct column structure");

    println!("🎉 pg_user SQLite view test passed!");
}

#[tokio::test]
async fn test_pg_roles_specific_queries() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_roles_specific.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Test postgres role query (common Django pattern)
    let result = db_handler.query("SELECT rolname, rolsuper FROM pg_roles WHERE rolname = 'postgres'").await.unwrap();
    assert!(!result.rows.is_empty(), "Should find postgres role");

    let rolname_bytes = result.rows[0][0].as_ref().unwrap();
    let rolname = String::from_utf8(rolname_bytes.clone()).unwrap();
    assert_eq!(rolname, "postgres", "Should find postgres role");

    let rolsuper_bytes = result.rows[0][1].as_ref().unwrap();
    let rolsuper = String::from_utf8(rolsuper_bytes.clone()).unwrap();
    assert_eq!(rolsuper, "t", "postgres should be superuser");

    println!("✅ Found postgres role with superuser privileges");

    // Test role privilege query (common SQLAlchemy pattern)
    let result = db_handler.query("SELECT rolname FROM pg_roles WHERE rolcanlogin = 't'").await.unwrap();
    assert!(result.rows.len() >= 2, "Should find at least 2 login roles");
    println!("✅ Found {} roles that can login", result.rows.len());

    // Test public role query (common ORM pattern)
    let result = db_handler.query("SELECT rolname, rolsuper FROM pg_roles WHERE rolname = 'public'").await.unwrap();
    assert!(!result.rows.is_empty(), "Should find public role");

    let rolsuper_bytes = result.rows[0][1].as_ref().unwrap();
    let rolsuper = String::from_utf8(rolsuper_bytes.clone()).unwrap();
    assert_eq!(rolsuper, "f", "public should not be superuser");

    println!("✅ Found public role without superuser privileges");

    println!("🎉 pg_roles specific queries test passed!");
}

#[tokio::test]
async fn test_pg_user_specific_queries() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_user_specific.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Test current user query (common Django pattern)
    let result = db_handler.query("SELECT usename, usesuper FROM pg_user WHERE usename = 'pgsqlite_user'").await.unwrap();
    assert!(!result.rows.is_empty(), "Should find pgsqlite_user");

    let usename_bytes = result.rows[0][0].as_ref().unwrap();
    let usename = String::from_utf8(usename_bytes.clone()).unwrap();
    assert_eq!(usename, "pgsqlite_user", "Should find pgsqlite_user");

    println!("✅ Found pgsqlite_user in pg_user");

    // Test user privileges query (common Rails pattern)
    let result = db_handler.query("SELECT usename FROM pg_user WHERE usecreatedb = 't'").await.unwrap();
    assert!(result.rows.len() >= 2, "Should find users with createdb privilege");
    println!("✅ Found {} users that can create databases", result.rows.len());

    // Test superuser query (common SQLAlchemy pattern)
    let result = db_handler.query("SELECT usename FROM pg_user WHERE usesuper = 't'").await.unwrap();
    assert!(result.rows.len() >= 2, "Should find superusers");
    println!("✅ Found {} superusers", result.rows.len());

    println!("🎉 pg_user specific queries test passed!");
}

#[tokio::test]
async fn test_orm_compatibility_patterns() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_orm_compatibility.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Django user management pattern
    let result = db_handler.query(r#"
        SELECT r.rolname, r.rolsuper, r.rolcreatedb, r.rolcanlogin
        FROM pg_roles r
        WHERE r.rolcanlogin = 't'
    "#).await.unwrap();
    assert!(!result.rows.is_empty(), "Django pattern should work");
    println!("✅ Django user management pattern works");

    // SQLAlchemy role-based access control pattern
    let result = db_handler.query(r#"
        SELECT rolname, rolsuper, rolbypassrls
        FROM pg_roles
        WHERE rolname IN ('postgres', 'pgsqlite_user')
    "#).await.unwrap();
    assert_eq!(result.rows.len(), 2, "Should find both users");
    println!("✅ SQLAlchemy role-based access control pattern works");

    // Rails authentication integration pattern
    let result = db_handler.query(r#"
        SELECT u.usename, u.usesuper, u.usecreatedb
        FROM pg_user u
        ORDER BY u.usename
    "#).await.unwrap();
    assert!(result.rows.len() >= 2, "Should find multiple users");
    println!("✅ Rails authentication integration pattern works");

    // Ecto user introspection pattern
    let result = db_handler.query(r#"
        SELECT COUNT(*) as user_count
        FROM pg_user
        WHERE usesuper = 't'
    "#).await.unwrap();
    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = String::from_utf8(count_bytes.clone()).unwrap();
    let count: i32 = count_str.parse().unwrap();
    assert!(count >= 1, "Should find at least one superuser");
    println!("✅ Ecto user introspection pattern works");

    println!("🎉 ORM compatibility patterns test passed!");
}