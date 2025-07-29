#!/usr/bin/env rust-script

//! Pure rusqlite test to mimic the working Python sqlite3 test
//! This will determine if the issue is in rusqlite or pgsqlite's layers

use rusqlite::{Connection, OpenFlags};
use std::fs;

fn main() -> rusqlite::Result<()> {
    let db_path = "/tmp/rusqlite_visibility_test.db";
    
    // Clean up any existing files
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path));
    let _ = fs::remove_file(format!("{}-shm", db_path));
    
    println!("Testing rusqlite visibility with database: {}", db_path);
    
    // Connection 1: Create, insert, update, commit (mimic Python test exactly)
    {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE 
            | OpenFlags::SQLITE_OPEN_CREATE 
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX;
            
        let conn1 = Connection::open_with_flags(db_path, flags)?;
        
        // Apply same pragmas as our working Python test
        conn1.execute("PRAGMA journal_mode = WAL", [])?;
        conn1.execute("PRAGMA synchronous = NORMAL", [])?;
        
        println!("✅ Connection 1: Applied pragmas");
        
        // Create table (simplified like our Python test)
        conn1.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT UNIQUE,
                full_name TEXT
            )", [])?;
        
        // Insert initial data
        conn1.execute(
            "INSERT INTO users (username, full_name) VALUES ('test_user', 'Original Name')", 
            [])?;
        
        println!("✅ Connection 1: Inserted user");
        
        // Update the name
        conn1.execute(
            "UPDATE users SET full_name = 'Updated Name' WHERE username = 'test_user'", 
            [])?;
        
        println!("✅ Connection 1: Updated name to 'Updated Name'");
        
        // Verify connection 1 sees the update
        let result: String = conn1.query_row(
            "SELECT full_name FROM users WHERE username = 'test_user'",
            [],
            |row| row.get(0)
        )?;
        
        println!("✅ Connection 1 sees: '{}'", result);
        
        // Explicitly drop connection 1 (like Python test closes it)
        drop(conn1);
        println!("✅ Connection 1 closed");
    }
    
    // Connection 2: New connection created AFTER commit and close (like SQLAlchemy separate engine)
    {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE 
            | OpenFlags::SQLITE_OPEN_CREATE 
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX;
            
        let conn2 = Connection::open_with_flags(db_path, flags)?;
        
        // Apply same pragmas
        conn2.execute("PRAGMA journal_mode = WAL", [])?;
        conn2.execute("PRAGMA synchronous = NORMAL", [])?;
        
        println!("✅ Connection 2: Applied pragmas");
        
        // Check what connection 2 sees
        let result: String = conn2.query_row(
            "SELECT full_name FROM users WHERE username = 'test_user'",
            [],
            |row| row.get(0)
        )?;
        
        println!("📍 Connection 2 sees: '{}'", result);
        
        if result == "Updated Name" {
            println!("✅ SUCCESS: rusqlite works correctly - new connection sees committed update");
        } else {
            println!("❌ FAILURE: rusqlite has same issue - new connection sees: '{}'", result);
        }
        
        drop(conn2);
    }
    
    // Cleanup
    let _ = fs::remove_file(db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path));
    let _ = fs::remove_file(format!("{}-shm", db_path));
    
    Ok(())
}