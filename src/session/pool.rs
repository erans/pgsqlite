use rusqlite::{Connection, Result};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

pub struct SqlitePool {
    path: String,
    connections: Arc<Mutex<Vec<Connection>>>,
    semaphore: Arc<Semaphore>,
    _max_connections: usize,
}

impl SqlitePool {
    pub fn new(path: &str) -> Result<Self> {
        Self::new_with_size(path, 5)
    }

    pub fn new_with_size(path: &str, max_connections: usize) -> Result<Self> {
        let pool = SqlitePool {
            path: path.to_string(),
            connections: Arc::new(Mutex::new(Vec::new())),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            _max_connections: max_connections,
        };
        
        // Pre-create initial connections (half of max)
        let initial_connections = (max_connections / 2).max(1);
        let mut conns = pool.connections.lock().unwrap();
        for _ in 0..initial_connections {
            let conn = pool.create_connection()?;
            conns.push(conn);
        }
        drop(conns);
        
        Ok(pool)
    }

    fn create_connection(&self) -> Result<Connection> {
        let conn = if self.path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(&self.path)?
        };
        
        // Set pragmas for better performance
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA cache_size=-64000;
             PRAGMA temp_store=MEMORY;
             PRAGMA mmap_size=268435456;"
        )?;
        
        Ok(conn)
    }
    
    pub async fn acquire(&self) -> Result<PooledConnection> {
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();
        
        let conn = {
            let mut conns = self.connections.lock().unwrap();
            conns.pop()
        };
        
        let conn = match conn {
            Some(c) => c,
            None => {
                // Create new connection if pool is empty
                self.create_connection()?
            }
        };
        
        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.connections.clone(),
            _permit: permit,
        })
    }
}

pub struct PooledConnection {
    conn: Option<Connection>,
    pool: Arc<Mutex<Vec<Connection>>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl std::ops::Deref for PooledConnection {
    type Target = Connection;
    
    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().unwrap()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let mut conns = self.pool.lock().unwrap();
            conns.push(conn);
        }
    }
}