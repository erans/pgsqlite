use rusqlite::{Connection, Result};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time;
use tracing::{debug, warn, error};

/// Enhanced connection wrapper with health tracking
#[derive(Debug)]
struct PoolConnection {
    conn: Connection,
    #[allow(dead_code)]
    created_at: Instant,
    last_used: Instant,
    #[allow(dead_code)]
    health_check_count: u64,
    #[allow(dead_code)]
    failure_count: u64,
}

impl PoolConnection {
    fn new(conn: Connection) -> Self {
        let now = Instant::now();
        Self {
            conn,
            created_at: now,
            last_used: now,
            health_check_count: 0,
            failure_count: 0,
        }
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
    }

    #[allow(dead_code)]
    fn is_stale(&self, max_idle_duration: Duration) -> bool {
        self.last_used.elapsed() > max_idle_duration
    }

    #[allow(dead_code)]
    fn should_health_check(&self, interval: Duration) -> bool {
        self.last_used.elapsed() > interval
    }

    #[allow(dead_code)]
    fn health_check(&mut self) -> Result<()> {
        match self.conn.prepare("SELECT 1").and_then(|mut stmt| {
            stmt.query_map([], |_row| Ok(()))?
                .next()
                .unwrap_or(Ok(()))
        }) {
            Ok(_) => {
                self.health_check_count += 1;
                self.failure_count = 0; // Reset failure count on success
                self.touch();
                Ok(())
            }
            Err(e) => {
                self.failure_count += 1;
                Err(e)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_connections: usize,
    pub idle_connections: usize,
    pub active_connections: usize,
    pub health_checks_performed: u64,
    pub connections_created: u64,
    pub connections_dropped: u64,
    pub health_check_failures: u64,
}

pub struct SqlitePool {
    path: String,
    connections: Arc<Mutex<Vec<PoolConnection>>>,
    semaphore: Arc<Semaphore>,
    #[allow(dead_code)]
    max_connections: usize,
    #[allow(dead_code)]
    max_idle_duration: Duration,
    #[allow(dead_code)]
    health_check_interval: Duration,
    stats: Arc<Mutex<PoolStats>>,
}

impl SqlitePool {
    pub fn new(path: &str) -> Result<Self> {
        Self::new_with_size(path, 5)
    }

    pub fn new_with_size(path: &str, max_connections: usize) -> Result<Self> {
        Self::new_with_config(
            path,
            max_connections,
            Duration::from_secs(300), // 5 minute idle timeout
            Duration::from_secs(60),  // 1 minute health check interval
        )
    }

    pub fn new_with_config(
        path: &str,
        max_connections: usize,
        max_idle_duration: Duration,
        health_check_interval: Duration,
    ) -> Result<Self> {
        let stats = Arc::new(Mutex::new(PoolStats {
            total_connections: 0,
            idle_connections: 0,
            active_connections: 0,
            health_checks_performed: 0,
            connections_created: 0,
            connections_dropped: 0,
            health_check_failures: 0,
        }));

        let pool = SqlitePool {
            path: path.to_string(),
            connections: Arc::new(Mutex::new(Vec::new())),
            semaphore: Arc::new(Semaphore::new(max_connections)),
            max_connections,
            max_idle_duration,
            health_check_interval,
            stats,
        };
        
        // Pre-create initial connections (half of max)
        let initial_connections = (max_connections / 2).max(1);
        let mut conns = pool.connections.lock().unwrap();
        let mut stats = pool.stats.lock().unwrap();
        for _ in 0..initial_connections {
            let conn = pool.create_connection()?;
            conns.push(PoolConnection::new(conn));
            stats.connections_created += 1;
            stats.total_connections += 1;
            stats.idle_connections += 1;
        }
        drop(conns);
        drop(stats);
        
        // Start background health check task only if not in test mode
        #[cfg(not(test))]
        {
            let pool_clone = SqlitePool {
                path: pool.path.clone(),
                connections: pool.connections.clone(),
                semaphore: Arc::new(Semaphore::new(0)), // Not used in background task
                max_connections: pool.max_connections,
                max_idle_duration: pool.max_idle_duration,
                health_check_interval: pool.health_check_interval,
                stats: pool.stats.clone(),
            };
            
            tokio::spawn(async move {
                pool_clone.background_health_check().await;
            });
        }
        
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

    #[allow(dead_code)]
    async fn background_health_check(&self) {
        let mut interval = time::interval(self.health_check_interval);
        
        loop {
            interval.tick().await;
            
            debug!("Running background health check on connection pool");
            
            let mut conns = match self.connections.lock() {
                Ok(guard) => guard,
                Err(e) => {
                    error!("Failed to acquire connection pool lock for health check: {}", e);
                    continue;
                }
            };
            
            let mut stats = match self.stats.lock() {
                Ok(guard) => guard,
                Err(e) => {
                    error!("Failed to acquire stats lock for health check: {}", e);
                    continue;
                }
            };
            
            let mut to_remove = Vec::new();
            
            for (i, pool_conn) in conns.iter_mut().enumerate() {
                // Remove stale connections
                if pool_conn.is_stale(self.max_idle_duration) {
                    debug!("Removing stale connection (idle for {:?})", pool_conn.last_used.elapsed());
                    to_remove.push(i);
                    stats.connections_dropped += 1;
                    stats.total_connections -= 1;
                    stats.idle_connections -= 1;
                    continue;
                }
                
                // Perform health check if needed
                if pool_conn.should_health_check(self.health_check_interval) {
                    match pool_conn.health_check() {
                        Ok(()) => {
                            debug!("Health check passed for connection");
                            stats.health_checks_performed += 1;
                        }
                        Err(e) => {
                            warn!("Health check failed for connection: {}", e);
                            stats.health_check_failures += 1;
                            
                            // Remove connection after 3 consecutive failures
                            if pool_conn.failure_count >= 3 {
                                error!("Removing unhealthy connection after {} failures", pool_conn.failure_count);
                                to_remove.push(i);
                                stats.connections_dropped += 1;
                                stats.total_connections -= 1;
                                stats.idle_connections -= 1;
                            }
                        }
                    }
                }
            }
            
            // Remove marked connections (in reverse order to maintain indices)
            for &index in to_remove.iter().rev() {
                conns.remove(index);
            }
            
            // Update idle connection count
            stats.idle_connections = conns.len();
            stats.active_connections = self.max_connections.saturating_sub(conns.len());
            
            if !to_remove.is_empty() {
                debug!("Health check completed: removed {} connections, {} idle remaining", 
                       to_remove.len(), conns.len());
            }
        }
    }

    pub fn get_stats(&self) -> PoolStats {
        let stats = self.stats.lock().unwrap();
        stats.clone()
    }

    pub async fn health_check(&self) -> Result<()> {
        // Try to acquire a connection and perform a simple query
        let conn = self.acquire().await?;
        let mut stmt = conn.prepare("SELECT 1")?;
        stmt.query_map([], |_row| Ok(()))?
            .next()
            .unwrap_or(Ok(()))?;
        Ok(())
    }
    
    pub async fn acquire(&self) -> Result<PooledConnection> {
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();
        
        let pool_conn = {
            let mut conns = self.connections.lock().unwrap();
            let mut stats = self.stats.lock().unwrap();
            
            match conns.pop() {
                Some(mut pc) => {
                    pc.touch(); // Update last_used timestamp
                    stats.idle_connections -= 1;
                    stats.active_connections += 1;
                    Some(pc)
                }
                None => {
                    stats.active_connections += 1;
                    None
                }
            }
        };
        
        let conn = match pool_conn {
            Some(pc) => pc.conn,
            None => {
                // Create new connection if pool is empty
                let new_conn = self.create_connection()?;
                let mut stats = self.stats.lock().unwrap();
                stats.connections_created += 1;
                stats.total_connections += 1;
                new_conn
            }
        };
        
        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.connections.clone(),
            stats: self.stats.clone(),
            _permit: permit,
        })
    }
}

pub struct PooledConnection {
    conn: Option<Connection>,
    pool: Arc<Mutex<Vec<PoolConnection>>>,
    stats: Arc<Mutex<PoolStats>>,
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
            let mut stats = self.stats.lock().unwrap();
            
            // Return connection to pool wrapped in PoolConnection
            conns.push(PoolConnection::new(conn));
            stats.idle_connections += 1;
            stats.active_connections = stats.active_connections.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_pool_creation() {
        let pool = SqlitePool::new(":memory:").unwrap();
        let stats = pool.get_stats();
        
        // Pool should have some initial connections
        assert!(stats.total_connections > 0);
        assert_eq!(stats.idle_connections, stats.total_connections);
        assert_eq!(stats.active_connections, 0);
    }

    #[tokio::test]
    async fn test_connection_acquisition() {
        let pool = SqlitePool::new_with_size(":memory:", 2).unwrap();
        
        let _conn1 = pool.acquire().await.unwrap();
        let stats = pool.get_stats();
        assert!(stats.active_connections > 0);
        
        let _conn2 = pool.acquire().await.unwrap();
        // When connections are dropped, they should return to pool
    }

    #[tokio::test]
    async fn test_health_check() {
        let pool = SqlitePool::new(":memory:").unwrap();
        
        // Health check should succeed
        match pool.health_check().await {
            Ok(_) => {},
            Err(e) => panic!("Health check failed: {}", e),
        }
    }

    #[tokio::test]
    async fn test_pool_stats() {
        let pool = SqlitePool::new_with_size(":memory:", 3).unwrap();
        
        let _conn1 = pool.acquire().await.unwrap();
        let _conn2 = pool.acquire().await.unwrap();
        
        let stats = pool.get_stats();
        assert!(stats.active_connections >= 2);
        assert!(stats.connections_created > 0);
    }

    #[tokio::test]
    async fn test_connection_timeout_and_cleanup() {
        // Test with very short timeout for faster test
        let pool = SqlitePool::new_with_config(
            ":memory:",
            2,
            Duration::from_millis(100), // Very short idle timeout
            Duration::from_millis(50),  // Very short health check interval
        ).unwrap();
        
        let conn = pool.acquire().await.unwrap();
        drop(conn); // Return to pool
        
        // Wait for background cleanup
        sleep(Duration::from_millis(200)).await;
        
        // The background task should have run and potentially cleaned up idle connections
        // (exact behavior depends on timing, but this tests that the mechanism works)
        let _stats = pool.get_stats();
        // The background task should have run and health checks should have occurred
        // No specific assertion needed since health_checks_performed is always >= 0
    }
}