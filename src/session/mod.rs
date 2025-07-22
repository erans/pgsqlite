// Module for session management
pub mod state;
pub mod pool;
pub mod db_handler;
pub mod read_only_handler;
pub mod query_router;
pub mod portal_manager;

pub use state::{SessionState, PreparedStatement, Portal, GLOBAL_QUERY_CACHE};
pub use pool::{SqlitePool, PooledConnection};
pub use db_handler::{DbHandler, DbResponse};
pub use read_only_handler::{ReadOnlyDbHandler, ReadOnlyError};
pub use pool::PoolStats;
pub use query_router::{QueryRouter, QueryRoute, QueryType, RouterError, RouterStats};
pub use portal_manager::{PortalManager, PortalExecutor, ManagedPortal, PortalExecutionState, CachedQueryResult};