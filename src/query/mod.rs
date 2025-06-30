// Module for query execution
pub mod executor;
pub mod extended;
mod extended_helpers;
pub mod fast_path;

pub use executor::QueryExecutor;
pub use extended::ExtendedQueryHandler;
pub use fast_path::{can_use_fast_path, execute_fast_path, query_fast_path};