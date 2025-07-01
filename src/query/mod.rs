// Module for query execution
pub mod executor;
pub mod extended;
mod extended_helpers;
pub mod fast_path;

#[cfg(feature = "zero-copy-protocol")]
pub mod zero_copy_executor;

#[cfg(feature = "zero-copy-protocol")]
pub mod executor_zero_copy;

#[cfg(feature = "zero-copy-protocol")]
pub mod executor_batch;

pub use executor::QueryExecutor;
pub use extended::ExtendedQueryHandler;
pub use fast_path::{
    can_use_fast_path, execute_fast_path, query_fast_path,
    can_use_fast_path_enhanced, execute_fast_path_enhanced, query_fast_path_enhanced,
    execute_fast_path_enhanced_with_params, query_fast_path_enhanced_with_params,
    clear_decimal_cache, FastPathQuery, FastPathOperation, WhereClause
};

#[cfg(feature = "zero-copy-protocol")]
pub use zero_copy_executor::ZeroCopyExecutor;

#[cfg(feature = "zero-copy-protocol")]
pub use executor_zero_copy::{QueryExecutorZeroCopy, should_use_zero_copy};

#[cfg(feature = "zero-copy-protocol")]
pub use executor_batch::{QueryExecutorBatch, BatchConfig};