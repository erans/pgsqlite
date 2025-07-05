// Module for query execution
pub mod executor;
pub mod extended;
mod extended_helpers;
pub mod fast_path;
pub mod extended_fast_path;
pub mod query_type_detection;
pub mod comment_stripper;
pub mod lazy_processor;

pub use executor::QueryExecutor;
pub use extended::ExtendedQueryHandler;
pub use fast_path::{
    can_use_fast_path, execute_fast_path, query_fast_path,
    can_use_fast_path_enhanced, execute_fast_path_enhanced, query_fast_path_enhanced,
    execute_fast_path_enhanced_with_params, query_fast_path_enhanced_with_params,
    clear_decimal_cache, FastPathQuery, FastPathOperation, WhereClause
};
pub use query_type_detection::{QueryTypeDetector, QueryType};
pub use comment_stripper::strip_sql_comments;
pub use lazy_processor::LazyQueryProcessor;