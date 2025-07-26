// Module for query execution
pub mod executor;
pub mod extended;
mod extended_helpers;
pub mod fast_path;
pub mod extended_fast_path;
pub mod query_type_detection;
pub mod comment_stripper;
pub mod lazy_processor;
pub mod set_handler;
pub mod simple_query_detector;
pub mod parameter_parser;
pub mod pattern_optimizer;
pub mod query_handler;
pub mod join_type_inference;

pub use executor::QueryExecutor;
pub use query_handler::{QueryHandler, QueryHandlerImpl};
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
pub use set_handler::SetHandler;
pub use parameter_parser::ParameterParser;
pub use pattern_optimizer::{QueryPatternOptimizer, QueryPattern, OptimizationHints, QueryComplexity, ResultSize};