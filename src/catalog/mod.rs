// Module for system catalog implementation
pub mod query_interceptor;
pub mod pg_class;
pub mod pg_attribute;
pub mod pg_enum;
pub mod system_functions;
pub mod where_evaluator;
pub mod constraint_populator;

pub use query_interceptor::CatalogInterceptor;