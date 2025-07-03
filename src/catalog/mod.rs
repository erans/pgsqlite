// Module for system catalog implementation
pub mod query_interceptor;
pub mod pg_class;
pub mod pg_attribute;

pub use query_interceptor::CatalogInterceptor;