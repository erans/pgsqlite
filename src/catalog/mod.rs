// Module for system catalog implementation
pub mod query_interceptor;
pub mod pg_class;
pub mod pg_attribute;
pub mod pg_constraint;
pub mod pg_depend;
pub mod pg_enum;
pub mod pg_proc;
pub mod pg_description;
pub mod pg_roles;
pub mod pg_user;
pub mod pg_stats;
pub mod pg_sequence;
pub mod pg_trigger;
pub mod pg_settings;
pub mod system_functions;
pub mod where_evaluator;
pub mod constraint_populator;

pub use query_interceptor::CatalogInterceptor;