# Changelog

All notable changes to pgsqlite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Array Type Support**: Comprehensive PostgreSQL array type implementation
  - Support for all base type arrays (INTEGER[], TEXT[], BOOLEAN[], REAL[], etc.)
  - Multi-dimensional array support (e.g., INTEGER[][])
  - Array literal formats: ARRAY[1,2,3] and '{1,2,3}'
  - JSON-based storage with automatic validation
  - Wire protocol support with proper array type OIDs
  - Migration v8 adds __pgsqlite_array_types table and pg_type enhancements
  - Full integration with CI/CD test suite
- **Batch INSERT Support**: Full support for multi-row INSERT syntax with dramatic performance improvements
  - Fast path optimization achieving up to 112.9x speedup for simple batch INSERTs
  - Prepared statement caching with fingerprinting for repeated batch patterns
  - Enhanced error messages indicating specific row numbers when errors occur
  - Comprehensive test coverage including edge cases and error scenarios
  - Support for datetime value conversion in batch operations
- **Performance Benchmarks**: Added batch INSERT performance benchmarks showing:
  - 10-row batches: 11.5x speedup over single-row INSERTs
  - 100-row batches: 51.3x speedup
  - 1000-row batches: 76.4x speedup
- **JSON Operator Support**: PostgreSQL JSON/JSONB operator translation
  - Implemented -> and ->> operators for JSON field extraction
  - Added #> and #>> operators for JSON path extraction
  - Implemented @> and <@ operators for containment checks
  - Automatic operator translation in query executor pipeline
  - Full test coverage for all JSON operators
- **JSON Functions**: Core PostgreSQL JSON functions implementation
  - json_valid(), json_typeof(), json_array_length() functions
  - jsonb_object_keys(), to_json(), to_jsonb() conversions
  - json_extract_scalar(), jsonb_contains(), jsonb_contained() operations
  - json_array_elements(), json_strip_nulls() utility functions
  - jsonb_set(), json_extract_path(), json_extract_path_text() path operations
- **Row to JSON Conversion**: Complete row_to_json() function implementation
  - RowToJsonTranslator for converting PostgreSQL subquery patterns to json_object() calls
  - Pattern matching for `SELECT row_to_json(t) FROM (SELECT ...) t` syntax
  - Column extraction with support for aliases from SELECT clauses
  - SQLite function registration for simple value conversion cases
  - Integration with both simple and extended query protocols
  - TranslationMetadata support for proper JSON type inference
  - Comprehensive test coverage for all usage scenarios
- **Complete JSON Function Test Coverage**: CI/CD pipeline enhanced with comprehensive JSON testing
  - All JSON functions included in test_queries.sql (json_agg, json_object_agg, row_to_json, json_each, etc.)
  - Fixed row_to_json() subquery alias handling and JSON existence operator compatibility
  - 100% test success rate across all connection modes (TCP+SSL, TCP-only, Unix sockets, file databases)
  - Production-ready validation ensures reliable deployment across all supported configurations

### Changed
- Enhanced InsertTranslator to handle array value conversion from PostgreSQL to JSON format
- Updated simple_query_detector to exclude array patterns from ultra-fast path
- Modified CreateTableTranslator to support array column declarations
- Enhanced InsertTranslator to handle multi-row VALUES clauses efficiently
- Improved error handling to provide more helpful messages for batch operations
- Updated simple query detector to recognize and optimize batch INSERT patterns
- Modified statement pool to support batch INSERT fingerprinting for better caching
- **Code Quality Improvements**: Fixed major clippy warnings for better performance and maintainability

### Fixed
- **Array Function Type Inference**: Fixed incorrect type detection for array operations
  - ArithmeticAnalyzer regex pattern was too permissive, incorrectly matching array expressions
  - Array functions now correctly return TEXT type instead of array OIDs (since data is stored as JSON)
  - Array concatenation operator (||) properly detected and typed
  - All 4 array operator integration tests now pass
  - Fixed array function parameter handling to accept non-string types (INTEGER, REAL, NULL, etc.)
- **Arithmetic Expression Type Inference**: Fixed complex nested parentheses expressions
  - Enhanced ArithmeticAnalyzer to properly handle expressions like ((a + b) * c) / d
  - Improved regex pattern to match complex arithmetic including nested parentheses
  - Fixed type inference for arithmetic operations to correctly return float types
  - Resolved test_nested_parentheses failure in arithmetic_complex_test.rs
  - Boxed large ErrorResponse enum variant to reduce memory usage
  - Fixed inconsistent digit grouping in datetime constants
  - Simplified complex type definitions with type aliases
  - Updated format strings to use inline syntax

### Fixed
- Fixed JSON validation constraint to handle NULL arrays properly (NULL check before json_valid())
- Fixed migration execution order in benchmark tests
- Fixed unused variable warnings in batch INSERT fingerprinting
- Fixed batch INSERT handling of datetime functions (CURRENT_DATE, CURRENT_TIME, NOW(), etc.)
- Fixed NOW() function translation to CURRENT_TIMESTAMP for SQLite compatibility
- Fixed INSERT statement parsing to properly handle trailing semicolons
- **Array Type Wire Protocol**: Fixed \"cannot convert between Rust type String and Postgres type _text\" error
  - Array functions now properly convert JSON storage format to PostgreSQL array format
  - Added array data conversion in query executor for proper client deserialization
  - Text protocol correctly converts JSON arrays to PostgreSQL format (e.g., [\"a\",\"b\"] → {a,b})

## [0.0.5] - Previous Release

[Previous changelog entries...]