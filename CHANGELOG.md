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

### Changed
- Enhanced InsertTranslator to handle array value conversion from PostgreSQL to JSON format
- Updated simple_query_detector to exclude array patterns from ultra-fast path
- Modified CreateTableTranslator to support array column declarations
- Enhanced InsertTranslator to handle multi-row VALUES clauses efficiently
- Improved error handling to provide more helpful messages for batch operations
- Updated simple query detector to recognize and optimize batch INSERT patterns
- Modified statement pool to support batch INSERT fingerprinting for better caching

### Fixed
- Fixed JSON validation constraint to handle NULL arrays properly (NULL check before json_valid())
- Fixed migration execution order in benchmark tests
- Fixed unused variable warnings in batch INSERT fingerprinting
- Fixed batch INSERT handling of datetime functions (CURRENT_DATE, CURRENT_TIME, NOW(), etc.)
- Fixed NOW() function translation to CURRENT_TIMESTAMP for SQLite compatibility
- Fixed INSERT statement parsing to properly handle trailing semicolons

## [0.0.5] - Previous Release

[Previous changelog entries...]