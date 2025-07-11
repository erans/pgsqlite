# Changelog

All notable changes to pgsqlite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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

### Changed
- Enhanced InsertTranslator to handle multi-row VALUES clauses efficiently
- Improved error handling to provide more helpful messages for batch operations
- Updated simple query detector to recognize and optimize batch INSERT patterns
- Modified statement pool to support batch INSERT fingerprinting for better caching

### Fixed
- Fixed migration execution order in benchmark tests
- Fixed unused variable warnings in batch INSERT fingerprinting

## [0.0.5] - Previous Release

[Previous changelog entries...]