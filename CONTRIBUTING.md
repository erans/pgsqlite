# Contributing to pgsqlite

Thank you for your interest in contributing to pgsqlite! This guide will help you get started.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Create a new branch for your feature or bug fix
4. Make your changes
5. Run tests to ensure everything works
6. Submit a pull request

## Development Setup

```bash
# Clone the repository
git clone https://github.com/your-username/pgsqlite
cd pgsqlite

# Build the project
cargo build

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run
```

## Code Style

- Follow Rust conventions and idioms
- Use `cargo fmt` to format your code
- Run `cargo clippy` to catch common issues
- Keep code concise and well-documented
- Avoid unnecessary comments in code

## Testing

### Running Tests

```bash
# Run all unit tests
cargo test

# Run integration tests
./run_ssl_tests.sh

# Run specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture
```

### Writing Tests

- Write tests for all new functionality
- Test edge cases and error conditions
- Ensure tests actually verify behavior
- Use descriptive test names

## Reporting Issues

When reporting issues, please include:

1. **SQL statements** that reproduce the issue
2. **Expected behavior** - what should happen
3. **Actual behavior** - what actually happened
4. **Error messages** if any
5. **Environment details** (OS, Rust version, etc.)

### Good Issue Example

```
Title: INSERT with RETURNING clause fails for SERIAL columns

PostgreSQL SQL:
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255)
);
INSERT INTO users (email) VALUES ('test@example.com') RETURNING id;

Expected: Should return the generated ID
Actual: Error: "RETURNING clause not supported"

Environment: Ubuntu 22.04, Rust 1.75, pgsqlite v0.1.0
```

## Submitting Pull Requests

### Before Submitting

Follow this complete checklist to ensure code quality:

- [ ] Run `cargo build` - no compilation errors
- [ ] Run `cargo test` - all tests pass
- [ ] Run `cargo clippy` - no warnings (use `cargo clippy --fix` for automatic fixes)
- [ ] Run `cargo fmt` - code is formatted
- [ ] Update documentation if needed
- [ ] Add tests for new functionality
- [ ] Update TODO.md if applicable (mark completed tasks, add new discoveries)

**Pre-commit checklist**: Run ALL of these before committing:
1. `cargo check` - No errors or warnings
2. `cargo clippy` - Review and fix warnings where reasonable
3. `cargo build` - Successful build
4. `cargo test` - All tests pass

### PR Guidelines

1. **Clear Description**: Explain what and why
2. **Small Changes**: Keep PRs focused
3. **Test Coverage**: Include tests
4. **Documentation**: Update if needed
5. **Clean History**: Squash commits if messy

### PR Title Format

- `feat: Add support for ARRAY types`
- `fix: Handle NULL values in DECIMAL columns`
- `perf: Optimize query cache lookup`
- `docs: Update SSL configuration guide`

## Working with TODO.md

When working on pgsqlite:

1. Check `TODO.md` for planned work
2. Mark items as `[x]` when completed
3. Add new items discovered during development
4. Document partial progress with notes

## Architecture Guidelines

### Type System

- Never use column names to infer types
- Types come from:
  - PostgreSQL type declarations
  - SQLite schema (PRAGMA table_info)
  - Explicit casts in queries
  - Value-based inference as last resort

### Performance

- Cache aggressively but invalidate correctly
- Prefer batch operations
- Minimize allocations in hot paths
- Profile before optimizing

#### Performance Context
pgsqlite adds ~360x overhead vs pure SQLite (~80ms vs 0.22ms per operation) in exchange for full PostgreSQL compatibility. This overhead is acceptable for most web applications where database operations represent 10-20% of total request time.

#### Driver Performance Recommendations
Based on comprehensive benchmarking (100 operations each):

**For Read-Heavy Workloads** - Use psycopg3-binary:
- SELECT: 0.452ms (best read performance)
- Best for applications with frequent SELECT queries
- 21.8x faster SELECT than psycopg2

**For Write-Heavy Workloads** - Use psycopg2:
- INSERT: 0.214ms (3.6x faster than psycopg3)
- UPDATE/DELETE: ~0.06-0.09ms (2x faster than psycopg3)
- Best for data ingestion and batch updates

**For Balanced Workloads** - Use psycopg3-text:
- Reasonable performance across all operations
- Good middle ground for mixed usage patterns

#### Optimization Guidelines
- **Batch operations**: 10-76x speedup for bulk INSERT operations
  - 10-row batches: ~11x faster than single-row INSERTs
  - 100-row batches: ~51x faster
  - 1000-row batches: ~76x faster
- **Connection architecture**: Connection-per-session provides excellent isolation
- **Ultra-fast path**: Optimized execution for simple SELECT queries
- **Type efficiency**: Use native PostgreSQL types to reduce conversion overhead
- **Protocol choice**: Consider workload pattern when selecting driver

### Error Handling

- Return PostgreSQL-compatible error codes
- Provide helpful error messages
- Never panic in production code
- Handle all Result types explicitly

## Areas for Contribution

### Good First Issues

- Improve error messages
- Add more SQL function translations
- Enhance documentation
- Add more integration tests

### Advanced Contributions

- New PostgreSQL type support
- Performance optimizations
- Protocol enhancements
- System catalog emulation

## Questions?

- Open an issue for discussion
- Check existing issues and PRs
- Read the architecture documentation
- Ask in pull request comments

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.