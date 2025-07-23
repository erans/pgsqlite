# Portal Management Benchmarks

This document describes the comprehensive benchmark suite for pgsqlite's portal management system, including how to run the benchmarks and interpret the results.

## Overview

The portal management benchmark suite demonstrates the performance benefits of pgsqlite's Enhanced Extended Query Protocol implementation across real-world usage scenarios. The benchmarks focus on:

- **Memory Efficiency**: Reduced memory usage with large result sets
- **Concurrent Operations**: Multiple portals operating simultaneously
- **Partial Result Fetching**: Incremental data retrieval with max_rows
- **Resource Management**: Portal lifecycle and cleanup performance
- **Real-world Scenarios**: Practical use cases where portals provide significant benefits

## Benchmark Files

### 1. `benchmark_portal_management.rs` - Core Performance Benchmarks
Comprehensive technical benchmarks focusing on portal management internals:

- **Memory Efficiency**: Compares full-fetch vs partial-fetch for 50K+ records
- **Partial Result Fetching**: Tests different chunk sizes and pagination strategies  
- **Concurrent Portal Operations**: Multiple portals with 1-20 concurrent operations
- **Protocol Comparison**: Extended Query Protocol vs Simple Query Protocol
- **Resource Management**: Portal creation, cleanup, and resource limits
- **Memory Stress Test**: 100K+ record datasets with various chunk sizes

### 2. `benchmark_portal_realistic.rs` - Real-world Scenarios
Business-focused benchmarks showing practical portal benefits:

- **Data Export Scenario**: CSV export with memory constraints
- **Pagination Scenario**: Web API serving paginated results  
- **Report Generation**: Business analytics with large datasets
- **ETL Streaming**: Data transformation with limited memory
- **Multi-tenant Access**: Concurrent tenant applications

## Running the Benchmarks

### Prerequisites
1. **Rust Environment**: Ensure you have Rust and Cargo installed
2. **Build pgsqlite**: `cargo build --release`
3. **Dependencies**: The benchmarks will automatically start/stop pgsqlite server instances

### Basic Benchmark Execution

```bash
# Run core portal management benchmarks
cargo test benchmark_portal_management_comprehensive -- --ignored --nocapture

# Run realistic scenario benchmarks  
cargo test benchmark_realistic_portal_scenarios -- --ignored --nocapture

# Run memory stress tests
cargo test benchmark_portal_memory_stress -- --ignored --nocapture
```

### Running All Portal Benchmarks
```bash
# Run all portal benchmarks with detailed output
cargo test benchmark_portal -- --ignored --nocapture
```

### Individual Test Execution
```bash
# Run specific benchmark functions
cargo test benchmark_memory_efficiency -- --ignored --nocapture
cargo test benchmark_data_export_scenario -- --ignored --nocapture
cargo test benchmark_multitenant_scenario -- --ignored --nocapture
```

## Benchmark Results Interpretation

### Memory Efficiency Metrics

**Key Indicators:**
- **Memory Reduction**: Percentage reduction in peak memory usage
- **Time per Row**: Processing efficiency per record
- **Chunk Processing Time**: Performance of incremental fetching

**Example Output:**
```
📈 Memory Efficiency Results:
  💾 Memory reduction: 98.0% (from ~10.00MB to ~0.20MB peak)
  ⚡ Time per row - Full: 2.35μs, Portal: 1.87μs
  🚀 Portal approach is 1.3x faster per row
```

**What This Means:**
- Portal management reduces memory usage by 98% (critical for large datasets)
- Processing is actually faster per row due to better cache utilization
- System can handle much larger datasets without memory constraints

### Concurrent Operations Performance

**Key Indicators:**
- **Concurrency Speedup**: How much faster concurrent operations are vs sequential
- **Portal Efficiency**: Queries per second across multiple portals
- **Resource Utilization**: System efficiency under load

**Example Output:**
```
📈 Concurrency Analysis:
  🚀 Concurrency speedup: 4.2x
  🎯 Throughput: 2,340 queries/sec, 45,600 rows/sec
  🎪 Portal efficiency: 468 concurrent queries/sec across 5 tenants
```

**What This Means:**
- 4.2x speedup indicates excellent concurrent portal management
- High throughput shows system can handle multiple simultaneous users
- Portal isolation prevents interference between operations

### Real-world Scenario Benefits

**Data Export Scenario:**
- **Memory Impact**: Shows dramatic memory reduction (99%+) for large exports
- **Streaming Benefits**: Continuous processing without memory accumulation
- **Processing Time**: May be slightly slower but enables processing of unlimited dataset sizes

**Pagination Scenario:**
- **Query Efficiency**: Prepared statements vs dynamic queries
- **API Response Time**: Average response time per page
- **Caching Benefits**: Statement preparation overhead amortized across requests

**ETL Processing:**
- **Throughput**: Records processed per second
- **Memory Stability**: Constant memory usage regardless of dataset size
- **Resource Predictability**: Consistent performance characteristics

## Performance Baselines and Targets

### Expected Performance Characteristics

| Scenario | Memory Reduction | Performance Impact | Concurrency Gain |
|----------|------------------|-------------------|-------------------|
| Large Result Sets (50K+ rows) | 95-99% | 0-20% slower | N/A |
| Pagination (20 rows/page) | 90%+ | 10-30% faster | 2-5x |
| Concurrent Operations (5+ portals) | 80%+ | Same or better | 3-10x |
| ETL Streaming | 95-99% | 0-15% slower | 2-4x |

### Performance Regression Detection

**Red Flags:**
- Memory reduction < 90% for large datasets
- Portal operations > 2x slower than simple queries for small datasets
- Concurrency speedup < 2x with 5+ concurrent portals
- Resource cleanup taking > 1ms per portal

**Acceptable Trade-offs:**
- 10-20% performance overhead for 95%+ memory reduction
- Slightly slower individual operations for dramatically better concurrency
- Small initialization overhead for long-term resource efficiency

## Troubleshooting Benchmark Issues

### Common Problems

**Server Connection Issues:**
```bash
# Check if pgsqlite is already running
ps aux | grep pgsqlite
killall pgsqlite  # Stop any running instances

# Check port availability
netstat -tlnp | grep 5433
```

**Memory-related Failures:**
- Reduce dataset sizes in benchmark setup functions
- Increase system memory limits if available
- Run benchmarks individually rather than as a suite

**Timing Inconsistencies:**
- Run benchmarks multiple times for average results
- Ensure system is not under heavy load during benchmarking
- Consider running on dedicated test hardware

### Customizing Benchmarks

**Adjusting Dataset Sizes:**
```rust
// In setup_large_dataset() function
let total_records = 10_000;  // Reduce from 50_000 for faster testing

// In setup_stress_dataset() function  
let total_records = 20_000;  // Reduce from 100_000 for memory-constrained systems
```

**Modifying Chunk Sizes:**
```rust
let chunk_sizes = vec![100, 500, 1000]; // Test different portal max_rows values
```

**Changing Concurrency Levels:**
```rust
let portal_counts = vec![1, 2, 5];  // Test fewer concurrent portals
```

## Integration with CI/CD

### Automated Performance Testing

```yaml
# Example GitHub Actions workflow
- name: Portal Performance Benchmarks
  run: |
    cargo test benchmark_portal_management_comprehensive -- --ignored --nocapture > portal_results.txt
    # Parse results and fail if performance regression detected
```

### Performance Monitoring
- Track benchmark results over time to detect performance regressions
- Set up alerts for significant performance degradation
- Include portal benchmark results in release notes

## Benchmark Evolution

### Adding New Scenarios
1. Identify real-world use cases that benefit from portal management
2. Create isolated test scenarios with measurable metrics
3. Include both positive and negative test cases
4. Document expected performance characteristics

### Improving Existing Benchmarks
- Add more realistic data patterns
- Include error handling and edge cases
- Expand concurrency testing scenarios  
- Add network latency simulation for distributed scenarios

The portal management benchmark suite provides comprehensive validation that pgsqlite's Enhanced Extended Query Protocol delivers significant benefits for memory efficiency, concurrent operations, and real-world application scenarios while maintaining excellent performance characteristics.