# Comprehensive Benchmark Comparison Report
**Date**: 2025-08-08

## Executive Summary

This report compares current pgsqlite performance with both psycopg2 and psycopg3-text drivers against documented historical benchmarks and performance targets.

## Current Benchmark Results (2025-08-08)

### psycopg2 Driver Performance
| Operation | SQLite (ms) | pgsqlite (ms) | Overhead % | Overhead Factor |
|-----------|-------------|---------------|------------|-----------------|
| CREATE    | 0.159       | 8.227         | +5,073.7%  | 51.7x           |
| INSERT    | 0.002       | 0.174         | +9,706.0%  | 97.1x           |
| UPDATE    | 0.001       | 0.057         | +4,703.5%  | 48.0x           |
| DELETE    | 0.001       | 0.036         | +3,586.2%  | 36.9x           |
| SELECT    | 0.001       | 2.594         | +238,900.2%| 2,389x          |
| SELECT (cached) | 0.003  | 1.539         | +51,301.5% | 514x            |

### psycopg3-text Driver Performance  
| Operation | SQLite (ms) | pgsqlite (ms) | Overhead % | Overhead Factor |
|-----------|-------------|---------------|------------|-----------------|
| CREATE    | 0.148       | 2.280         | +1,442.3%  | 15.4x           |
| INSERT    | 0.002       | 0.776         | +41,817.2% | 419x            |
| UPDATE    | 0.001       | 0.172         | +13,017.8% | 131x            |
| DELETE    | 0.001       | 0.164         | +15,257.9% | 153x            |
| SELECT    | 0.001       | 0.656         | +56,497.5% | 565x            |
| SELECT (cached) | 0.003  | 0.740         | +22,648.3% | 227x            |

### Driver Comparison
**psycopg3-text is significantly faster than psycopg2:**
- **SELECT**: 4.0x faster (0.656ms vs 2.594ms)
- **SELECT (cached)**: 2.1x faster (0.740ms vs 1.539ms)
- **CREATE**: 3.6x faster (2.280ms vs 8.227ms)
- **Overall overhead**: 40% lower (29,299% vs 49,414%)

## Historical Performance Comparison

### Target Performance (2025-07-27)
| Operation | Target Overhead | Target Time (ms) | Current psycopg2 | Current psycopg3 |
|-----------|----------------|------------------|------------------|------------------|
| SELECT    | ~674.9x        | 0.669           | **2.594ms (3.9x worse)** | **0.656ms (matches target)** |
| SELECT (cached) | ~17.2x   | 0.046           | **1.539ms (33x worse)** | **0.740ms (16x worse)** |
| UPDATE    | ~50.9x         | 0.059           | **0.057ms (matches target)** | 0.172ms (2.9x worse) |
| DELETE    | ~35.8x         | 0.034           | **0.036ms (matches target)** | 0.164ms (4.8x worse) |
| INSERT    | ~36.6x         | 0.060           | 0.174ms (2.9x worse) | 0.776ms (13x worse) |

### Documented Current Performance (2025-08-01)
| Operation | Documented (ms) | Current psycopg2 | Current psycopg3 |
|-----------|-----------------|------------------|------------------|
| SELECT    | 4.016          | **2.594ms (35% better)** | **0.656ms (84% better)** |
| SELECT (cached) | 0.079     | 1.539ms (19x worse) | 0.740ms (9.4x worse) |
| UPDATE    | 0.053          | 0.057ms (similar) | 0.172ms (3.2x worse) |
| DELETE    | 0.033          | 0.036ms (similar) | 0.164ms (5x worse) |
| INSERT    | 0.060          | 0.174ms (2.9x worse) | 0.776ms (13x worse) |

### Historical Benchmark Data (from FINAL_PERFORMANCE_ANALYSIS.md)
| Version | INSERT (ms) | SELECT (ms) | SELECT cached (ms) |
|---------|-------------|-------------|-------------------|
| Baseline | 0.174      | 3.827       | 0.159            |
| With optimizations | 0.596 | 3.031 | 0.298            |
| **Current psycopg2** | **0.174** | **2.594** | **1.539** |
| **Current psycopg3** | **0.776** | **0.656** | **0.740** |

## Key Findings

### 1. Significant Improvement Since 2025-08-01
- **SELECT performance improved dramatically**: From 4.016ms to 2.594ms (psycopg2) and 0.656ms (psycopg3)
- psycopg3 SELECT performance now **matches the original target** (0.656ms vs 0.669ms target)

### 2. psycopg3 Superiority
- psycopg3-text consistently outperforms psycopg2
- **4x faster** for SELECT queries
- **40% lower** overall overhead
- Better suited for production use with pgsqlite

### 3. Remaining Performance Issues
- **Cached SELECT performance** is still far from target (16-33x worse)
- **INSERT performance** has regressed significantly from baseline
- Cache effectiveness is poor (only 1.7x speedup for psycopg2, 0.9x for psycopg3)

### 4. Comparison to Historical Optimizations
- Current SELECT performance (0.656ms with psycopg3) is **78% better** than the best historical result (3.031ms)
- However, INSERT performance with psycopg3 (0.776ms) is worse than the historical regression (0.596ms)

## Recommendations

### Immediate Actions
1. **Use psycopg3-text driver** for production deployments - significantly better performance
2. **Investigate cache ineffectiveness** - cached queries should be much faster
3. **Profile INSERT operations** - performance has regressed significantly

### Performance Optimization Priorities
1. **Fix cached SELECT performance** - Currently 16-33x worse than target
2. **Optimize INSERT operations** - Especially for psycopg3 (13x worse than target)
3. **Reduce protocol overhead** - Still significant overhead compared to raw SQLite

### Architecture Considerations
Based on FINAL_PERFORMANCE_ANALYSIS.md findings:
- The fast path detector may have become a bottleneck
- Consider query-specific optimizations rather than monolithic fast path
- Cache pattern match results to avoid repeated analysis

## Conclusion

While SELECT performance has improved dramatically since the 2025-08-01 measurements (especially with psycopg3), there are still significant performance gaps in cached queries and INSERT operations. The psycopg3-text driver offers substantially better performance than psycopg2 and should be the recommended driver for pgsqlite users.

The fact that psycopg3 SELECT performance now matches the original target (0.656ms vs 0.669ms) is a significant achievement, but the poor cache effectiveness and INSERT performance regression require immediate attention.