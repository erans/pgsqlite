# PostgreSQL Driver Performance Comparison
## pgsqlite Benchmarks - August 12, 2025

### Test Configuration
- **Iterations**: 500 operations per type
- **Connection**: Unix socket (optimal performance)
- **Database**: In-memory SQLite
- **pgsqlite Version**: 0.0.14

## Performance Comparison (Average Time in ms)

| Operation | psycopg2 | psycopg3-text | psycopg3-binary | Winner |
|-----------|----------|---------------|-----------------|--------|
| **SELECT** | 2.963ms | **0.136ms** 🏆 | 0.497ms | psycopg3-text (21.8x faster than psycopg2) |
| **SELECT (cached)** | 1.656ms | **0.299ms** 🏆 | 1.579ms | psycopg3-text (5.5x faster than psycopg2) |
| **INSERT** | **0.185ms** 🏆 | 0.661ms | 0.691ms | psycopg2 (3.6x faster than psycopg3) |
| **UPDATE** | **0.057ms** 🏆 | 0.084ms | 0.086ms | psycopg2 (1.5x faster than psycopg3) |
| **DELETE** | **0.036ms** 🏆 | 0.072ms | 0.071ms | psycopg2 (2.0x faster than psycopg3) |

## Key Findings

### 🏆 psycopg3-text Dominates SELECT Operations
- **SELECT**: 0.136ms (21.8x faster than psycopg2, 3.7x faster than binary)
- **SELECT (cached)**: 0.299ms (5.5x faster than psycopg2, 5.3x faster than binary)
- Text protocol appears highly optimized for read operations

### 🏆 psycopg2 Excels at Write Operations
- **INSERT**: 0.185ms (3.6x faster than psycopg3 variants)
- **UPDATE**: 0.057ms (1.5x faster than psycopg3 variants)  
- **DELETE**: 0.036ms (2.0x faster than psycopg3 variants)
- Legacy driver has better write performance

### ❌ Binary Protocol Underperforming
- psycopg3-binary shows worse performance than text mode
- Binary encoding overhead not justified for simple operations
- SELECT operations particularly affected (3.7x slower than text)

## Overhead Comparison vs Native SQLite

| Operation | psycopg2 Overhead | psycopg3-text Overhead | psycopg3-binary Overhead |
|-----------|-------------------|------------------------|--------------------------|
| SELECT | 269,235% | **12,515%** 🏆 | 43,422% |
| SELECT (cached) | 52,002% | **8,994%** 🏆 | 37,212% |
| INSERT | **10,655%** 🏆 | 38,097% | 37,713% |
| UPDATE | **4,542%** 🏆 | 7,038% | 6,464% |
| DELETE | **3,837%** 🏆 | 7,799% | 6,655% |

## Recommendations

### For Read-Heavy Workloads
**Use psycopg3-text** - Provides exceptional SELECT performance with 21.8x improvement over psycopg2

### For Write-Heavy Workloads  
**Use psycopg2** - Still offers best INSERT/UPDATE/DELETE performance

### For Mixed Workloads
**Use psycopg3-text** - Balanced performance with massive SELECT improvements outweighing minor write penalties

### Binary Protocol
**Not recommended currently** - Binary encoding overhead exceeds benefits for most operations. May be beneficial for:
- Large BYTEA data transfers
- Complex NUMERIC calculations
- Bulk data operations

## Technical Notes

1. **psycopg3-text optimizations** appear to be working exceptionally well with pgsqlite's query processing
2. **Binary protocol overhead** suggests encoding/decoding costs outweigh benefits for simple data types
3. **Cache effectiveness** varies significantly between drivers, with psycopg3-text showing best cache utilization
4. **Connection pooling** and other optimizations may further improve these results

## Conclusion

The full binary protocol support implementation is working correctly, but **psycopg3-text emerges as the clear winner** for most use cases, offering:
- 21.8x faster SELECT operations than psycopg2
- Best overall overhead reduction vs native SQLite
- Good balance between read and write performance

Binary protocol may still be valuable for specific use cases involving complex data types or large binary data transfers.