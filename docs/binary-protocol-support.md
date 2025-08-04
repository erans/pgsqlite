# Binary Protocol Support in pgsqlite

## Overview

pgsqlite implements PostgreSQL's binary wire protocol format, enabling efficient data transfer for clients that support binary mode, such as psycopg3. Binary format provides better performance and precision compared to text format for many data types.

## Supported Binary Types

### Core Types (Implemented)
- **Boolean (OID 16)**: 1 byte (0 or 1)
- **Int2/Smallint (OID 21)**: 2 bytes, big-endian
- **Int4/Integer (OID 23)**: 4 bytes, big-endian
- **Int8/Bigint (OID 20)**: 8 bytes, big-endian
- **Float4/Real (OID 700)**: 4 bytes, IEEE 754 single precision
- **Float8/Double (OID 701)**: 8 bytes, IEEE 754 double precision
- **Text/Varchar (OID 25/1043)**: UTF-8 bytes (same as text format)
- **Bytea (OID 17)**: Raw bytes

### Advanced Types (Implemented)
- **Numeric/Decimal (OID 1700)**: PostgreSQL's custom format with weight, scale, and digit groups
- **UUID (OID 2950)**: 16 bytes raw UUID (no hyphens)
- **JSON (OID 114)**: UTF-8 JSON text (same as text format)
- **JSONB (OID 3802)**: 1-byte version header + JSON text
- **Money (OID 790)**: 8-byte integer representing cents * 100

### Date/Time Types (Implemented)
- **Date (OID 1082)**: 4 bytes, days since 2000-01-01
- **Time (OID 1083)**: 8 bytes, microseconds since midnight
- **Timestamp (OID 1114)**: 8 bytes, microseconds since 2000-01-01
- **Timestamptz (OID 1184)**: 8 bytes, microseconds since 2000-01-01 UTC
- **Interval (OID 1186)**: 16 bytes (8 bytes microseconds + 4 bytes days + 4 bytes months)

### Pending Implementation
- **Array Types**: Complex structure with dimensions, flags, and element OIDs
- **Range Types** (int4range, int8range, numrange): Flags byte + bounds
- **Network Types** (CIDR, INET, MACADDR): Address family + network bytes
- **Bit/Varbit**: Bit string encoding
- **Full-text Search** (tsvector, tsquery): Custom binary formats

## Using Binary Protocol

### With psycopg3

```python
import psycopg

# psycopg3 automatically uses binary format when beneficial
conn = psycopg.connect("host=localhost port=5432 dbname=mydb")

# Force binary format for specific queries
with conn.cursor() as cur:
    # Execute with binary parameter and result formats
    cur.execute(
        "SELECT id, amount, data FROM mytable WHERE id = %s",
        [123],
        binary=True  # Use binary format
    )
    row = cur.fetchone()
```

### With SQLAlchemy

```bash
# Run SQLAlchemy tests with psycopg3 binary mode
./tests/python/run_sqlalchemy_tests.sh --driver psycopg3-binary
```

## Performance Benefits

Binary format provides significant advantages for:
- **Large binary data**: No base64 encoding overhead
- **Numeric precision**: Exact decimal representation without string parsing
- **Network efficiency**: Smaller payload for numeric types
- **CPU efficiency**: No string parsing/formatting overhead

## Implementation Details

### Wire Protocol Integration

Binary format is negotiated in the PostgreSQL Extended Query Protocol:
1. **Parse** message: Client prepares statement
2. **Bind** message: Client specifies parameter format codes (0=text, 1=binary)
3. **Execute** message: Server responds with DataRow messages in requested format

### Type-Specific Encoding

Each PostgreSQL type has a specific binary representation:
- All multi-byte values use network byte order (big-endian)
- NULL values are represented by length -1
- Non-NULL values have a 4-byte length prefix followed by data

### Example: Numeric Binary Format

PostgreSQL's NUMERIC type uses a complex format:
```
struct NumericBinary {
    ndigits: i16,    // Number of digit groups
    weight: i16,     // Weight of first digit group
    sign: i16,       // 0x0000=positive, 0x4000=negative
    dscale: i16,     // Display scale
    digits: [i16],   // Digit groups (base 10000)
}
```

## Testing Binary Protocol

### Unit Tests
```bash
cargo test binary::tests --lib
```

### Integration Test
```python
# tests/python/test_psycopg3_binary.py
python test_psycopg3_binary.py
```

### Performance Comparison
```bash
# Compare text vs binary format performance
./tests/python/run_sqlalchemy_tests.sh --driver psycopg2         # Text format
./tests/python/run_sqlalchemy_tests.sh --driver psycopg3-binary  # Binary format
```

## Troubleshooting

### Common Issues

1. **"Unknown format code"**: Client requested binary format for unsupported type
   - Solution: Type will fall back to text format automatically

2. **Data corruption**: Binary data interpreted as text or vice versa
   - Check format codes in Bind/Execute messages
   - Verify type OIDs match expected values

3. **Endianness issues**: Values appear byte-swapped
   - All PostgreSQL binary formats use big-endian (network byte order)
   - Use `to_be_bytes()` / `from_be_bytes()` in Rust

### Debug Logging

Enable debug logging to see binary format details:
```bash
RUST_LOG=pgsqlite::protocol::binary=debug pgsqlite --database mydb.db
```

## Future Enhancements

1. **Complete Type Coverage**: Implement remaining PostgreSQL types
2. **Binary COPY Protocol**: Support for high-speed bulk data transfer
3. **Custom Type Support**: Allow plugins for domain-specific binary formats
4. **Compression**: Optional compression for large binary values