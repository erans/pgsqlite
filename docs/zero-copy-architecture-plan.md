# Zero-Copy Message Construction Architecture Plan

## Executive Summary

This document outlines a comprehensive plan to re-architect pgsqlite's protocol handling to achieve true zero-copy message construction. The current implementation uses a framed codec approach that requires message serialization, preventing optimal zero-copy operations. This plan proposes a fundamental redesign of the protocol layer to eliminate unnecessary allocations and memory copies.

## Current Architecture Limitations

### 1. Framed Codec Design
- **Current**: Uses `tokio_util::codec::Framed` with `PostgresCodec`
- **Problem**: Requires encoding messages into intermediate buffers before transmission
- **Impact**: Every message is serialized into `Vec<u8>` before being written to the socket

### 2. Message Type Architecture
```rust
// Current approach - requires allocation
pub enum Message {
    Query(String),
    DataRow(Vec<Option<Vec<u8>>>),
    // ... other variants
}

impl Encoder<Message> for PostgresCodec {
    fn encode(&mut self, msg: Message, dst: &mut BytesMut) -> Result<()> {
        // Must serialize the entire message structure
    }
}
```

### 3. Multiple Serialization Steps
1. Value conversion (SQLite → PostgreSQL format)
2. Message construction (creating Message enum)
3. Protocol encoding (Message → wire format)
4. Network transmission (buffer → socket)

## Proposed Zero-Copy Architecture

### 1. Direct Socket Writing
Replace the framed codec with direct socket operations:

```rust
pub struct ZeroCopyProtocolHandler {
    socket: TcpStream,
    write_buffer: BytesMut,
    read_buffer: BytesMut,
}

impl ZeroCopyProtocolHandler {
    async fn write_data_row(&mut self, row: &rusqlite::Row) -> Result<()> {
        // Write directly to socket buffer
        self.write_buffer.put_u8(b'D'); // Message type
        let len_pos = self.write_buffer.len();
        self.write_buffer.put_i32(0); // Placeholder
        
        // Direct value encoding without intermediate storage
        let column_count = row.column_count();
        self.write_buffer.put_i16(column_count as i16);
        
        for i in 0..column_count {
            // Encode values directly from SQLite to wire format
            self.encode_value_direct(row, i)?;
        }
        
        // Update length and flush
        self.update_message_length(len_pos);
        self.socket.write_all(&self.write_buffer).await?;
        self.write_buffer.clear();
        Ok(())
    }
}
```

### 2. Streaming Result Processing
Instead of collecting all rows before sending:

```rust
pub struct StreamingQueryExecutor {
    socket_writer: SocketWriter,
    type_converters: Arc<TypeConverterTable>,
}

impl StreamingQueryExecutor {
    async fn execute_query(&mut self, stmt: &rusqlite::Statement) -> Result<()> {
        // Send RowDescription immediately
        self.send_row_description(&stmt).await?;
        
        // Stream rows as they're fetched
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            // Write directly to socket without collecting
            self.socket_writer.write_data_row_direct(&row).await?;
        }
        
        // Send completion
        self.socket_writer.write_command_complete(tag).await?;
        Ok(())
    }
}
```

### 3. Memory-Mapped Value Access
For large values, use memory mapping:

```rust
pub struct MappedValueReader {
    mmap: memmap2::Mmap,
    offset: usize,
    length: usize,
}

impl MappedValueReader {
    async fn write_to_socket(&self, socket: &mut TcpStream) -> Result<()> {
        // Zero-copy write from mmap to socket
        let slice = &self.mmap[self.offset..self.offset + self.length];
        socket.write_all(slice).await?;
        Ok(())
    }
}
```

## Implementation Phases

### Phase 1: Protocol Handler Refactoring
**Duration**: 2-3 weeks

1. **Extract Protocol Writing**
   - Create `ProtocolWriter` trait
   - Implement both framed and direct versions
   - Add feature flag for switching implementations

2. **Benchmark Infrastructure**
   - Create benchmarks comparing framed vs direct
   - Measure allocation counts and memory usage
   - Profile CPU usage in serialization paths

### Phase 2: Direct Socket Implementation
**Duration**: 3-4 weeks

1. **Replace Framed Codec**
   - Implement `DirectProtocolHandler`
   - Handle message framing manually
   - Manage read/write buffers efficiently

2. **Connection Management**
   - Update `ServerConnection` to use direct handler
   - Implement proper backpressure handling
   - Add connection pooling for buffers

### Phase 3: Streaming Query Execution
**Duration**: 2-3 weeks

1. **Streaming Infrastructure**
   - Modify query executor to stream results
   - Remove result collection phase
   - Implement chunked transmission

2. **Memory Management**
   - Pre-allocate buffers based on query patterns
   - Implement buffer recycling
   - Add memory pressure monitoring

### Phase 4: Value Encoding Optimization
**Duration**: 2-3 weeks

1. **Direct SQLite-to-Wire Encoding**
   - Implement type-specific encoders
   - Skip intermediate PostgreSQL type conversion
   - Use unsafe code where beneficial

2. **Large Value Handling**
   - Implement memory-mapped access for BLOBs
   - Add streaming for large text values
   - Zero-copy BYTEA transmission

### Phase 5: Integration and Testing
**Duration**: 2 weeks

1. **Integration Testing**
   - Ensure protocol compliance
   - Test with various PostgreSQL clients
   - Verify correctness under load

2. **Performance Validation**
   - Benchmark against current implementation
   - Measure memory usage reduction
   - Profile allocation patterns

## Technical Challenges

### 1. Protocol Compliance
- Must maintain exact PostgreSQL wire protocol compatibility
- Handle all edge cases (partial writes, connection drops)
- Support both text and binary formats

### 2. Error Handling
- Cannot use `?` operator in hot paths (allocates for errors)
- Need careful error propagation without allocation
- Must handle partial message transmission

### 3. Async Complexity
- Direct socket operations are more complex than framed
- Need careful buffer management to avoid data races
- Backpressure handling becomes manual

### 4. Type System Integration
- Current type conversion assumes intermediate representations
- Need to redesign for direct encoding
- Must maintain correctness for all 40+ supported types

## Performance Targets

### Expected Improvements
1. **Memory Allocation**: 80-90% reduction in allocations per query
2. **CPU Usage**: 30-40% reduction in serialization overhead
3. **Latency**: 20-30% improvement for small result sets
4. **Throughput**: 40-50% improvement for large result sets

### Measurement Criteria
- Allocations per query (measured via custom allocator)
- CPU cycles in serialization paths (via perf)
- End-to-end latency for various query types
- Memory bandwidth utilization

## Risk Mitigation

### 1. Compatibility Risks
- Maintain extensive protocol test suite
- Test against multiple PostgreSQL client libraries
- Implement gradual rollout with feature flags

### 2. Complexity Risks
- Keep framed implementation as fallback
- Implement in phases with validation
- Extensive documentation and examples

### 3. Performance Risks
- May not achieve expected gains for small queries
- Network latency might dominate for remote connections
- Profile continuously during development

## Alternative Approaches Considered

### 1. io_uring on Linux
- **Pros**: True zero-copy with kernel bypass
- **Cons**: Linux-only, complex implementation
- **Decision**: Consider as future enhancement

### 2. Custom Allocator
- **Pros**: Can optimize allocation patterns
- **Cons**: Doesn't eliminate fundamental copies
- **Decision**: Not sufficient alone

### 3. Shared Memory Protocol
- **Pros**: Eliminates network stack entirely
- **Cons**: Local connections only
- **Decision**: Consider as additional option

## Success Criteria

1. **Functional**: All existing tests pass
2. **Performance**: 40%+ reduction in serialization overhead
3. **Memory**: 80%+ reduction in allocations
4. **Compatibility**: Works with all major PostgreSQL clients
5. **Maintainability**: Clean, documented architecture

## Timeline Summary

- **Phase 1**: 2-3 weeks - Protocol handler refactoring
- **Phase 2**: 3-4 weeks - Direct socket implementation  
- **Phase 3**: 2-3 weeks - Streaming query execution
- **Phase 4**: 2-3 weeks - Value encoding optimization
- **Phase 5**: 2 weeks - Integration and testing

**Total Duration**: 12-15 weeks

## Conclusion

Implementing true zero-copy message construction requires fundamental architectural changes but promises significant performance improvements. The phased approach allows for incremental validation while maintaining system stability. The investment is justified for a high-performance protocol adapter where serialization overhead is a significant bottleneck.