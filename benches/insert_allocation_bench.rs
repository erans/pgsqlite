use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use pgsqlite::protocol::BackendMessage;

/// Benchmark to demonstrate the allocation overhead of the current approach
fn bench_insert_allocations(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_allocations");
    
    // Benchmark creating CommandComplete messages the current way
    group.bench_function("current_approach", |b| {
        b.iter(|| {
            // This is what happens in the current implementation
            let tag = String::from("INSERT 0 1");
            let msg = BackendMessage::CommandComplete { tag };
            // In practice, this would be sent through framed.send(msg)
            std::hint::black_box(msg);
        });
    });
    
    // Benchmark the zero-copy approach (simulated)
    group.bench_function("zero_copy_approach", |b| {
        b.iter(|| {
            // This is what would happen with DirectWriter
            let tag = "INSERT 0 1"; // &'static str, no allocation
            // DirectWriter would write this directly to buffer
            std::hint::black_box(tag);
        });
    });
    
    // Benchmark with varying row counts
    for rows in [1, 10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("current_with_rows", rows),
            &rows,
            |b, &rows| {
                b.iter(|| {
                    let tag = format!("INSERT 0 {}", rows);
                    let msg = BackendMessage::CommandComplete { tag };
                    std::hint::black_box(msg);
                });
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("zero_copy_with_rows", rows),
            &rows,
            |b, &rows| {
                b.iter(|| {
                    // For zero-copy, we'd use static strings for common cases
                    let tag = match rows {
                        1 => "INSERT 0 1",
                        _ => {
                            // Only allocate for uncommon cases
                            let allocated = format!("INSERT 0 {}", rows);
                            std::hint::black_box(allocated);
                            return;
                        }
                    };
                    std::hint::black_box(tag);
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark the full message creation cycle
fn bench_message_lifecycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_lifecycle");
    
    // Current approach: Create enum, serialize, send
    group.bench_function("current_full_cycle", |b| {
        let mut buffer = bytes::BytesMut::with_capacity(1024);
        
        b.iter(|| {
            buffer.clear();
            
            // Step 1: Allocate String
            let tag = String::from("INSERT 0 1");
            
            // Step 2: Create enum variant (moves String)
            let msg = BackendMessage::CommandComplete { tag };
            
            // Step 3: Serialize (would happen in encoder)
            // Simulating what the encoder does
            buffer.extend_from_slice(b"C"); // Message type
            buffer.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
            buffer.extend_from_slice(b"INSERT 0 1");
            buffer.extend_from_slice(&[0]); // Null terminator
            
            std::hint::black_box(&buffer);
        });
    });
    
    // Zero-copy approach: Direct buffer writing
    group.bench_function("zero_copy_full_cycle", |b| {
        let mut buffer = bytes::BytesMut::with_capacity(1024);
        
        b.iter(|| {
            buffer.clear();
            
            // Direct writing, no intermediate allocations
            buffer.extend_from_slice(b"C"); // Message type
            buffer.extend_from_slice(&[0, 0, 0, 0]); // Length placeholder
            buffer.extend_from_slice(b"INSERT 0 1");
            buffer.extend_from_slice(&[0]); // Null terminator
            
            std::hint::black_box(&buffer);
        });
    });
    
    group.finish();
}

criterion_group!(benches, bench_insert_allocations, bench_message_lifecycle);
criterion_main!(benches);