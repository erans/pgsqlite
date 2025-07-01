use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use tokio::runtime::Runtime;
use pgsqlite::protocol::{ProtocolWriter, FramedWriter, DirectWriter, PostgresCodec, FieldDescription, TransactionStatus};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use std::time::Duration;

async fn create_socket_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    
    let client_future = TcpStream::connect(addr);
    let server_future = async {
        listener.accept().await.unwrap().0
    };
    
    let (client, server) = tokio::join!(client_future, server_future);
    (client.unwrap(), server)
}

fn create_test_fields(count: usize) -> Vec<FieldDescription> {
    (0..count)
        .map(|i| FieldDescription {
            name: format!("column_{}", i),
            table_oid: 0,
            column_id: i as i16,
            type_oid: 25, // TEXT
            type_size: -1,
            type_modifier: -1,
            format: 0,
        })
        .collect()
}

fn create_test_row(columns: usize) -> Vec<Option<Vec<u8>>> {
    (0..columns)
        .map(|i| Some(format!("value_{}", i).into_bytes()))
        .collect()
}

async fn benchmark_framed_writer(writer: &mut FramedWriter, fields: &[FieldDescription], rows: usize) {
    // Send row description
    writer.send_row_description(fields).await.unwrap();
    
    // Send data rows
    let row = create_test_row(fields.len());
    for _ in 0..rows {
        writer.send_data_row(&row).await.unwrap();
    }
    
    // Send completion
    writer.send_command_complete(&format!("SELECT {}", rows)).await.unwrap();
    writer.send_ready_for_query(TransactionStatus::Idle).await.unwrap();
    writer.flush().await.unwrap();
}

async fn benchmark_direct_writer(writer: &mut DirectWriter, fields: &[FieldDescription], rows: usize) {
    // Send row description
    writer.send_row_description(fields).await.unwrap();
    
    // Send data rows
    let row = create_test_row(fields.len());
    for _ in 0..rows {
        writer.send_data_row(&row).await.unwrap();
    }
    
    // Send completion
    writer.send_command_complete(&format!("SELECT {}", rows)).await.unwrap();
    writer.send_ready_for_query(TransactionStatus::Idle).await.unwrap();
    writer.flush().await.unwrap();
}

fn bench_protocol_writers(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("protocol_writers");
    
    // Test different scenarios
    let scenarios = vec![
        ("small_result", 5, 10),    // 5 columns, 10 rows
        ("medium_result", 10, 100), // 10 columns, 100 rows
        ("large_result", 20, 1000), // 20 columns, 1000 rows
    ];
    
    for (name, columns, rows) in scenarios {
        let fields = create_test_fields(columns);
        
        // Benchmark Framed writer
        group.bench_with_input(
            BenchmarkId::new("framed", name),
            &(columns, rows),
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let (_client, server) = create_socket_pair().await;
                        let framed = Framed::new(server, PostgresCodec::new());
                        let mut writer = FramedWriter::new(framed);
                        benchmark_framed_writer(&mut writer, &fields, rows).await;
                    })
                });
            },
        );
        
        // Benchmark Direct writer
        group.bench_with_input(
            BenchmarkId::new("direct", name),
            &(columns, rows),
            |b, _| {
                b.iter(|| {
                    rt.block_on(async {
                        let (_client, server) = create_socket_pair().await;
                        let mut writer = DirectWriter::new(server);
                        benchmark_direct_writer(&mut writer, &fields, rows).await;
                    })
                });
            },
        );
    }
    
    group.finish();
}

fn bench_message_encoding(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("message_encoding");
    group.measurement_time(Duration::from_secs(10));
    
    // Benchmark individual message encoding
    group.bench_function("encode_data_row_10_columns", |b| {
        let row = create_test_row(10);
        b.iter(|| {
            rt.block_on(async {
                let (_client, server) = create_socket_pair().await;
                let mut writer = DirectWriter::new(server);
                writer.send_data_row(&row).await.unwrap();
            })
        });
    });
    
    group.bench_function("encode_row_description_10_fields", |b| {
        let fields = create_test_fields(10);
        b.iter(|| {
            rt.block_on(async {
                let (_client, server) = create_socket_pair().await;
                let mut writer = DirectWriter::new(server);
                writer.send_row_description(&fields).await.unwrap();
            })
        });
    });
    
    group.finish();
}

fn bench_zero_copy_effectiveness(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("zero_copy_effectiveness");
    
    // Compare sending pre-encoded vs owned data
    let row_data: Vec<Option<Vec<u8>>> = (0..10)
        .map(|i| Some(format!("value_{}", i).into_bytes()))
        .collect();
    
    let row_refs: Vec<Option<&[u8]>> = row_data.iter()
        .map(|v| v.as_ref().map(|vec| vec.as_slice()))
        .collect();
    
    group.bench_function("send_owned_data", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (_client, server) = create_socket_pair().await;
                let mut writer = DirectWriter::new(server);
                writer.send_data_row(&row_data).await.unwrap();
            })
        });
    });
    
    group.bench_function("send_borrowed_data", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (_client, server) = create_socket_pair().await;
                let mut writer = DirectWriter::new(server);
                writer.send_data_row_raw(&row_refs).await.unwrap();
            })
        });
    });
    
    group.finish();
}

criterion_group!(benches, bench_protocol_writers, bench_message_encoding, bench_zero_copy_effectiveness);
criterion_main!(benches);