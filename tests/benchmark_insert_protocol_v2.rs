use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use rusqlite::{params, Connection};

/// Benchmark to profile INSERT protocol overhead
#[tokio::test]
#[ignore]
async fn benchmark_insert_protocol_overhead_v2() {
    // Setup test database
    let db_path = "/tmp/pgsqlite_bench_insert.db";
    let _ = std::fs::remove_file(db_path);
    
    let conn = Connection::open(db_path).unwrap();
    
    // Create a simple table
    conn.execute(
        "CREATE TABLE benchmark_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
        [],
    ).unwrap();
    
    // Also create the pgsqlite schema table (needed for the server)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            column_type TEXT NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )",
        [],
    ).unwrap();
    
    // Insert schema info
    conn.execute(
        "INSERT INTO __pgsqlite_schema VALUES ('benchmark_table', 'id', 'int4')",
        [],
    ).unwrap();
    conn.execute(
        "INSERT INTO __pgsqlite_schema VALUES ('benchmark_table', 'name', 'text')",
        [],
    ).unwrap();
    conn.execute(
        "INSERT INTO __pgsqlite_schema VALUES ('benchmark_table', 'value', 'int4')",
        [],
    ).unwrap();
    
    drop(conn); // Close connection before starting server
    
    // Kill any existing server on port 5434
    let _ = std::process::Command::new("pkill")
        .args(&["-f", "pgsqlite.*5434"])
        .output();
    
    // Start pgsqlite server
    let _server_handle = tokio::spawn(async move {
        let output = std::process::Command::new("cargo")
            .args(&["run", "--release", "--", "-d", db_path, "-p", "5434", "--log-level", "error"])
            .spawn()
            .expect("Failed to start pgsqlite server");
        
        // Give server time to start
        tokio::time::sleep(Duration::from_millis(500)).await;
        output
    });
    
    // Wait for server to start
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Benchmark configurations
    const WARMUP_ITERATIONS: usize = 100;
    const TEST_ITERATIONS: usize = 1000;
    
    println!("\n=== INSERT Protocol Overhead Benchmark ===\n");
    
    // 1. Baseline: Direct SQLite INSERT
    let conn = Connection::open(db_path).unwrap();
    let mut baseline_times = Vec::new();
    
    for i in 0..WARMUP_ITERATIONS + TEST_ITERATIONS {
        let start = Instant::now();
        conn.execute(
            "INSERT INTO benchmark_table (name, value) VALUES (?1, ?2)",
            params![format!("test_{}", i), i as i32],
        ).unwrap();
        let elapsed = start.elapsed();
        
        if i >= WARMUP_ITERATIONS {
            baseline_times.push(elapsed);
        }
    }
    let baseline_avg = average_duration(&baseline_times);
    println!("1. Direct SQLite INSERT: {:?} avg", baseline_avg);
    
    // 2. Full protocol - Simple Query
    let mut stream = TcpStream::connect("127.0.0.1:5434").await.unwrap();
    perform_startup(&mut stream).await;
    
    let mut simple_protocol_times = Vec::new();
    for i in 0..WARMUP_ITERATIONS + TEST_ITERATIONS {
        let query = format!("INSERT INTO benchmark_table (name, value) VALUES ('test_simple_{}', {})", i, i);
        let start = Instant::now();
        
        // Send Query message
        send_query(&mut stream, &query).await;
        
        // Read CommandComplete and ReadyForQuery
        read_command_complete(&mut stream).await;
        read_ready_for_query(&mut stream).await;
        
        let elapsed = start.elapsed();
        if i >= WARMUP_ITERATIONS {
            simple_protocol_times.push(elapsed);
        }
    }
    let simple_protocol_avg = average_duration(&simple_protocol_times);
    println!("2. Simple protocol INSERT: {:?} avg ({:.1}x overhead)", 
        simple_protocol_avg, simple_protocol_avg.as_secs_f64() / baseline_avg.as_secs_f64());
    
    // 3. Full protocol - Extended Query with parameters
    let mut extended_protocol_times = Vec::new();
    
    // Prepare statement once
    send_parse(&mut stream, "INSERT INTO benchmark_table (name, value) VALUES ($1, $2)").await;
    read_parse_complete(&mut stream).await;
    
    for i in 0..WARMUP_ITERATIONS + TEST_ITERATIONS {
        let start = Instant::now();
        
        // Bind parameters
        send_bind(&mut stream, &[
            format!("test_ext_{}", i).as_bytes().to_vec(),
            i.to_string().as_bytes().to_vec(),
        ]).await;
        read_bind_complete(&mut stream).await;
        
        // Execute
        send_execute(&mut stream).await;
        read_command_complete(&mut stream).await;
        
        // Sync
        send_sync(&mut stream).await;
        read_ready_for_query(&mut stream).await;
        
        let elapsed = start.elapsed();
        if i >= WARMUP_ITERATIONS {
            extended_protocol_times.push(elapsed);
        }
    }
    let extended_protocol_avg = average_duration(&extended_protocol_times);
    println!("3. Extended protocol INSERT: {:?} avg ({:.1}x overhead)", 
        extended_protocol_avg, extended_protocol_avg.as_secs_f64() / baseline_avg.as_secs_f64());
    
    // 4. Measure network round-trip time
    let mut ping_times = Vec::new();
    for _ in 0..100 {
        let start = Instant::now();
        // Send a simple query that does nothing
        send_query(&mut stream, "SELECT 1").await;
        read_data_row(&mut stream).await;
        read_command_complete(&mut stream).await;
        read_ready_for_query(&mut stream).await;
        ping_times.push(start.elapsed());
    }
    let ping_avg = average_duration(&ping_times);
    println!("\n4. Network round-trip (SELECT 1): {:?} avg", ping_avg);
    
    // 5. Profile individual protocol steps
    println!("\n=== Protocol Step Breakdown ===");
    profile_protocol_steps(&mut stream, baseline_avg).await;
    
    // Summary
    println!("\n=== Summary ===");
    println!("Baseline (Direct SQLite):     {:?}", baseline_avg);
    println!("Simple protocol overhead:     {:.1}x", simple_protocol_avg.as_secs_f64() / baseline_avg.as_secs_f64());
    println!("Extended protocol overhead:   {:.1}x", extended_protocol_avg.as_secs_f64() / baseline_avg.as_secs_f64());
    println!("Network round-trip time:      {:?}", ping_avg);
    println!("\nProtocol overhead per INSERT: {:?}", simple_protocol_avg - baseline_avg);
    println!("Network overhead percentage:  {:.1}%", 
        (ping_avg.as_secs_f64() / simple_protocol_avg.as_secs_f64()) * 100.0);
    
    // Kill the server
    std::process::Command::new("pkill")
        .args(&["-f", "pgsqlite.*5434"])
        .output()
        .unwrap();
}

async fn profile_protocol_steps(stream: &mut TcpStream, baseline: Duration) {
    const ITERATIONS: usize = 100;
    
    // Measure individual message overhead
    println!("\nMessage Processing Times:");
    
    // Parse step
    let mut parse_times = Vec::new();
    for i in 0..ITERATIONS {
        let query = format!("INSERT INTO benchmark_table (name, value) VALUES ($1, $2) -- {}", i);
        let start = Instant::now();
        send_parse(stream, &query).await;
        read_parse_complete(stream).await;
        parse_times.push(start.elapsed());
    }
    let parse_avg = average_duration(&parse_times);
    println!("  Parse message:    {:?} ({:.1}x of baseline)", 
        parse_avg, parse_avg.as_secs_f64() / baseline.as_secs_f64());
    
    // Bind step
    let mut bind_times = Vec::new();
    for i in 0..ITERATIONS {
        let start = Instant::now();
        send_bind(stream, &[format!("bind_{}", i).as_bytes().to_vec(), b"123".to_vec()]).await;
        read_bind_complete(stream).await;
        bind_times.push(start.elapsed());
    }
    let bind_avg = average_duration(&bind_times);
    println!("  Bind message:     {:?} ({:.1}x of baseline)", 
        bind_avg, bind_avg.as_secs_f64() / baseline.as_secs_f64());
    
    // Execute step (with actual INSERT)
    let mut execute_times = Vec::new();
    send_parse(stream, "INSERT INTO benchmark_table (name, value) VALUES ($1, $2)").await;
    read_parse_complete(stream).await;
    
    for i in 0..ITERATIONS {
        send_bind(stream, &[format!("exec_{}", i).as_bytes().to_vec(), i.to_string().as_bytes().to_vec()]).await;
        read_bind_complete(stream).await;
        
        let start = Instant::now();
        send_execute(stream).await;
        read_command_complete(stream).await;
        execute_times.push(start.elapsed());
        
        send_sync(stream).await;
        read_ready_for_query(stream).await;
    }
    let execute_avg = average_duration(&execute_times);
    println!("  Execute message:  {:?} ({:.1}x of baseline)", 
        execute_avg, execute_avg.as_secs_f64() / baseline.as_secs_f64());
    
    // Total protocol overhead
    let total_extended = parse_avg + bind_avg + execute_avg;
    println!("\nTotal extended protocol: {:?} ({:.1}x of baseline)",
        total_extended, total_extended.as_secs_f64() / baseline.as_secs_f64());
    
    // Breakdown percentages
    println!("\nProtocol overhead breakdown:");
    println!("  Parse:   {:.1}% of total", (parse_avg.as_secs_f64() / total_extended.as_secs_f64()) * 100.0);
    println!("  Bind:    {:.1}% of total", (bind_avg.as_secs_f64() / total_extended.as_secs_f64()) * 100.0);
    println!("  Execute: {:.1}% of total", (execute_avg.as_secs_f64() / total_extended.as_secs_f64()) * 100.0);
}

fn average_duration(times: &[Duration]) -> Duration {
    let total: Duration = times.iter().sum();
    total / times.len() as u32
}

// Protocol helper functions
async fn perform_startup(stream: &mut TcpStream) {
    // Send startup message
    let mut startup = vec![];
    startup.extend_from_slice(&196608u32.to_be_bytes()); // Protocol version 3.0
    startup.extend_from_slice(b"user\0test\0database\0test\0\0");
    let len = ((startup.len() + 4) as u32).to_be_bytes();
    stream.write_all(&len).await.unwrap();
    stream.write_all(&startup).await.unwrap();
    
    // Read until ReadyForQuery
    loop {
        let mut msg_type = [0u8; 1];
        stream.read_exact(&mut msg_type).await.unwrap();
        
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize - 4;
        
        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await.unwrap();
        
        if msg_type[0] == b'Z' {
            break;
        }
    }
}

async fn send_query(stream: &mut TcpStream, query: &str) {
    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&((query.len() + 5) as u32).to_be_bytes());
    msg.extend_from_slice(query.as_bytes());
    msg.push(0);
    stream.write_all(&msg).await.unwrap();
}

async fn send_parse(stream: &mut TcpStream, query: &str) {
    let mut msg = vec![b'P'];
    let payload_len = 1 + query.len() + 1 + 2;
    msg.extend_from_slice(&((payload_len + 4) as u32).to_be_bytes());
    msg.push(0); // Empty statement name
    msg.extend_from_slice(query.as_bytes());
    msg.push(0);
    msg.extend_from_slice(&0u16.to_be_bytes()); // No parameter types
    stream.write_all(&msg).await.unwrap();
}

async fn send_bind(stream: &mut TcpStream, params: &[Vec<u8>]) {
    let mut msg = vec![b'B'];
    let mut payload = vec![];
    payload.push(0); // Empty portal name
    payload.push(0); // Empty statement name
    payload.extend_from_slice(&0u16.to_be_bytes()); // All text format
    payload.extend_from_slice(&(params.len() as u16).to_be_bytes());
    
    for param in params {
        payload.extend_from_slice(&(param.len() as u32).to_be_bytes());
        payload.extend_from_slice(param);
    }
    
    payload.extend_from_slice(&0u16.to_be_bytes()); // All text results
    
    msg.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    msg.extend_from_slice(&payload);
    stream.write_all(&msg).await.unwrap();
}

async fn send_execute(stream: &mut TcpStream) {
    let msg = vec![
        b'E',
        0, 0, 0, 9, // Length
        0, // Empty portal name
        0, 0, 0, 0, // No row limit
    ];
    stream.write_all(&msg).await.unwrap();
}

async fn send_sync(stream: &mut TcpStream) {
    let msg = vec![b'S', 0, 0, 0, 4];
    stream.write_all(&msg).await.unwrap();
}

async fn read_parse_complete(stream: &mut TcpStream) {
    let mut buf = vec![0u8; 5];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf[0], b'1'); // ParseComplete
}

async fn read_bind_complete(stream: &mut TcpStream) {
    let mut buf = vec![0u8; 5];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf[0], b'2'); // BindComplete
}

async fn read_command_complete(stream: &mut TcpStream) {
    let mut msg_type = [0u8; 1];
    stream.read_exact(&mut msg_type).await.unwrap();
    assert_eq!(msg_type[0], b'C');
    
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize - 4;
    
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await.unwrap();
}

async fn read_ready_for_query(stream: &mut TcpStream) {
    let mut buf = vec![0u8; 6];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(buf[0], b'Z'); // ReadyForQuery
}

async fn read_data_row(stream: &mut TcpStream) {
    let mut msg_type = [0u8; 1];
    stream.read_exact(&mut msg_type).await.unwrap();
    
    if msg_type[0] == b'T' {
        // RowDescription, skip it
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize - 4;
        let mut data = vec![0u8; len];
        stream.read_exact(&mut data).await.unwrap();
        
        // Now read actual DataRow
        stream.read_exact(&mut msg_type).await.unwrap();
    }
    
    assert_eq!(msg_type[0], b'D'); // DataRow
    
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize - 4;
    
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await.unwrap();
}