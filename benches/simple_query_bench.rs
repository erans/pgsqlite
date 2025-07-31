use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pgsqlite::query::simple_query_detector::is_fast_path_simple_query;

fn benchmark_simple_query_detection(c: &mut Criterion) {
    let simple_queries = vec![
        "SELECT * FROM users",
        "SELECT id, name FROM users WHERE id = $1",
        "INSERT INTO users (name, email) VALUES ($1, $2)",
        "UPDATE users SET name = $1 WHERE id = $2",
        "DELETE FROM users WHERE id = $1",
    ];
    
    let complex_queries = vec![
        "SELECT * FROM users WHERE created_at::date = $1",
        "SELECT * FROM pg_catalog.pg_tables",
        "SELECT * FROM users WHERE email ~ '@gmail.com'",
        "SELECT * FROM users WHERE id = ANY($1)",
        "DELETE FROM users USING orders WHERE users.id = orders.user_id",
    ];
    
    c.bench_function("fast_path_simple_queries", |b| {
        b.iter(|| {
            for query in &simple_queries {
                black_box(is_fast_path_simple_query(query));
            }
        })
    });
    
    c.bench_function("fast_path_complex_queries", |b| {
        b.iter(|| {
            for query in &complex_queries {
                black_box(is_fast_path_simple_query(query));
            }
        })
    });
}

criterion_group!(benches, benchmark_simple_query_detection);
criterion_main!(benches);