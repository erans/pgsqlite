use pgsqlite::query::simple_query_detector::is_fast_path_simple_query;

fn main() {
    let queries = vec\![
        "INSERT INTO benchmark_table_pg (text_col, int_col, real_col, bool_col) VALUES (%s, %s, %s, %s) RETURNING id",
        "UPDATE benchmark_table_pg SET text_col = %s WHERE id = %s",
        "DELETE FROM benchmark_table_pg WHERE id = %s",
        "SELECT * FROM benchmark_table_pg WHERE int_col > %s",
    ];
    
    for query in queries {
        println\!("{}: {}", query, is_fast_path_simple_query(query));
    }
}
