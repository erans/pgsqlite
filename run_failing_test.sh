#!/bin/bash
RUST_LOG=info,pgsqlite=debug,tokio_postgres=debug cargo test --test catalog_where_simple_test test_pg_class_all_columns -- --nocapture 2>&1 | tee test_output.log