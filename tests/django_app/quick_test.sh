#!/bin/bash

# Quick test to verify Django + pgsqlite basic functionality
set -e

echo "=== Quick Django + pgsqlite Test ==="

# Change to the django app directory
cd tests/django_app

# Start pgsqlite in background
../../target/release/pgsqlite --database "test_quick.db" --port 5432 &
PGSQLITE_PID=$!

# Wait for pgsqlite to start
sleep 3

echo "pgsqlite started with PID $PGSQLITE_PID"

# Test database connection
if poetry run python -c "
import psycopg2
try:
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='testdb',
        user='testuser',
        password='testpass'
    )
    print('✓ Database connection successful')
    conn.close()
except Exception as e:
    print(f'✗ Database connection failed: {e}')
    exit(1)
"; then
    echo "✓ PostgreSQL connection via pgsqlite working"
else
    echo "✗ PostgreSQL connection failed"
fi

# Test Django migration
echo "Testing Django migrations..."
poetry run python manage.py makemigrations books
poetry run python manage.py migrate

echo "✓ Django migrations look good"

# Cleanup
kill $PGSQLITE_PID 2>/dev/null || true
rm -f test_quick.db

echo "✓ Quick test completed successfully!"