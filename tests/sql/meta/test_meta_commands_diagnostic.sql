-- Diagnostic test for psql meta-commands
-- This will help identify exactly which commands fail

\echo '=== Testing \d (list all relations) ==='
\d

-- Create test tables
CREATE TABLE IF NOT EXISTS meta_test_users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS meta_test_products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price DECIMAL(10,2),
    stock INTEGER DEFAULT 0
);

\echo '=== Testing \dt (list tables) ==='
\dt

\echo '=== Testing \di (list indexes) ==='
\di

\echo '=== Testing \d table_name (describe table) ==='
\echo 'Attempting: \d meta_test_users'
-- This will likely fail due to ::regclass casting
\d meta_test_users

\echo '=== Creating enum type ==='
CREATE TYPE IF NOT EXISTS order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled');

\echo '=== Testing \dT (list types) ==='
-- This will likely fail due to pg_type JOIN issues
\dT

\echo '=== Testing \dT type_name (describe type) ==='
-- This will likely fail
\dT order_status

\echo '=== Creating view ==='
CREATE VIEW IF NOT EXISTS active_products AS
    SELECT * FROM meta_test_products WHERE stock > 0;

\echo '=== Testing \dv (list views) ==='
\dv

\echo '=== Testing \ds (list sequences) ==='
\ds

\echo '=== Clean up ==='
DROP VIEW IF EXISTS active_products;
DROP TABLE IF EXISTS meta_test_users;
DROP TABLE IF EXISTS meta_test_products;  
DROP TYPE IF EXISTS order_status;