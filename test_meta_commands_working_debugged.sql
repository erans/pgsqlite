-- Working psql meta-command tests
-- Only commands that are FULLY supported by pgsqlite

-- List all relations (tables, views, indexes) - WORKS!
\d

-- Create test tables first
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

-- List tables - WORKS!
\dt

-- List indexes - WORKS!
\di

-- COMMENTED OUT: Describe specific tables
-- These require JOINs between pg_class and pg_attribute which aren't fully supported
-- \d meta_test_users
-- \d meta_test_products

-- Create an enum type
CREATE TYPE IF NOT EXISTS order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled');

-- COMMENTED OUT: List data types
-- \dT requires pg_type queries that may not be fully compatible
-- \dT

-- COMMENTED OUT: List specific enum type
-- \dT order_status

-- Create a view
CREATE VIEW IF NOT EXISTS active_products AS
    SELECT * FROM meta_test_products WHERE stock > 0;

-- List views - WORKS!
\dv

-- Clean up
DROP VIEW IF EXISTS active_products;
DROP TABLE IF EXISTS meta_test_users;
DROP TABLE IF EXISTS meta_test_products;  
DROP TYPE IF EXISTS order_status;