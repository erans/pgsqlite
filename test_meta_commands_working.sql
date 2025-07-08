-- Working psql meta-command tests
-- Only commands that work with current pgsqlite implementation

-- List all relations (tables, views, indexes) - NOW WORKS!
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

-- List tables (uses simpler query than \d)
\dt

-- List indexes
\di

-- Describe specific tables (these work because they query single tables)
\d meta_test_users
\d meta_test_products

-- Create an enum type
CREATE TYPE IF NOT EXISTS order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled');

-- List data types
\dT

-- List enum types specifically
\dT order_status

-- Create a view
CREATE VIEW IF NOT EXISTS active_products AS
    SELECT * FROM meta_test_products WHERE stock > 0;

-- List views
\dv

-- Clean up
DROP VIEW IF EXISTS active_products;
DROP TABLE IF EXISTS meta_test_users;
DROP TABLE IF EXISTS meta_test_products;  
DROP TYPE IF EXISTS order_status;