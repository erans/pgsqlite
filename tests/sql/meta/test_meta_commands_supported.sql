-- Supported psql meta-command tests
-- These meta commands are fully supported in pgsqlite as of 2025-07-08

-- =============================================================================
-- FULLY WORKING META COMMANDS
-- =============================================================================

-- List all relations (tables, views, indexes) - WORKS!
-- Uses: pg_class view with pg_table_is_visible() function
\d

-- List only tables - WORKS!
-- Uses: pg_class with relkind='r' filter
\dt

-- List only views - WORKS!
-- Uses: pg_class with relkind='v' filter
\dv

-- List only indexes - WORKS!
-- Uses: pg_class with relkind='i' filter
\di

-- List only sequences - WORKS!
-- Uses: pg_class with relkind='S' filter
\ds

-- =============================================================================
-- PARTIALLY WORKING META COMMANDS
-- =============================================================================

-- Describe specific table - PARTIALLY WORKS
-- Works for tables created by pgsqlite, but not for all tables
-- Needs enhanced pg_attribute implementation
-- \d specific_table

-- List data types - PARTIALLY WORKS  
-- Shows built-in types but may not show all user-defined types correctly
-- \dT

-- =============================================================================
-- TEST DATA SETUP
-- =============================================================================

-- Create test tables to demonstrate working commands
CREATE TABLE IF NOT EXISTS test_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS test_products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2),
    stock INTEGER DEFAULT 0
);

-- Create a view
CREATE VIEW IF NOT EXISTS active_products AS
    SELECT * FROM test_products WHERE stock > 0;

-- Create an index
CREATE INDEX IF NOT EXISTS idx_users_email ON test_users(email);

-- Now demonstrate the working commands with actual data
\echo 'Listing all relations:'
\d

\echo '\nListing only tables:'
\dt

\echo '\nListing only views:'
\dv

\echo '\nListing only indexes:'
\di

-- Clean up
DROP VIEW IF EXISTS active_products;
DROP TABLE IF EXISTS test_users CASCADE;
DROP TABLE IF EXISTS test_products CASCADE;

-- Final check - should show only system catalog tables/views
\echo '\nAfter cleanup - only system catalogs remain:'
\d