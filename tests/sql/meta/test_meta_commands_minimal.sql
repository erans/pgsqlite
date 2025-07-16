-- Minimal psql meta-command tests
-- Only includes commands that should work with current pgsqlite implementation

-- Basic table listing (should work - uses pg_class)
\d

-- List tables (should work - uses pg_class)
\dt

-- List views (should work - uses pg_class)
\dv

-- List indexes (should work - uses pg_class)
\di

-- List sequences (should work - uses pg_class)
\ds

-- Describe a specific table if it exists
-- First create a test table
CREATE TABLE IF NOT EXISTS test_meta_table (
    id INTEGER PRIMARY KEY,
    name TEXT,
    value NUMERIC(10,2)
);

-- Now describe it (uses pg_class and pg_attribute)
\d test_meta_table

-- List types (should work - uses pg_type)
\dT

-- Create an enum type to test
CREATE TYPE IF NOT EXISTS test_status AS ENUM ('active', 'inactive', 'pending');

-- List enum types specifically
\dT test_status

-- Show enum values (uses pg_enum)
SELECT enumlabel FROM pg_catalog.pg_enum WHERE enumtypid = (SELECT oid FROM pg_catalog.pg_type WHERE typname = 'test_status') ORDER BY enumsortorder;

-- Clean up
DROP TABLE IF EXISTS test_meta_table;
DROP TYPE IF EXISTS test_status;