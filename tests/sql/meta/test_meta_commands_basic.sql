-- Basic psql meta-command tests
-- Only the most fundamental commands

-- Create a test table first
CREATE TABLE IF NOT EXISTS test_table (
    id INTEGER PRIMARY KEY,
    name TEXT,
    value NUMERIC(10,2)
);

-- List tables only (simpler than \d)
\dt

-- Describe the specific table we created
\d test_table

-- Create an enum type
CREATE TYPE IF NOT EXISTS status_type AS ENUM ('active', 'inactive');

-- List user-defined types
\dT

-- Clean up
DROP TABLE IF EXISTS test_table;
DROP TYPE IF EXISTS status_type;