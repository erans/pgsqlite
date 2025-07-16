-- Safe psql meta-command tests
-- Only the commands that definitely work

-- Create test tables first
CREATE TABLE IF NOT EXISTS meta_test_table (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    value DECIMAL(10,2)
);

-- Create an enum type
CREATE TYPE IF NOT EXISTS test_enum AS ENUM ('active', 'inactive');

-- Describe a specific table (this should work with our pg_get_userbyid fix)
\d meta_test_table

-- List user-defined types (without pattern matching)
SELECT typname FROM pg_catalog.pg_type WHERE typtype = 'e' ORDER BY typname;

-- Show the enum values
SELECT enumlabel FROM pg_catalog.pg_enum 
WHERE enumtypid = (SELECT oid FROM pg_catalog.pg_type WHERE typname = 'test_enum') 
ORDER BY enumsortorder;

-- Direct catalog queries that should work
SELECT relname AS "Table Name" FROM pg_catalog.pg_class WHERE relkind = 'r' ORDER BY relname;

-- Clean up
DROP TABLE IF EXISTS meta_test_table;
DROP TYPE IF EXISTS test_enum;