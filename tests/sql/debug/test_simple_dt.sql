-- Test just \dt which should work better than \d

-- Create a test table
CREATE TABLE IF NOT EXISTS test_table (
    id INTEGER PRIMARY KEY,
    name TEXT
);

-- List tables only
\dt

-- Clean up
DROP TABLE IF EXISTS test_table;