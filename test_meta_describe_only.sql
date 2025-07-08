-- Test only the \d tablename command which we fixed

-- Create a simple test table
CREATE TABLE test_describe (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    value INTEGER
);

-- This should work with our pg_get_userbyid fix
\d test_describe

-- Clean up
DROP TABLE test_describe;