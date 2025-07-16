-- Test regex operators directly

-- Create a test table
CREATE TABLE IF NOT EXISTS test_regex (
    id INTEGER PRIMARY KEY,
    name TEXT
);

-- Insert test data
INSERT INTO test_regex (id, name) VALUES 
    (1, 'pg_catalog'),
    (2, 'pg_toast_123'),
    (3, 'public_table'),
    (4, 'my_table');

-- Test regex match operator (~)
SELECT name FROM test_regex WHERE name ~ '^pg_';

-- Test regex NOT match operator (!~)
SELECT name FROM test_regex WHERE name !~ '^pg_';

-- Test case-insensitive match (~*)
SELECT name FROM test_regex WHERE name ~* 'PUBLIC';

-- Test case-insensitive NOT match (!~*)
SELECT name FROM test_regex WHERE name !~* 'PUBLIC';

-- Clean up
DROP TABLE test_regex;