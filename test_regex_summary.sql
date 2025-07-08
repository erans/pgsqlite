-- Summary of PostgreSQL Regex Operator Support in pgsqlite

-- Create test data
CREATE TABLE IF NOT EXISTS regex_test (
    id INTEGER PRIMARY KEY,
    text_value TEXT
);

INSERT INTO regex_test (id, text_value) VALUES 
    (1, 'PostgreSQL'),
    (2, 'postgresql'),
    (3, 'MySQL'),
    (4, 'SQLite'),
    (5, 'pg_catalog'),
    (6, 'pg_toast_123'),
    (7, 'user_table');

-- Test 1: Case-sensitive match (~)
SELECT '=== Test 1: ~ operator (case-sensitive match) ===' AS test;
SELECT text_value FROM regex_test WHERE text_value ~ '^Post';
-- Expected: PostgreSQL

-- Test 2: Case-sensitive NOT match (!~)
SELECT '=== Test 2: !~ operator (case-sensitive NOT match) ===' AS test;
SELECT text_value FROM regex_test WHERE text_value !~ '^pg_';
-- Expected: PostgreSQL, postgresql, MySQL, SQLite, user_table

-- Test 3: Case-insensitive match (~*)
SELECT '=== Test 3: ~* operator (case-insensitive match) ===' AS test;
SELECT text_value FROM regex_test WHERE text_value ~* '^post';
-- Expected: PostgreSQL, postgresql

-- Test 4: Case-insensitive NOT match (!~*)
SELECT '=== Test 4: !~* operator (case-insensitive NOT match) ===' AS test;
SELECT text_value FROM regex_test WHERE text_value !~* 'sql';
-- Expected: pg_catalog, pg_toast_123, user_table

-- Test 5: Complex patterns
SELECT '=== Test 5: Complex regex patterns ===' AS test;
SELECT text_value FROM regex_test WHERE text_value ~ '(SQL|pg_)';
-- Expected: PostgreSQL, MySQL, SQLite, pg_catalog, pg_toast_123

-- Clean up
DROP TABLE regex_test;

-- Note: psql meta-commands like \d still fail due to:
-- 1. Missing pg_namespace and pg_am catalog tables
-- 2. No support for JOINs in catalog query handlers
-- 3. OPERATOR(pg_catalog.~) syntax not yet handled
-- But direct regex operators now work perfectly!