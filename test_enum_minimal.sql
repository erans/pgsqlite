-- Minimal ENUM test
-- Step 1: Create ENUM type
CREATE TYPE mood AS ENUM ('happy', 'sad');

-- Step 2: Create table with ENUM column
CREATE TABLE test_mood (
    id INTEGER PRIMARY KEY,
    user_mood mood
);

-- Step 3: Check metadata tables
SELECT '__pgsqlite_enum_types:' as table_name;
SELECT * FROM __pgsqlite_enum_types;

SELECT '__pgsqlite_enum_values:' as table_name;
SELECT * FROM __pgsqlite_enum_values;

-- Step 4: Check table structure
SELECT 'sqlite_master for test_mood:' as info;
SELECT sql FROM sqlite_master WHERE name = 'test_mood';

-- Step 5: Try insert
INSERT INTO test_mood (id, user_mood) VALUES (1, 'happy');