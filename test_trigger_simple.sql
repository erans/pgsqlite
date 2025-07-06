-- Simple test to check trigger-based validation
DROP TYPE IF EXISTS mood CASCADE;

-- Create ENUM type
CREATE TYPE mood AS ENUM ('happy', 'sad');

-- Check if metadata tables exist
SELECT 'Checking metadata tables:' as info;
SELECT name FROM sqlite_master WHERE name LIKE '__pgsqlite%' ORDER BY name;

-- Create table with ENUM
CREATE TABLE test_mood (
    id INTEGER PRIMARY KEY,
    user_mood mood
);

-- Check triggers
SELECT 'Checking triggers:' as info;
SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name;

-- Insert valid value
INSERT INTO test_mood (id, user_mood) VALUES (1, 'happy');

-- Show data
SELECT * FROM test_mood;