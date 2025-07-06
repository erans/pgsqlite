-- Test ALTER TYPE ADD VALUE with trigger-based validation
DROP TYPE IF EXISTS mood CASCADE;

-- Create initial ENUM type
CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');

-- Create table with ENUM column
CREATE TABLE test_mood (
    id INTEGER PRIMARY KEY,
    user_mood mood
);

-- Insert initial values
INSERT INTO test_mood (id, user_mood) VALUES 
    (1, 'happy'),
    (2, 'sad'),
    (3, 'neutral');

-- Show current data
SELECT 'Initial data:' as info;
SELECT * FROM test_mood ORDER BY id;

-- Add new values to the ENUM
ALTER TYPE mood ADD VALUE 'confused' AFTER 'neutral';
ALTER TYPE mood ADD VALUE 'hopeful' BEFORE 'happy';

-- Insert new values that were just added
INSERT INTO test_mood (id, user_mood) VALUES 
    (4, 'confused'),
    (5, 'hopeful');

-- Show all data including new values
SELECT 'After ALTER TYPE:' as info;
SELECT * FROM test_mood ORDER BY id;

-- Test that invalid values still fail
-- This should fail: INSERT INTO test_mood (id, user_mood) VALUES (6, 'invalid');

-- Check the metadata
SELECT 'ENUM metadata:' as info;
SELECT ev.label, ev.sort_order 
FROM __pgsqlite_enum_values ev
JOIN __pgsqlite_enum_types et ON ev.type_oid = et.type_oid
WHERE et.type_name = 'mood' 
ORDER BY ev.sort_order;