-- Debug trigger SQL
DROP TYPE IF EXISTS mood CASCADE;
CREATE TYPE mood AS ENUM ('happy', 'sad');

CREATE TABLE test_mood (
    id INTEGER PRIMARY KEY,
    user_mood mood
);

-- Show the actual trigger SQL
SELECT 'Trigger SQL:' as info;
SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name LIKE '%test_mood%';