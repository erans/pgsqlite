-- Test that invalid ENUM values are rejected
DROP TYPE IF EXISTS mood CASCADE;
CREATE TYPE mood AS ENUM ('happy', 'sad');

CREATE TABLE test_mood (
    id INTEGER PRIMARY KEY,
    user_mood mood
);

-- Valid insert
INSERT INTO test_mood (id, user_mood) VALUES (1, 'happy');

-- This should fail with proper error message
INSERT INTO test_mood (id, user_mood) VALUES (2, 'invalid');