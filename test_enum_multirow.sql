-- Test multi-row INSERT with ENUMs
CREATE TYPE mood AS ENUM ('happy', 'sad');

CREATE TABLE test_mood (
    id INTEGER PRIMARY KEY,
    user_mood mood
);

-- Multi-row insert
INSERT INTO test_mood (id, user_mood) VALUES 
    (1, 'happy'),
    (2, 'sad'),
    (3, 'happy');

SELECT * FROM test_mood;