-- Simple ENUM test
DROP TYPE IF EXISTS test_mood;
CREATE TYPE test_mood AS ENUM ('happy', 'sad');

CREATE TABLE test_enum_table (
    id INTEGER PRIMARY KEY,
    mood test_mood
);

INSERT INTO test_enum_table (id, mood) VALUES (1, 'happy');
INSERT INTO test_enum_table (id, mood) VALUES (2, 'sad');

SELECT * FROM test_enum_table;