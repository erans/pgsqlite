-- Test direct trigger execution
DROP TABLE IF EXISTS test_mood;
DROP TYPE IF EXISTS mood;

-- Create the enum metadata manually
INSERT INTO __pgsqlite_enum_types (type_name, type_oid, namespace_oid, created_at) 
VALUES ('mood', 12345, 2200, datetime('now'));

INSERT INTO __pgsqlite_enum_values (type_name, enum_value, enum_label_oid, sort_order)
VALUES ('mood', 'happy', 1001, 1), ('mood', 'sad', 1002, 2);

-- Create table
CREATE TABLE test_mood (
    id INTEGER PRIMARY KEY,
    user_mood TEXT
);

-- Create trigger manually to test
CREATE TRIGGER test_trigger
BEFORE INSERT ON test_mood
FOR EACH ROW
WHEN NEW.user_mood IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM __pgsqlite_enum_values 
    WHERE type_name = 'mood' AND label = NEW.user_mood
)
BEGIN
    SELECT RAISE(ABORT, 'invalid enum value');
END;

-- Test insert
INSERT INTO test_mood (id, user_mood) VALUES (1, 'happy');