-- Debug ENUM metadata
SELECT 'Checking __pgsqlite_enum_types...' as debug_msg;
SELECT * FROM __pgsqlite_enum_types;

SELECT 'Checking __pgsqlite_enum_values...' as debug_msg;
SELECT * FROM __pgsqlite_enum_values ORDER BY type_name, sort_order;

SELECT 'Checking sqlite_master for test_enums table...' as debug_msg;
SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'test_enums';

-- Try simple insert
SELECT 'Attempting simple insert...' as debug_msg;
INSERT INTO test_enums (id, user_mood, task_status, task_priority, description) 
VALUES (1, 'happy', 'pending', 'low', 'Test');