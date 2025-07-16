-- Test JSON operator translation in pgsqlite
-- These tests verify that PostgreSQL JSON operators are properly translated to SQLite functions

-- Create test table with JSON columns
CREATE TABLE test_json_ops (
    id INTEGER PRIMARY KEY,
    data JSON,
    config JSONB,
    metadata TEXT
);

-- Insert test data
INSERT INTO test_json_ops (id, data, config, metadata) VALUES 
(1, '{"name": "Alice", "age": 30, "email": "alice@example.com"}', '{"active": true, "role": "admin"}', 'user1'),
(2, '{"name": "Bob", "age": 25, "email": "bob@example.com"}', '{"active": false, "role": "user"}', 'user2'),
(3, '{"name": "Charlie", "age": 35, "skills": ["python", "rust", "go"]}', '{"active": true, "role": "developer"}', 'user3'),
(4, '{"items": [{"id": 1, "name": "Widget"}, {"id": 2, "name": "Gadget"}]}', '{"type": "order", "status": "pending"}', 'order1'),
(5, '{"nested": {"level1": {"level2": {"value": 42}}}}', '{"complex": true}', 'nested1');

-- Test ->> operator (extract text)
SELECT id, data->>'name' AS name FROM test_json_ops WHERE id <= 3;
SELECT id, data->>'email' AS email FROM test_json_ops WHERE data->>'age' = '30';
SELECT id, config->>'role' AS role FROM test_json_ops WHERE config->>'active' = 'true';

-- Test -> operator (extract JSON)
SELECT id, data->'skills' AS skills FROM test_json_ops WHERE id = 3;
SELECT id, data->'items'->0 AS first_item FROM test_json_ops WHERE id = 4;
SELECT id, data->'nested'->'level1' AS level1 FROM test_json_ops WHERE id = 5;

-- Test array index access
SELECT id, data->'skills'->>0 AS first_skill FROM test_json_ops WHERE id = 3;
SELECT id, data->'skills'->>1 AS second_skill FROM test_json_ops WHERE id = 3;
SELECT id, data->'items'->1->>'name' AS second_item_name FROM test_json_ops WHERE id = 4;

-- Test #> operator (path extraction as JSON)
SELECT id, data#>'{nested,level1,level2}' AS deep_value FROM test_json_ops WHERE id = 5;
SELECT id, data#>'{items,0}' AS first_item_path FROM test_json_ops WHERE id = 4;

-- Test #>> operator (path extraction as text)
SELECT id, data#>>'{nested,level1,level2,value}' AS deep_text_value FROM test_json_ops WHERE id = 5;
SELECT id, data#>>'{items,1,name}' AS second_item_name_path FROM test_json_ops WHERE id = 4;

-- Test @> operator (contains)
SELECT id, data FROM test_json_ops WHERE data @> '{"name": "Alice"}';
SELECT id, config FROM test_json_ops WHERE config @> '{"active": true}';
SELECT id, data FROM test_json_ops WHERE data @> '{"nested": {"level1": {}}}';

-- Test <@ operator (is contained by)
SELECT id FROM test_json_ops WHERE '{"name": "Bob"}' <@ data;
SELECT id FROM test_json_ops WHERE '{"role": "admin"}' <@ config;

-- Complex queries with multiple operators
SELECT 
    id,
    data->>'name' AS name,
    data->>'age' AS age,
    config->>'role' AS role,
    config->>'active' AS is_active
FROM test_json_ops 
WHERE data @> '{"age": 30}' OR config->>'role' = 'developer';

-- Nested operator usage
SELECT 
    id,
    data->'nested'->'level1'->>'level2' AS nested_path,
    data#>>'{nested,level1,level2,value}' AS path_value
FROM test_json_ops 
WHERE id = 5;

-- Clean up
DROP TABLE test_json_ops;