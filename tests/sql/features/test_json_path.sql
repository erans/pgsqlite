-- Test JSON path operators that contain $ characters
CREATE TABLE test_json (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO test_json VALUES (1, '{"items":[{"name":"item1"},{"name":"item2"}],"user":{"name":"Alice"}}');

-- Test #> operator with array path (this should work now)
SELECT id, data#>'{items,0}' AS first_item FROM test_json WHERE id = 1;

-- Test #>> operator with array path
SELECT id, data#>>'{items,1,name}' AS second_item_name FROM test_json WHERE id = 1;

-- Test -> and ->> operators
SELECT id, data->>'user' AS user_info FROM test_json WHERE id = 1;
SELECT id, data->'items'->>0 AS first_item_text FROM test_json WHERE id = 1;