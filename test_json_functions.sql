-- Test JSON functions in pgsqlite
-- These tests verify that PostgreSQL JSON functions are properly implemented

-- Create test table with JSON columns
CREATE TABLE test_json_funcs (
    id INTEGER PRIMARY KEY,
    data JSON,
    config JSONB,
    metadata TEXT
);

-- Insert test data
INSERT INTO test_json_funcs (id, data, config, metadata) VALUES 
(1, '{"name": "Alice", "age": 30, "email": "alice@example.com"}', '{"active": true, "role": "admin"}', 'user1'),
(2, '{"name": "Bob", "age": 25, "email": "bob@example.com"}', '{"active": false, "role": "user"}', 'user2'),
(3, '{"name": "Charlie", "age": 35, "skills": ["python", "rust", "go"]}', '{"active": true, "role": "developer"}', 'user3'),
(4, '{"items": [{"id": 1, "name": "Widget"}, {"id": 2, "name": "Gadget"}]}', '{"type": "order", "status": "pending"}', 'order1'),
(5, '{"nested": {"level1": {"level2": {"value": 42}}}}', '{"complex": true}', 'nested1'),
(6, '[1, 2, 3, 4, 5]', '["a", "b", "c"]', 'array1'),
(7, 'null', 'null', 'null_value'),
(8, '123', '"text"', 'scalar1'),
(9, 'true', 'false', 'boolean1'),
(10, '{"a": 1, "b": null, "c": {"d": null, "e": 2}}', '{"x": null, "y": [1, null, 3]}', 'nulls1');

-- Test json_valid function
SELECT 'json_valid tests:' AS test_group;
SELECT json_valid(data) AS is_valid, data FROM test_json_funcs WHERE id <= 5;
SELECT json_valid('{"invalid": }') AS should_be_false;
SELECT json_valid('null') AS should_be_true;
SELECT json_valid('[1, 2, 3]') AS should_be_true;

-- Test json_typeof and jsonb_typeof functions
SELECT 'json_typeof tests:' AS test_group;
SELECT id, json_typeof(data) AS data_type, jsonb_typeof(config) AS config_type FROM test_json_funcs WHERE id IN (1, 6, 7, 8, 9);
SELECT json_typeof('{}') AS empty_object_type;
SELECT json_typeof('[]') AS empty_array_type;
SELECT json_typeof('null') AS null_type;
SELECT json_typeof('123') AS number_type;
SELECT json_typeof('"string"') AS string_type;
SELECT json_typeof('true') AS boolean_type;

-- Test json_array_length and jsonb_array_length functions
SELECT 'json_array_length tests:' AS test_group;
SELECT id, json_array_length(data) AS data_length FROM test_json_funcs WHERE id = 6;
SELECT jsonb_array_length(config) AS config_length FROM test_json_funcs WHERE id = 6;
SELECT json_array_length('[]') AS empty_array_length;
SELECT json_array_length('[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]') AS ten_elements;
SELECT json_array_length(data->'skills') AS skills_length FROM test_json_funcs WHERE id = 3;

-- Test jsonb_object_keys function
SELECT 'jsonb_object_keys tests:' AS test_group;
SELECT id, jsonb_object_keys(data) AS keys FROM test_json_funcs WHERE id = 1;
SELECT jsonb_object_keys(data) AS keys FROM test_json_funcs WHERE id = 5;
SELECT jsonb_object_keys('{}') AS empty_object_keys;
SELECT jsonb_object_keys('{"a": 1, "b": 2, "c": 3}') AS three_keys;

-- Test to_json and to_jsonb functions
SELECT 'to_json/to_jsonb tests:' AS test_group;
SELECT to_json('hello world') AS text_to_json;
SELECT to_json(123) AS number_to_json;
SELECT to_json(NULL) AS null_to_json;
SELECT to_jsonb(true) AS bool_to_jsonb;
SELECT to_jsonb(3.14159) AS float_to_jsonb;

-- Test json_build_object function (2-argument version)
SELECT 'json_build_object tests:' AS test_group;
SELECT json_build_object('name', 'John') AS simple_object;
SELECT json_build_object('age', 30) AS number_value;
SELECT json_build_object('active', true) AS bool_value;
SELECT json_build_object('data', NULL) AS null_value;

-- Test json_extract_path and json_extract_path_text functions
SELECT 'json_extract_path tests:' AS test_group;
SELECT id, json_extract_path(data, 'name') AS name FROM test_json_funcs WHERE id <= 3;
SELECT id, json_extract_path_text(data, 'age') AS age_text FROM test_json_funcs WHERE id <= 3;
SELECT json_extract_path(data, 'nested') AS nested_obj FROM test_json_funcs WHERE id = 5;
SELECT json_extract_path_text(data, 'nested') AS nested_text FROM test_json_funcs WHERE id = 5;
SELECT json_extract_path('{"a": {"b": {"c": 42}}}', 'a') AS path_a;

-- Test json_extract_scalar function
SELECT 'json_extract_scalar tests:' AS test_group;
SELECT id, json_extract_scalar(data, 'email') AS email FROM test_json_funcs WHERE id <= 2;
SELECT json_extract_scalar(data, 'age') AS age FROM test_json_funcs WHERE id = 1;
SELECT json_extract_scalar(data, 'missing') AS missing_key FROM test_json_funcs WHERE id = 1;

-- Test jsonb_contains and jsonb_contained functions
SELECT 'jsonb_contains/contained tests:' AS test_group;
SELECT id FROM test_json_funcs WHERE jsonb_contains(data, '{"name": "Alice"}');
SELECT id FROM test_json_funcs WHERE jsonb_contains(config, '{"active": true}');
SELECT jsonb_contains('{"a": 1, "b": 2, "c": 3}', '{"a": 1, "b": 2}') AS should_be_true;
SELECT jsonb_contains('{"a": 1, "b": 2}', '{"a": 1, "b": 2, "c": 3}') AS should_be_false;
SELECT jsonb_contained('{"a": 1}', '{"a": 1, "b": 2}') AS should_be_true;
SELECT jsonb_contained('{"a": 1, "b": 2}', '{"a": 1}') AS should_be_false;

-- Test json_array_elements and jsonb_array_elements functions
SELECT 'json_array_elements tests:' AS test_group;
SELECT id, json_array_elements(data) AS elements FROM test_json_funcs WHERE id = 6;
SELECT jsonb_array_elements('[1, 2, 3]') AS simple_array;
SELECT json_array_elements('["a", "b", "c"]') AS string_array;
SELECT json_array_elements(data->'skills') AS skills FROM test_json_funcs WHERE id = 3;

-- Test json_array_elements_text function
SELECT 'json_array_elements_text tests:' AS test_group;
SELECT id, json_array_elements_text(config) AS config_elements FROM test_json_funcs WHERE id = 6;
SELECT json_array_elements_text('["hello", "world", "test"]') AS text_elements;
SELECT json_array_elements_text('[1, 2, 3]') AS number_elements_as_text;

-- Test json_strip_nulls and jsonb_strip_nulls functions
SELECT 'json_strip_nulls tests:' AS test_group;
SELECT id, json_strip_nulls(data) AS stripped_data FROM test_json_funcs WHERE id = 10;
SELECT jsonb_strip_nulls(config) AS stripped_config FROM test_json_funcs WHERE id = 10;
SELECT json_strip_nulls('{"a": 1, "b": null, "c": 3}') AS simple_strip;
SELECT json_strip_nulls('{"a": {"b": null, "c": 2}, "d": null}') AS nested_strip;
SELECT json_strip_nulls('[1, null, 3, null, 5]') AS array_strip;

-- Test jsonb_set function (3-argument version)
SELECT 'jsonb_set tests:' AS test_group;
SELECT jsonb_set('{"a": 1, "b": 2}', '{c}', '3') AS add_key;
SELECT jsonb_set('{"a": 1, "b": 2}', '{a}', '10') AS update_key;
SELECT jsonb_set('{"a": {"b": 2}}', '{a,c}', '"new"') AS nested_add;
SELECT jsonb_set('[1, 2, 3]', '{1}', '20') AS array_update;
SELECT jsonb_set(data, '{age}', '40') AS updated_age FROM test_json_funcs WHERE id = 1;

-- Complex queries combining multiple JSON functions
SELECT 'complex json function tests:' AS test_group;
SELECT 
    id,
    json_typeof(data) AS type,
    CASE 
        WHEN json_typeof(data) = 'object' THEN jsonb_object_keys(data)
        WHEN json_typeof(data) = 'array' THEN json_array_length(data)::text
        ELSE 'N/A'
    END AS type_info
FROM test_json_funcs 
WHERE id IN (1, 6);

-- Test with WHERE clauses using JSON functions
SELECT id, data 
FROM test_json_funcs 
WHERE json_typeof(data) = 'object' 
  AND jsonb_contains(data, '{"name": "Charlie"}');

SELECT id, config 
FROM test_json_funcs 
WHERE jsonb_contains(config, '{"active": true}')
  AND json_typeof(config) = 'object';

-- Test JSON functions with NULL values
SELECT 'null handling tests:' AS test_group;
SELECT json_valid(NULL) AS null_valid;
SELECT json_typeof(NULL) AS null_type;
SELECT json_array_length(NULL) AS null_length;
SELECT jsonb_object_keys(NULL) AS null_keys;
SELECT to_json(NULL) AS null_to_json;
SELECT json_strip_nulls(NULL) AS strip_null;

-- Test edge cases
SELECT 'edge case tests:' AS test_group;
SELECT json_typeof('') AS empty_string_type;
SELECT json_array_length('{"not": "array"}') AS object_array_length;
SELECT jsonb_object_keys('[1, 2, 3]') AS array_object_keys;
SELECT json_extract_path('{"a": [1, 2, 3]}', 'a') AS array_extract;
SELECT jsonb_contains('[]', '[]') AS empty_array_contains;
SELECT jsonb_contains('{}', '{}') AS empty_object_contains;

-- ===================================================================
-- JSON Aggregation and Advanced Functions (2025-07-16)
-- ===================================================================

-- Test json_agg aggregation function
SELECT 'json_agg tests:' AS test_group;
SELECT json_agg(metadata) AS all_metadata FROM test_json_funcs;
SELECT json_agg(id) AS active_ids FROM test_json_funcs WHERE id <= 3;

-- Test jsonb_agg aggregation function
SELECT 'jsonb_agg tests:' AS test_group;
SELECT jsonb_agg(metadata) AS all_metadata_jsonb FROM test_json_funcs;

-- Test json_object_agg aggregation function
SELECT 'json_object_agg tests:' AS test_group;
SELECT json_object_agg(id, metadata) AS id_to_metadata FROM test_json_funcs WHERE id <= 3;
SELECT json_object_agg(metadata, id) AS metadata_to_id FROM test_json_funcs WHERE id <= 2;

-- Test jsonb_object_agg aggregation function
SELECT 'jsonb_object_agg tests:' AS test_group;
SELECT jsonb_object_agg(id, metadata) AS id_to_metadata_jsonb FROM test_json_funcs WHERE id <= 3;

-- Test row_to_json function
SELECT 'row_to_json tests:' AS test_group;
SELECT row_to_json(t) FROM (SELECT id, metadata FROM test_json_funcs WHERE id = 1) t;
SELECT row_to_json(user_info) FROM (
    SELECT id AS user_id, 
           metadata AS username,
           data->>'name' AS full_name
    FROM test_json_funcs 
    WHERE id <= 2
) user_info;

-- Test json_each and json_each_text table functions
SELECT 'json_each tests:' AS test_group;
SELECT key, value FROM json_each('{"name": "test", "count": 42, "active": true}');
SELECT key, value FROM json_each_text('{"name": "test", "count": 42, "active": true}');

-- Test jsonb_each and jsonb_each_text table functions  
SELECT 'jsonb_each tests:' AS test_group;
SELECT key, value FROM jsonb_each('{"type": "example", "value": 123, "flag": false}');
SELECT key, value FROM jsonb_each_text('{"type": "example", "value": 123, "flag": false}');

-- Test JSON manipulation functions
SELECT 'json manipulation tests:' AS test_group;
SELECT jsonb_insert('{"a": 1, "b": 2}', '{c}', '3') AS insert_simple;
SELECT jsonb_insert('{"users": [{"id": 1}]}', '{users,1}', '{"id": 2, "name": "Bob"}') AS insert_array;
SELECT jsonb_delete('{"a": 1, "b": 2, "c": 3}', '{b}') AS delete_key;
SELECT jsonb_delete('{"arr": [1, 2, 3, 4]}', '{arr,1}') AS delete_array_element;
SELECT jsonb_pretty('{"compact":{"data":["item1","item2","item3"]}}') AS pretty_formatted;

-- Test JSON existence operators
SELECT 'json existence tests:' AS test_group;
SELECT config ? 'active' AS has_active FROM test_json_funcs WHERE id = 1;
SELECT data ? 'nonexistent' AS has_missing FROM test_json_funcs WHERE id = 1;
SELECT data ?| ARRAY['name', 'missing'] AS has_any_keys FROM test_json_funcs WHERE id = 1;
SELECT data ?& ARRAY['name', 'age'] AS has_all_keys FROM test_json_funcs WHERE id = 1;

-- Test empty aggregation results
SELECT 'empty result tests:' AS test_group;
SELECT json_agg(metadata) AS empty_agg FROM test_json_funcs WHERE id > 100;
SELECT json_object_agg(id, metadata) AS empty_obj_agg FROM test_json_funcs WHERE id > 100;

-- Test JSON record conversion functions
SELECT 'json record conversion tests:' AS test_group;
SELECT json_populate_record('null', '{"name": "David", "age": 28, "department": "Engineering"}') AS populate_employee;
SELECT json_populate_record('', '{"product": "Widget", "price": 29.99, "in_stock": true}') AS populate_product;
SELECT json_to_record('{"order_id": 12345, "customer": "Emma", "total": 199.50}') AS order_record;
SELECT json_to_record('{"session": "abc123", "user_id": 789, "authenticated": true, "expires": null}') AS session_record;
SELECT json_to_record('{}') AS empty_record;
SELECT json_to_record('[{"invalid": "array"}]') AS invalid_record;

-- Clean up
DROP TABLE test_json_funcs;