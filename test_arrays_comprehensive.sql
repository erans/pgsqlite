-- Comprehensive Array Test Suite for pgsqlite
-- Tests various array types and operations

-- Test 1: Create table with different array types
CREATE TABLE array_demo (
    id INTEGER PRIMARY KEY,
    int_arr INTEGER[],
    text_arr TEXT[],
    bool_arr BOOLEAN[],
    float_arr REAL[]
);

-- Test 2: Insert arrays using PostgreSQL format
INSERT INTO array_demo (id, int_arr, text_arr, bool_arr, float_arr) VALUES 
    (1, '{1,2,3}', '{apple,banana,cherry}', '{true,false,true}', '{1.1,2.2,3.3}'),
    (2, '{4,5,6}', '{dog,cat,bird}', '{false,false,true}', '{4.4,5.5,6.6}');

-- Test 3: Insert with quoted text
INSERT INTO array_demo (id, text_arr) VALUES 
    (3, '{"hello world","foo bar","test string"}');

-- Test 4: Insert empty and NULL arrays
INSERT INTO array_demo (id, int_arr, text_arr) VALUES 
    (4, '{}', '{}'),
    (5, NULL, NULL);

-- Test 5: Insert arrays with NULL elements
INSERT INTO array_demo (id, int_arr, text_arr) VALUES 
    (6, '{1,NULL,3}', '{first,NULL,third}');

-- Test 6: Query all data
SELECT * FROM array_demo ORDER BY id;

-- Test 7: Check array metadata
SELECT 
    column_name, 
    element_type, 
    dimensions 
FROM __pgsqlite_array_types 
WHERE table_name = 'array_demo'
ORDER BY column_name;

-- Test 8: Verify pg_type array support
SELECT 
    typname, 
    oid 
FROM pg_type 
WHERE typname IN ('_int4', '_text', '_bool', '_float4')
ORDER BY oid;

-- Clean up
DROP TABLE array_demo;

-- Summary
SELECT 'Array comprehensive test completed successfully' as result;