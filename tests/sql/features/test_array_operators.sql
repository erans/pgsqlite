-- Test Array Operators and Functions in pgsqlite
-- This file tests the implementation of PostgreSQL array operators

-- Create test tables
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT,
    tags TEXT[],
    prices REAL[]
);

CREATE TABLE inventory (
    id INTEGER PRIMARY KEY,
    item_id INTEGER,
    quantities INTEGER[]
);

-- Insert test data
INSERT INTO products (id, name, tags, prices) VALUES 
(1, 'Laptop', '{"electronics", "computers", "portable"}', '{999.99, 1199.99, 1499.99}'),
(2, 'Phone', '{"electronics", "mobile", "portable"}', '{499.99, 699.99}'),
(3, 'Book', '{"education", "reading"}', '{19.99, 24.99}'),
(4, 'Tablet', '{"electronics", "portable", "mobile"}', '{299.99, 399.99, 499.99}');

INSERT INTO inventory (id, item_id, quantities) VALUES
(1, 1, '{5, 10, 15}'),
(2, 2, '{20, 25}'),
(3, 3, '{100}'),
(4, 4, '{8, 12, 16}');

-- Test array subscript access
SELECT name, tags[1] AS first_tag FROM products WHERE id = 1;
SELECT name, prices[2] AS mid_price FROM products WHERE id = 1;

-- Test array_length function
SELECT name, array_length(tags, 1) AS tag_count FROM products;
SELECT item_id, array_length(quantities, 1) AS qty_levels FROM inventory;

-- Test array_upper and array_lower
SELECT name, array_upper(tags, 1) AS upper_bound, array_lower(tags, 1) AS lower_bound FROM products WHERE id = 1;

-- Test ANY operator
SELECT name FROM products WHERE 'portable' = ANY(tags);
SELECT name FROM products WHERE 'mobile' = ANY(tags);

-- Test array contains operator @>
SELECT name FROM products WHERE tags @> '{"electronics", "portable"}';
SELECT name FROM products WHERE tags @> '{"education"}';

-- Test array is contained by operator <@
SELECT name FROM products WHERE tags <@ '{"electronics", "computers", "portable", "mobile", "education", "reading"}';

-- Test array overlap operator &&
SELECT name FROM products WHERE tags && '{"mobile", "desktop"}';
SELECT name FROM products WHERE tags && '{"reading", "writing"}';

-- Test array concatenation operator ||
SELECT name, tags || '{"on-sale"}' AS updated_tags FROM products WHERE id = 1;
SELECT item_id, quantities || '{0}' AS with_zero FROM inventory WHERE id = 1;

-- Test array_append function
SELECT name, array_append(tags, 'new') AS tags_with_new FROM products WHERE id = 1;
SELECT item_id, array_append(quantities, 50) AS quantities_plus FROM inventory WHERE id = 1;

-- Test array_prepend function
SELECT name, array_prepend('featured', tags) AS featured_tags FROM products WHERE id = 1;
SELECT item_id, array_prepend(0, quantities) AS zero_quantities FROM inventory WHERE id = 1;

-- Test array_cat function
SELECT array_cat(tags, '{"special", "offer"}') AS combined_tags FROM products WHERE id = 1;
SELECT array_cat(quantities, '{30, 40}') AS combined_qty FROM inventory WHERE id = 1;

-- Test array_remove function
SELECT name, array_remove(tags, 'portable') AS tags_no_portable FROM products WHERE id = 1;
SELECT item_id, array_remove(quantities, 10) AS qty_no_ten FROM inventory WHERE id = 1;

-- Test array_replace function
SELECT name, array_replace(tags, 'computers', 'laptops') AS updated_tags FROM products WHERE id = 1;
SELECT item_id, array_replace(quantities, 5, 6) AS updated_qty FROM inventory WHERE id = 1;

-- Test array_position function
SELECT name, array_position(tags, 'portable') AS portable_pos FROM products;
SELECT item_id, array_position(quantities, 10) AS ten_pos FROM inventory;

-- Test array_positions function
SELECT array_positions('{"a", "b", "c", "b", "d", "b"}', 'b') AS b_positions;
SELECT array_positions('{1, 2, 3, 2, 4, 2}', '2') AS two_positions;

-- Test ALL operator (simplified test)
-- Note: Full ALL operator support may need more complex implementation
SELECT name FROM products WHERE array_length(tags, 1) > 0;

-- Test array aggregation
SELECT array_agg(name ORDER BY name) AS all_products FROM products;
SELECT array_agg(DISTINCT tags[1]) AS unique_first_tags FROM products WHERE tags[1] IS NOT NULL;

-- Test array with NULL values
INSERT INTO products (id, name, tags, prices) VALUES 
(5, 'Test Product', '{"tag1", NULL, "tag3"}', '{10.0, NULL, 30.0}');

SELECT name, tags FROM products WHERE id = 5;
SELECT name, array_remove(tags, NULL) AS tags_no_null FROM products WHERE id = 5;

-- Test empty arrays
INSERT INTO products (id, name, tags, prices) VALUES 
(6, 'Empty Arrays', '{}', '{}');

SELECT name, array_length(tags, 1) AS empty_length FROM products WHERE id = 6;
SELECT name, array_append(tags, 'first') AS first_tag FROM products WHERE id = 6;

-- Clean up
DROP TABLE inventory;
DROP TABLE products;