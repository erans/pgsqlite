-- pgsqlite Comprehensive Test Suite
-- This file contains all supported query combinations for testing
-- Note: PREPARED statements are not included as they're not supported yet

-- ============================================
-- 1. SCHEMA OPERATIONS
-- ============================================

-- Drop tables if they exist
DROP TABLE IF EXISTS test_arrays;
DROP TABLE IF EXISTS test_special_types;
DROP TABLE IF EXISTS test_numeric_types;
DROP TABLE IF EXISTS test_basic_types;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;

-- Create basic types table
CREATE TABLE test_basic_types (
    id SERIAL PRIMARY KEY,
    text_col TEXT,
    varchar_col VARCHAR(100),
    char_col CHAR(10),
    bool_col BOOLEAN,
    int2_col SMALLINT,
    int4_col INTEGER,
    int8_col BIGINT,
    float4_col REAL,
    float8_col DOUBLE PRECISION,
    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMPTZ,
    interval_col INTERVAL,
    uuid_col UUID,
    json_col JSON,
    jsonb_col JSONB,
    bytea_col BYTEA
);

-- Create numeric types table
CREATE TABLE test_numeric_types (
    id SERIAL PRIMARY KEY,
    numeric_col NUMERIC,
    numeric_precision NUMERIC(10,2),
    decimal_col DECIMAL(15,3),
    money_col MONEY
);

-- Create special types table
CREATE TABLE test_special_types (
    id SERIAL PRIMARY KEY,
    inet_col INET,
    cidr_col CIDR,
    macaddr_col MACADDR,
    bit_col BIT(8),
    varbit_col BIT VARYING(16),
    int4range_col INT4RANGE,
    int8range_col INT8RANGE,
    numrange_col NUMRANGE,
    tsrange_col TSRANGE,
    tstzrange_col TSTZRANGE,
    daterange_col DATERANGE
);

-- Create arrays table
CREATE TABLE test_arrays (
    id SERIAL PRIMARY KEY,
    int_array INTEGER[],
    text_array TEXT[],
    bool_array BOOLEAN[]
);

-- Create tables for JOIN testing
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price NUMERIC(10,2),
    stock_quantity INTEGER DEFAULT 0
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    order_date DATE DEFAULT CURRENT_DATE,
    total_amount NUMERIC(10,2)
);

-- Create indexes
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_product ON orders(product_id);
CREATE INDEX idx_orders_date ON orders(order_date);

-- ============================================
-- 2. INSERT OPERATIONS
-- ============================================

-- Basic types insertion
INSERT INTO test_basic_types (
    text_col, varchar_col, char_col, bool_col,
    int2_col, int4_col, int8_col,
    float4_col, float8_col,
    date_col, time_col, timestamp_col, timestamptz_col,
    interval_col, uuid_col, json_col, jsonb_col, bytea_col
) VALUES 
    ('Hello World', 'Variable text', 'Fixed', true,
     32767, 2147483647, 9223372036854775807,
     3.14159, 2.718281828,
     '2025-01-15', '14:30:00', '2025-01-15 14:30:00', '2025-01-15 14:30:00+00',
     '1 year 2 months 3 days', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
     '{"key": "value"}', '{"key": "value"}', '\\x48656C6C6F'),
    (NULL, NULL, NULL, false,
     -32768, -2147483648, -9223372036854775808,
     -3.14159, -2.718281828,
     '1900-01-01', '00:00:00', '1900-01-01 00:00:00', '1900-01-01 00:00:00+00',
     '-1 year', NULL, NULL, NULL, NULL);

-- Numeric types insertion
INSERT INTO test_numeric_types (numeric_col, numeric_precision, decimal_col, money_col) VALUES
    (12345.6789, 12345.67, 123456789.123, 1234.56),
    (99999999999999999999.9999999999, 99999999.99, 999999999999.999, 99999999.99),
    (-12345.6789, -12345.67, -123456789.123, -1234.56);

-- Special types insertion
INSERT INTO test_special_types (
    inet_col, cidr_col, macaddr_col,
    bit_col, varbit_col,
    int4range_col, int8range_col, numrange_col,
    tsrange_col, tstzrange_col, daterange_col
) VALUES
    ('192.168.1.1', '10.0.0.0/8', '08:00:2b:01:02:03',
     '10101010', '1111000011110000',
     '[1,10)', '[100,200)', '[1.5,2.5)',
     '[2025-01-01 00:00:00,2025-12-31 23:59:59]',
     '[2025-01-01 00:00:00+00,2025-12-31 23:59:59+00)',
     '[2025-01-01,2025-12-31)'),
    ('::1', '2001:db8::/32', 'FF:FF:FF:FF:FF:FF',
     '11111111', '0000111100001111',
     '(1,10]', '(100,200]', '(1.5,2.5]',
     '(,)', '(,)', '(,)');

-- Arrays insertion
INSERT INTO test_arrays (int_array, text_array, bool_array) VALUES
    ('{1,2,3,4,5}', '{"apple","banana","cherry"}', '{true,false,true}'),
    ('{}', '{}', '{}'),
    (NULL, NULL, NULL);

-- Customer data
INSERT INTO customers (name, email) VALUES
    ('John Doe', 'john.doe@example.com'),
    ('Jane Smith', 'jane.smith@example.com'),
    ('Bob Johnson', 'bob.johnson@example.com'),
    ('Alice Williams', 'alice.williams@example.com'),
    ('Charlie Brown', 'charlie.brown@example.com');

-- Product data
INSERT INTO products (name, price, stock_quantity) VALUES
    ('Laptop', 999.99, 50),
    ('Mouse', 29.99, 200),
    ('Keyboard', 79.99, 150),
    ('Monitor', 299.99, 75),
    ('Headphones', 149.99, 100);

-- Order data with multi-row insert (testing batch performance)
INSERT INTO orders (customer_id, product_id, quantity, order_date, total_amount) VALUES
    (1, 1, 1, '2025-01-01', 999.99),
    (1, 2, 2, '2025-01-01', 59.98),
    (2, 3, 1, '2025-01-02', 79.99),
    (2, 4, 1, '2025-01-02', 299.99),
    (3, 5, 3, '2025-01-03', 449.97),
    (4, 1, 2, '2025-01-04', 1999.98),
    (5, 2, 5, '2025-01-05', 149.95),
    (5, 3, 2, '2025-01-05', 159.98),
    (1, 4, 1, '2025-01-06', 299.99),
    (2, 5, 1, '2025-01-07', 149.99);

-- ============================================
-- 3. SELECT QUERIES
-- ============================================

-- Basic SELECT
SELECT * FROM test_basic_types;
SELECT id, text_col, bool_col FROM test_basic_types WHERE bool_col = true;
SELECT COUNT(*) FROM test_basic_types;

-- Numeric operations with mixed types (implicit casting)
SELECT 
    numeric_col,
    numeric_col + 100 as addition,  -- NUMERIC + INTEGER
    numeric_col * 2 as multiplication,  -- NUMERIC * INTEGER
    numeric_col / 3 as division,  -- NUMERIC / INTEGER
    ROUND(numeric_col, 2) as rounded,
    ABS(numeric_col) as absolute,
    numeric_col + 10.5 as decimal_addition,  -- NUMERIC + FLOAT
    numeric_col || ' units' as numeric_to_text  -- NUMERIC to TEXT
FROM test_numeric_types;

-- Aggregate functions
SELECT 
    COUNT(*) as total_orders,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value,
    MIN(total_amount) as min_order,
    MAX(total_amount) as max_order
FROM orders;

-- GROUP BY with HAVING
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as customer_total
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY customer_total DESC;

-- JOINs
SELECT 
    c.name as customer_name,
    p.name as product_name,
    o.quantity,
    o.total_amount
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN products p ON o.product_id = p.product_id
ORDER BY o.order_date;

-- LEFT JOIN
SELECT 
    c.name,
    c.email,
    COUNT(o.order_id) as order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email
ORDER BY order_count DESC;

-- Subqueries
SELECT name, email
FROM customers
WHERE customer_id IN (
    SELECT customer_id 
    FROM orders 
    GROUP BY customer_id 
    HAVING SUM(total_amount) > 500
);

-- Correlated subquery
SELECT 
    p.name,
    p.price,
    (SELECT SUM(quantity) FROM orders o WHERE o.product_id = p.product_id) as total_sold
FROM products p;

-- CTE (Common Table Expression)
WITH customer_stats AS (
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(total_amount) as total_spent
    FROM orders
    GROUP BY customer_id
)
SELECT 
    c.name,
    cs.order_count,
    cs.total_spent
FROM customers c
JOIN customer_stats cs ON c.customer_id = cs.customer_id
WHERE cs.total_spent > 400;

-- Window functions (if supported)
SELECT 
    customer_id,
    order_date,
    total_amount,
    SUM(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date) as running_total
FROM orders;

-- String operations
SELECT 
    text_col,
    LENGTH(text_col) as text_length,
    UPPER(text_col) as uppercase,
    LOWER(text_col) as lowercase,
    SUBSTR(text_col, 1, 5) as substring_result
FROM test_basic_types
WHERE text_col IS NOT NULL;

-- Date operations
SELECT 
    date_col,
    STRFTIME('%Y', date_col) as year,
    STRFTIME('%m', date_col) as month,
    STRFTIME('%d', date_col) as day,
    DATE(date_col, '+1 month') as next_month
FROM test_basic_types
WHERE date_col IS NOT NULL;

-- Type casting (explicit)
SELECT 
    CAST('123' AS INTEGER) as int_cast,
    CAST(123 AS TEXT) as text_cast,
    CAST('123.45' AS NUMERIC) as numeric_cast,
    CAST('456' AS INTEGER) as pg_cast_int,
    CAST(456 AS TEXT) as pg_cast_text;

-- Implicit type casting in expressions
SELECT 
    '123' + 456 as implicit_int_addition,  -- string to int
    123 || ' text' as implicit_text_concat,  -- int to text
    '3.14' * 2 as implicit_float_mult,  -- string to float
    true + 1 as implicit_bool_to_int,  -- boolean to int
    '2025-01-15' < CURRENT_DATE as implicit_date_comparison;  -- string to date

-- Special types queries
SELECT * FROM test_special_types;
-- Network containment operator (may not be supported)
-- SELECT inet_col, cidr_col FROM test_special_types WHERE inet_col << '192.168.0.0/16'::CIDR;

-- Array queries (basic support)
SELECT * FROM test_arrays WHERE int_array IS NOT NULL;

-- ============================================
-- 4. UPDATE OPERATIONS
-- ============================================

-- Simple UPDATE
UPDATE products SET stock_quantity = stock_quantity - 10 WHERE product_id = 1;

-- UPDATE with subquery
UPDATE products 
SET stock_quantity = stock_quantity - (
    SELECT SUM(quantity) 
    FROM orders 
    WHERE orders.product_id = products.product_id
)
WHERE product_id IN (SELECT DISTINCT product_id FROM orders);

-- UPDATE multiple columns
UPDATE test_basic_types 
SET 
    text_col = 'Updated text',
    bool_col = NOT bool_col,
    timestamp_col = CURRENT_TIMESTAMP
WHERE id = 1;

-- ============================================
-- 5. DELETE OPERATIONS
-- ============================================

-- Simple DELETE
DELETE FROM test_arrays WHERE id = 3;

-- DELETE with WHERE clause
DELETE FROM orders WHERE order_date < '2025-01-03';

-- DELETE with subquery
DELETE FROM orders 
WHERE customer_id IN (
    SELECT customer_id 
    FROM customers 
    WHERE email LIKE '%example.com'
);

-- ============================================
-- 6. TRANSACTION OPERATIONS
-- ============================================

-- Transaction test
BEGIN;
INSERT INTO products (name, price, stock_quantity) VALUES ('Test Product', 99.99, 10);
SELECT * FROM products WHERE name = 'Test Product';
ROLLBACK;

-- Verify rollback
SELECT * FROM products WHERE name = 'Test Product';

-- Successful transaction
BEGIN;
UPDATE products SET price = price * 1.1 WHERE product_id <= 3;
COMMIT;

-- ============================================
-- 7. SYSTEM CATALOG QUERIES
-- ============================================

-- Query pg_class (tables and indexes)
SELECT 
    oid,
    relname,
    relnamespace,
    reltype,
    relkind,
    relnatts
FROM pg_catalog.pg_class
WHERE relkind IN ('r', 'i')
ORDER BY relname;

-- Query pg_attribute (columns)
SELECT 
    attrelid,
    attname,
    atttypid,
    attlen,
    attnum,
    attnotnull
FROM pg_catalog.pg_attribute
WHERE attrelid IN (
    SELECT oid FROM pg_catalog.pg_class WHERE relname = 'customers'
)
AND attnum > 0
ORDER BY attnum;

-- ============================================
-- 8. PERFORMANCE QUERIES
-- ============================================

-- Test query plan cache effectiveness
SELECT * FROM customers WHERE customer_id = 1;
SELECT * FROM customers WHERE customer_id = 2;
SELECT * FROM customers WHERE customer_id = 3;

-- Test prepared statement performance (parameterized queries)
SELECT * FROM products WHERE product_id = 1;
SELECT * FROM products WHERE product_id = 2;
SELECT * FROM products WHERE product_id = 3;

-- ============================================
-- 9. EDGE CASES AND ERROR HANDLING
-- ============================================

-- Division by zero (commented out as it will cause an error)
-- SELECT 1/0;

-- NULL handling
SELECT NULL + 5, NULL || 'text', COALESCE(NULL, 'default');

-- Empty result sets
SELECT * FROM customers WHERE customer_id = 99999;

-- Complex expressions
SELECT 
    CASE 
        WHEN price < 50 THEN 'Budget'
        WHEN price < 200 THEN 'Mid-range'
        ELSE 'Premium'
    END as price_category,
    COUNT(*) as product_count
FROM products
GROUP BY price_category;

-- ============================================
-- 10. CLEANUP (Optional - comment out if needed)
-- ============================================

-- Final statistics
SELECT 
    'customers' as table_name, COUNT(*) as row_count FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'test_basic_types', COUNT(*) FROM test_basic_types
UNION ALL
SELECT 'test_numeric_types', COUNT(*) FROM test_numeric_types
UNION ALL
SELECT 'test_special_types', COUNT(*) FROM test_special_types
UNION ALL
SELECT 'test_arrays', COUNT(*) FROM test_arrays;

-- End of test suite