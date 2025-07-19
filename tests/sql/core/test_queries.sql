-- pgsqlite Comprehensive Test Suite
-- This file contains all supported query combinations for testing
-- Note: PREPARED statements are not included as they're not supported yet

-- ============================================
-- 1. SCHEMA OPERATIONS
-- ============================================

-- Drop tables if they exist (including ENUM test tables)
DROP TABLE IF EXISTS test_enum_complex;
DROP TABLE IF EXISTS test_enums;
DROP TABLE IF EXISTS test_arrays;
DROP TABLE IF EXISTS test_special_types;
DROP TABLE IF EXISTS test_numeric_types;
DROP TABLE IF EXISTS test_basic_types;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;

-- Drop ENUM types if they exist
DROP TYPE IF EXISTS mood;
DROP TYPE IF EXISTS status;
DROP TYPE IF EXISTS priority;

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
-- ENUM TYPE TESTING
-- ============================================

-- Create ENUM types
CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral', 'excited', 'angry');
CREATE TYPE status AS ENUM ('pending', 'processing', 'completed', 'cancelled');
CREATE TYPE priority AS ENUM ('low', 'medium', 'high', 'urgent');

-- Create table with ENUM columns
CREATE TABLE test_enums (
    id SERIAL PRIMARY KEY,
    user_mood mood,
    task_status status DEFAULT 'pending',
    task_priority priority NOT NULL DEFAULT 'medium',
    description TEXT
);

-- Create complex table mixing ENUMs with other types
CREATE TABLE test_enum_complex (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_status status NOT NULL DEFAULT 'pending',
    priority_level priority DEFAULT 'low',
    customer_mood mood,
    amount NUMERIC(10,2),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_urgent BOOLEAN DEFAULT false
);

-- Add new values to existing ENUM (ALTER TYPE)
-- NOTE: With trigger-based validation, ALTER TYPE ADD VALUE now works correctly!
ALTER TYPE mood ADD VALUE 'confused' AFTER 'neutral';
ALTER TYPE mood ADD VALUE 'hopeful' BEFORE 'happy';
ALTER TYPE status ADD VALUE 'on_hold' AFTER 'processing';

-- ============================================
-- 2. INSERT OPERATIONS
-- ============================================

-- Basic types insertion with comprehensive datetime examples
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
     '-1 year', NULL, NULL, NULL, NULL),
    ('Test DateTime', 'DateTime tests', 'DT_TEST', true,
     100, 1000, 10000,
     1.23, 4.56,
     '2024-12-25', '23:59:59', '2024-12-25 23:59:59', '2024-12-25 23:59:59+05',
     '6 months 15 days 8 hours', 'b1ffbc99-8c1b-4ef8-bb6d-6bb9bd380a22',
     '{"date": "2024-12-25"}', '{"timestamp": "2024-12-25T23:59:59Z"}', '\\x44617465'),
    ('Timezone Test', 'TZ examples', 'TZ_DATA', false,
     2025, 20250108, 20250108000000,
     -123.45, 678.90,
     CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, NOW(),
     '2 hours 30 minutes', 'c2ddbc99-7c2b-4ef8-bb6d-6bb9bd380a33',
     '{"current": true}', '{"now": true}', '\\x54696D65'),
    ('Edge Cases', 'Boundary values', 'EDGE_TEST', NULL,
     0, 0, 0,
     0.0, 0.0,
     '1970-01-01', '00:00:00.000001', '1970-01-01 00:00:00.000001', '1970-01-01 00:00:00.000001+00',
     '0 seconds', 'd3eebc99-6c3b-4ef8-bb6d-6bb9bd380a44',
     '{}', '{}', '\\x00');

-- Numeric types insertion
INSERT INTO test_numeric_types (numeric_col, numeric_precision, decimal_col, money_col) VALUES
    (12345.6789, 12345.67, 123456789.123, 1234.56),
    (9999999999999999999.999999999, 99999999.99, 999999999999.999, 99999999.99),
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

-- ENUM insertions
INSERT INTO test_enums (user_mood, task_status, task_priority, description) VALUES
    ('happy', 'pending', 'low', 'First task'),
    ('sad', 'processing', 'high', 'Urgent issue'),
    ('neutral', 'completed', 'medium', 'Regular work'),
    ('excited', 'cancelled', 'urgent', 'Changed plans'),
    ('angry', 'pending', 'high', 'Complaint'),
    ('confused', 'on_hold', 'medium', 'Needs clarification'),
    ('hopeful', 'processing', 'low', 'Future project'),
    (NULL, 'pending', 'medium', 'Mood not specified'),
    ('happy', 'pending', 'medium', 'Using defaults');

-- Complex ENUM table insertions with JOINs
INSERT INTO test_enum_complex (customer_id, order_status, priority_level, customer_mood, amount, notes, is_urgent)
SELECT 
    c.customer_id,
    CASE 
        WHEN o.total_amount > 1000 THEN 'completed'::status
        WHEN o.total_amount > 500 THEN 'processing'::status
        ELSE 'pending'::status
    END,
    CASE 
        WHEN o.total_amount > 1500 THEN 'urgent'::priority
        WHEN o.total_amount > 800 THEN 'high'::priority
        WHEN o.total_amount > 300 THEN 'medium'::priority
        ELSE 'low'::priority
    END,
    CASE (c.customer_id % 7)
        WHEN 0 THEN 'happy'::mood
        WHEN 1 THEN 'sad'::mood
        WHEN 2 THEN 'neutral'::mood
        WHEN 3 THEN 'excited'::mood
        WHEN 4 THEN 'angry'::mood
        WHEN 5 THEN 'confused'::mood
        ELSE 'hopeful'::mood
    END,
    o.total_amount,
    'Order from ' || c.name,
    o.total_amount > 1000
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id;

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

-- ============================================
-- DATETIME AND TIMEZONE COMPREHENSIVE TESTS
-- ============================================

-- PostgreSQL datetime functions and operations
SELECT 
    NOW() as current_timestamp,
    CURRENT_DATE as current_date,
    CURRENT_TIME as current_time,
    CURRENT_TIMESTAMP as current_timestamp_alt;

-- Date arithmetic and intervals
SELECT 
    CURRENT_DATE + INTERVAL '1 day' as tomorrow,
    CURRENT_DATE - INTERVAL '1 week' as last_week,
    CURRENT_TIMESTAMP + INTERVAL '2 hours 30 minutes' as later_today,
    CURRENT_TIMESTAMP - INTERVAL '1 year 3 months' as past_date;

-- Date extraction functions (basic examples)
SELECT 
    id,
    date_col,
    timestamp_col
FROM test_basic_types 
WHERE id = 1;

-- Timezone operations
SELECT 
    CURRENT_TIMESTAMP as utc_now,
    CURRENT_TIMESTAMP AT TIME ZONE 'UTC' as utc_explicit,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York' as new_york_time,
    CURRENT_TIMESTAMP AT TIME ZONE 'Europe/London' as london_time,
    CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Tokyo' as tokyo_time;

-- Date formatting and parsing (basic support)
SELECT 
    CAST(CURRENT_DATE AS TEXT) as date_as_text,
    CAST(CURRENT_TIMESTAMP AS TEXT) as timestamp_as_text,
    DATE('2025-12-25') as christmas_2025,
    '2025-01-01 12:00:00'::TIMESTAMP as new_year_noon;

-- Date differences and arithmetic
SELECT 
    CURRENT_DATE - '2000-01-01'::DATE as days_since_y2k,
    '2025-12-31'::DATE - CURRENT_DATE as days_until_end_of_year,
    CURRENT_DATE - '2024-01-01'::DATE as days_since_start_of_2024;

-- Complex datetime queries with table data
SELECT 
    id,
    date_col,
    timestamp_col,
    timestamptz_col,
    timestamp_col + INTERVAL '1 hour' as one_hour_later,
    timestamp_col - INTERVAL '30 minutes' as thirty_min_earlier
FROM test_basic_types 
WHERE timestamp_col IS NOT NULL;

-- Interval arithmetic and operations
SELECT 
    INTERVAL '1 year 2 months 3 days' as complex_interval,
    INTERVAL '1 year' + INTERVAL '6 months' as interval_addition,
    INTERVAL '2 hours' * 3 as interval_multiplication,
    INTERVAL '1 week' / 7 as one_day_interval;

-- Date range queries using datetime functions
SELECT 
    c.name,
    c.created_at,
    CURRENT_TIMESTAMP - c.created_at as time_since_created,
    CASE 
        WHEN c.created_at > CURRENT_TIMESTAMP - INTERVAL '1 week' THEN 'Recent'
        WHEN c.created_at > CURRENT_TIMESTAMP - INTERVAL '1 month' THEN 'This month'
        ELSE 'Older'
    END as recency_category
FROM customers c
WHERE c.created_at IS NOT NULL;

-- Time-based aggregations
SELECT 
    o.order_date,
    COUNT(*) as orders_count,
    SUM(o.total_amount) as daily_revenue,
    AVG(o.total_amount) as avg_order_value
FROM orders o
GROUP BY o.order_date
ORDER BY o.order_date;

-- Timezone conversion examples
SELECT 
    '2025-01-15 12:00:00'::TIMESTAMP as local_time,
    '2025-01-15 12:00:00'::TIMESTAMP AT TIME ZONE 'UTC' as utc_time,
    '2025-01-15 12:00:00+00'::TIMESTAMPTZ as timestamptz_input,
    '2025-01-15 12:00:00+00'::TIMESTAMPTZ AT TIME ZONE 'America/New_York' as ny_time,
    '2025-01-15 12:00:00+00'::TIMESTAMPTZ AT TIME ZONE 'Europe/Paris' as paris_time;

-- Complex datetime calculations with business logic
SELECT 
    o.order_id,
    o.order_date,
    CASE CAST(STRFTIME('%w', o.order_date) AS INTEGER)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_of_week,
    CASE 
        WHEN CAST(STRFTIME('%w', o.order_date) AS INTEGER) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    o.order_date + INTERVAL '30 days' as estimated_delivery
FROM orders o;

-- Date validation and edge cases
SELECT 
    '2025-02-28'::DATE as feb_28,
    '2025-02-28'::DATE + INTERVAL '1 day' as march_1,
    '2024-02-29'::DATE as leap_day_2024,
    '2025-12-31'::DATE + INTERVAL '1 day' as new_year_2026;

-- Working with different date formats
SELECT 
    CAST('2025-01-15' AS DATE) as iso_date_cast,
    CAST('2025-01-15 14:30:00' AS TIMESTAMP) as iso_timestamp_cast,
    CAST('2025-01-15 14:30:00+00' AS TIMESTAMPTZ) as iso_timestamptz_cast,
    '2025-01-15'::DATE as pg_date_cast,
    '2025-01-15 14:30:00'::TIMESTAMP as pg_timestamp_cast,
    '2025-01-15 14:30:00+00'::TIMESTAMPTZ as pg_timestamptz_cast;

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

-- Test array constructor syntax (Note: ARRAY constructor may not be fully supported)
-- INSERT INTO test_arrays (int_array, text_array, bool_array) VALUES
--     (ARRAY[10,20,30], ARRAY['hello', 'world'], ARRAY[true, true, false]),
--     (ARRAY[100], ARRAY['single'], ARRAY[false]);

-- Alternative: Insert using PostgreSQL array literal format
INSERT INTO test_arrays (int_array, text_array, bool_array) VALUES
    ('{10,20,30}', '{"hello","world"}', '{true,true,false}'),
    ('{100}', '{"single"}', '{false}');

-- Test multi-dimensional arrays
CREATE TABLE test_multidim_arrays (
    id SERIAL PRIMARY KEY,
    matrix_2d INTEGER[][],
    matrix_3d INTEGER[][][]
);

INSERT INTO test_multidim_arrays (matrix_2d, matrix_3d) VALUES
    ('{{1,2,3},{4,5,6}}', '{{{1,2},{3,4}},{{5,6},{7,8}}}'),
    ('{{10,20},{30,40}}', NULL);

-- Test array element access (Note: may not be fully supported)
-- SELECT int_array[1] AS first_element FROM test_arrays WHERE id = 1;
-- SELECT text_array[2] AS second_text FROM test_arrays WHERE id = 1;

-- Test array functions (Note: these may not be implemented yet)
-- SELECT array_length(int_array, 1) AS array_len FROM test_arrays;
-- SELECT array_upper(int_array, 1) AS upper_bound FROM test_arrays;
-- SELECT array_lower(int_array, 1) AS lower_bound FROM test_arrays;

-- Test array operators (these are implemented and working)
SELECT * FROM test_arrays WHERE int_array IS NOT NULL AND int_array @> '{2,3}';  -- contains
SELECT * FROM test_arrays WHERE int_array IS NOT NULL AND int_array <@ '{1,2,3,4,5,6}';  -- is contained by
SELECT * FROM test_arrays WHERE int_array IS NOT NULL AND int_array && '{3,4,5}';  -- overlaps

-- Test ANY/ALL operators (these are implemented and working)
SELECT * FROM test_arrays WHERE int_array IS NOT NULL AND 3 = ANY(int_array);
SELECT * FROM test_arrays WHERE int_array IS NOT NULL AND 10 > ALL(int_array);

-- Test array concatenation (implemented via || operator translation)
SELECT int_array || '{99}' AS concatenated FROM test_arrays WHERE id = 1;
SELECT '{0}' || int_array AS prepended FROM test_arrays WHERE id = 1;

-- Test empty arrays and NULL handling
SELECT * FROM test_arrays WHERE int_array = '{}';
SELECT * FROM test_arrays WHERE text_array IS NULL OR text_array = '{}';

-- Test array comparisons
SELECT * FROM test_arrays WHERE int_array = '{1,2,3,4,5}';
SELECT * FROM test_arrays WHERE text_array != '{}';

-- Clean up
DROP TABLE test_multidim_arrays;

-- ============================================
-- ENUM SELECT QUERIES
-- ============================================

-- Basic ENUM queries
SELECT * FROM test_enums;
SELECT * FROM test_enums WHERE user_mood = 'happy';
SELECT * FROM test_enums WHERE task_status IN ('pending', 'processing');
SELECT * FROM test_enums WHERE task_priority = 'high' OR task_priority = 'urgent';

-- ENUM with NULL handling
SELECT * FROM test_enums WHERE user_mood IS NULL;
SELECT * FROM test_enums WHERE user_mood IS NOT NULL;

-- ENUM ordering (alphabetical by default)
SELECT DISTINCT user_mood FROM test_enums WHERE user_mood IS NOT NULL ORDER BY user_mood;
SELECT DISTINCT task_priority FROM test_enums ORDER BY task_priority;

-- Complex queries with ENUMs
SELECT 
    e.customer_mood,
    COUNT(*) as mood_count,
    AVG(e.amount) as avg_amount,
    MIN(e.amount) as min_amount,
    MAX(e.amount) as max_amount
FROM test_enum_complex e
WHERE e.customer_mood IS NOT NULL
GROUP BY e.customer_mood
ORDER BY mood_count DESC;

-- JOIN with ENUM filtering
SELECT 
    c.name,
    e.order_status,
    e.priority_level,
    e.customer_mood,
    e.amount
FROM test_enum_complex e
JOIN customers c ON e.customer_id = c.customer_id
WHERE e.order_status = 'completed'
  AND e.priority_level IN ('high', 'urgent')
ORDER BY e.amount DESC;

-- ENUM in CASE expressions
SELECT 
    id,
    task_status,
    CASE task_status
        WHEN 'pending' THEN 'Not started'
        WHEN 'processing' THEN 'In progress'
        WHEN 'on_hold' THEN 'Paused'
        WHEN 'completed' THEN 'Done'
        WHEN 'cancelled' THEN 'Stopped'
        ELSE 'Unknown'
    END as status_description,
    CASE 
        WHEN task_priority IN ('urgent', 'high') THEN 'Critical'
        WHEN task_priority = 'medium' THEN 'Normal'
        ELSE 'Low priority'
    END as priority_category
FROM test_enums;

-- ENUM type casting
SELECT 
    'happy'::mood as casted_mood,
    CAST('pending' AS status) as casted_status,
    'high'::priority as casted_priority;

-- Complex aggregation with ENUMs
SELECT 
    order_status,
    priority_level,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(DISTINCT customer_id) as unique_customers
FROM test_enum_complex
GROUP BY order_status, priority_level
HAVING COUNT(*) > 1
ORDER BY order_status, priority_level;

-- Subquery with ENUMs
SELECT 
    c.name,
    (SELECT COUNT(*) 
     FROM test_enum_complex e 
     WHERE e.customer_id = c.customer_id 
       AND e.order_status = 'completed') as completed_orders,
    (SELECT COUNT(*) 
     FROM test_enum_complex e 
     WHERE e.customer_id = c.customer_id 
       AND e.priority_level IN ('high', 'urgent')) as high_priority_orders
FROM customers c
WHERE EXISTS (
    SELECT 1 
    FROM test_enum_complex e 
    WHERE e.customer_id = c.customer_id
);

-- CTE with ENUMs
WITH status_summary AS (
    SELECT 
        order_status,
        COUNT(*) as status_count,
        SUM(amount) as total_amount
    FROM test_enum_complex
    GROUP BY order_status
),
priority_summary AS (
    SELECT 
        priority_level,
        COUNT(*) as priority_count,
        AVG(amount) as avg_amount
    FROM test_enum_complex
    GROUP BY priority_level
)
SELECT 
    s.order_status,
    s.status_count,
    s.total_amount,
    p.priority_level,
    p.priority_count,
    p.avg_amount
FROM status_summary s
CROSS JOIN priority_summary p
WHERE s.status_count > 2 OR p.priority_count > 2;

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
-- ENUM UPDATE OPERATIONS
-- ============================================

-- Update ENUM columns
UPDATE test_enums SET user_mood = 'excited' WHERE id = 1;
UPDATE test_enums SET task_status = 'completed' WHERE task_status = 'processing';
UPDATE test_enums SET task_priority = 'urgent' WHERE id IN (2, 5);

-- Update ENUMs to NULL
UPDATE test_enums SET user_mood = NULL WHERE id = 8;

-- Update with CASE expression
UPDATE test_enums 
SET task_priority = CASE 
    WHEN task_status = 'cancelled' THEN 'low'
    WHEN task_status = 'completed' THEN 'medium'
    ELSE 'high'
END
WHERE task_priority != 'urgent';

-- Complex UPDATE with JOINs and ENUMs
UPDATE test_enum_complex
SET 
    order_status = 'completed',
    priority_level = 'low',
    customer_mood = 'happy'
WHERE customer_id IN (
    SELECT customer_id 
    FROM customers 
    WHERE name LIKE 'John%'
);

-- Update based on ENUM values
UPDATE test_enum_complex
SET amount = amount * 1.1
WHERE order_status = 'pending' 
  AND priority_level IN ('high', 'urgent');

-- Conditional ENUM updates
UPDATE test_enum_complex
SET customer_mood = CASE
    WHEN amount > 1500 THEN 'excited'
    WHEN amount > 1000 THEN 'happy'
    WHEN amount > 500 THEN 'neutral'
    ELSE 'sad'
END
WHERE customer_mood IS NOT NULL;

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
-- ENUM SYSTEM CATALOG QUERIES
-- ============================================

-- Query pg_type for ENUM types
SELECT 
    oid,
    typname,
    typnamespace,
    typtype,
    typcategory
FROM pg_catalog.pg_type
WHERE typtype = 'e'
ORDER BY typname;

-- Query pg_enum for ENUM values
SELECT 
    enumtypid,
    enumsortorder,
    enumlabel
FROM pg_catalog.pg_enum
ORDER BY enumtypid, enumsortorder;

-- Join pg_type and pg_enum to see ENUM types with their values
SELECT 
    t.typname as enum_type,
    e.enumlabel as enum_value,
    e.enumsortorder as sort_order
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
WHERE t.typtype = 'e'
ORDER BY t.typname, e.enumsortorder;

-- Find all columns using ENUM types
SELECT 
    c.relname as table_name,
    a.attname as column_name,
    t.typname as enum_type
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_attribute a ON c.oid = a.attrelid
JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
WHERE t.typtype = 'e'
  AND c.relkind = 'r'
  AND a.attnum > 0
ORDER BY c.relname, a.attname;

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
-- DATETIME PERFORMANCE AND OPTIMIZATION TESTS
-- ============================================

-- Test ultra-fast path with simple datetime queries (should bypass translation)
SELECT * FROM test_basic_types WHERE id = 1;
SELECT * FROM test_basic_types WHERE id = 2;
SELECT * FROM test_basic_types WHERE id = 3;

-- Simple datetime inserts (ultra-fast path candidates)
INSERT INTO test_basic_types (text_col, date_col) VALUES ('Fast Path Test 1', '2025-01-01');
INSERT INTO test_basic_types (text_col, date_col) VALUES ('Fast Path Test 2', '2025-01-02');
INSERT INTO test_basic_types (text_col, date_col) VALUES ('Fast Path Test 3', '2025-01-03');

-- Complex datetime queries (should use full translation pipeline)
SELECT COUNT(*) FROM test_basic_types WHERE date_col > CURRENT_DATE - INTERVAL '1 year';
SELECT id, timestamp_col::text FROM test_basic_types WHERE timestamp_col IS NOT NULL;
SELECT * FROM test_basic_types WHERE date_col >= '2025-01-01'::DATE;

-- Datetime aggregation performance test
SELECT 
    COUNT(*) as total_rows,
    MIN(date_col) as earliest_date,
    MAX(date_col) as latest_date,
    COUNT(DISTINCT timestamp_col) as unique_timestamps
FROM test_basic_types 
WHERE date_col IS NOT NULL;

-- Timezone conversion performance
SELECT 
    timestamp_col,
    timestamp_col AT TIME ZONE 'UTC' as utc_time,
    timestamp_col AT TIME ZONE 'America/New_York' as ny_time,
    timestamptz_col,
    timestamptz_col AT TIME ZONE 'Europe/London' as london_time
FROM test_basic_types 
WHERE timestamp_col IS NOT NULL
LIMIT 10;

-- Mixed datetime and arithmetic operations (tests type inference)
SELECT 
    id,
    date_col,
    timestamp_col,
    CURRENT_DATE - date_col as days_old,
    interval_col + INTERVAL '1 hour' as extended_interval,
    date_col + INTERVAL '1 week' as one_week_later
FROM test_basic_types 
WHERE date_col IS NOT NULL;

-- Batch datetime operations for performance testing
SELECT NOW(), CURRENT_DATE, CURRENT_TIME;
SELECT NOW() as base_time, NOW() + INTERVAL '10 seconds' as future_time;

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
-- ENUM ERROR CASES (commented out - will fail)
-- ============================================

-- Invalid ENUM value insertion (should fail with constraint violation)
-- INSERT INTO test_enums (user_mood, task_status, task_priority) VALUES ('invalid_mood', 'pending', 'medium');

-- Invalid ENUM value update (should fail)
-- UPDATE test_enums SET task_status = 'invalid_status' WHERE id = 1;

-- Invalid cast to ENUM (should fail)
-- SELECT 'not_a_mood'::mood;

-- Attempt to drop ENUM type still in use (should fail)
-- DROP TYPE mood;

-- Create duplicate ENUM type (should fail)
-- CREATE TYPE mood AS ENUM ('happy', 'sad');

-- Add duplicate ENUM value (should fail)
-- ALTER TYPE mood ADD VALUE 'happy';

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

-- End of test suite-- Test JSON operator translation in pgsqlite
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

-- Test JSON Functions
-- These tests verify PostgreSQL JSON functions work correctly

-- Test json_valid
SELECT json_valid('{"valid": true}') AS valid_json;
SELECT json_valid('{invalid}') AS invalid_json;
SELECT json_valid('null') AS null_valid;
SELECT json_valid('123') AS number_valid;

-- Test json_typeof / jsonb_typeof
SELECT json_typeof('123') AS number_type;
SELECT json_typeof('"text"') AS string_type;
SELECT json_typeof('{"a": 1}') AS object_type;
SELECT json_typeof('[1,2,3]') AS array_type;
SELECT json_typeof('true') AS bool_type;
SELECT json_typeof('null') AS null_type;
SELECT jsonb_typeof('{"test": "value"}') AS jsonb_object_type;

-- Test json_array_length / jsonb_array_length
SELECT json_array_length('[1, 2, 3, 4, 5]') AS five_elements;
SELECT json_array_length('[]') AS empty_array;
SELECT json_array_length('{"not": "array"}') AS not_array;
SELECT jsonb_array_length('[10, 20, 30]') AS jsonb_three;

-- Test jsonb_object_keys
SELECT jsonb_object_keys('{"name": "John", "age": 30, "city": "NYC"}') AS object_keys;
SELECT jsonb_object_keys('{}') AS empty_object_keys;

-- Test to_json / to_jsonb
SELECT to_json('hello world') AS text_to_json;
SELECT to_json(123) AS number_to_json;
SELECT to_json(NULL) AS null_to_json;
SELECT to_jsonb('test string') AS text_to_jsonb;

-- Test json_build_object
SELECT json_build_object('name', 'Alice') AS simple_object;
SELECT json_build_object('id', 42) AS number_object;

-- Test json_array_elements / jsonb_array_elements
SELECT json_array_elements('[1, 2, 3]') AS array_elements;
SELECT jsonb_array_elements('["a", "b", "c"]') AS jsonb_elements;

-- Test json_array_elements_text
SELECT json_array_elements_text('["hello", "world", "test"]') AS text_elements;
SELECT json_array_elements_text('[1, 2, 3]') AS number_text_elements;

-- Test json_strip_nulls / jsonb_strip_nulls
SELECT json_strip_nulls('{"a": 1, "b": null, "c": {"d": null, "e": 2}}') AS stripped_nulls;
SELECT jsonb_strip_nulls('{"x": null, "y": 10, "z": null}') AS jsonb_stripped;

-- Test jsonb_set
SELECT jsonb_set('{"a": 1, "b": 2}', '{b}', '99') AS set_value;
SELECT jsonb_set('{"a": {"b": 1}}', '{a,c}', '"new"') AS set_nested;
SELECT jsonb_set('[1, 2, 3]', '{1}', '42') AS set_array_element;

-- Test json_extract_path
SELECT json_extract_path('{"a": {"b": {"c": 42}}}', 'a.b.c') AS deep_path;
SELECT json_extract_path('{"name": "John", "age": 30}', 'name') AS simple_path;

-- Test json_extract_path_text
SELECT json_extract_path_text('{"user": {"name": "Alice", "id": 123}}', 'user.name') AS user_name;
SELECT json_extract_path_text('[10, 20, 30]', '1') AS array_element_text;

-- Test jsonb_contains / jsonb_contained
SELECT jsonb_contains('{"a": 1, "b": 2, "c": 3}', '{"a": 1}') AS contains_true;
SELECT jsonb_contains('{"a": 1}', '{"b": 2}') AS contains_false;
SELECT jsonb_contained('{"a": 1}', '{"a": 1, "b": 2}') AS contained_true;
SELECT jsonb_contained('{"a": 1, "b": 2}', '{"a": 1}') AS contained_false;

-- Complex JSON function tests with table data
CREATE TABLE json_func_test (
    id INTEGER PRIMARY KEY,
    data JSONB
);

INSERT INTO json_func_test VALUES
(1, '{"items": [{"name": "apple", "qty": 5}, {"name": "banana", "qty": 3}]}'),
(2, '{"user": {"name": "Bob", "prefs": {"theme": "dark", "lang": "en"}}}'),
(3, '[100, 200, 300, 400, 500]'),
(4, '{"stats": {"views": 1000, "likes": 50, "shares": null, "comments": null}}');

-- Test functions on table data
SELECT id, json_typeof(data) AS data_type FROM json_func_test;
SELECT id, json_array_length(data) AS array_len FROM json_func_test WHERE id = 3;
SELECT id, jsonb_object_keys(data) AS keys FROM json_func_test WHERE id IN (1, 2);
SELECT id, jsonb_strip_nulls(data) AS no_nulls FROM json_func_test WHERE id = 4;
SELECT id, json_extract_path_text(data, 'user.name') AS username FROM json_func_test WHERE id = 2;

-- Test jsonb_set on table data
UPDATE json_func_test 
SET data = jsonb_set(data, '{user,prefs,theme}', '"light"')
WHERE id = 2;

SELECT id, data FROM json_func_test WHERE id = 2;

-- Clean up
DROP TABLE json_func_test;

-- ===================================================================
-- JSON Aggregation and Row Conversion Function Tests
-- ===================================================================

-- Test json_agg and jsonb_agg
CREATE TABLE agg_test (
    id INTEGER,
    value TEXT,
    score INTEGER
);

INSERT INTO agg_test VALUES 
(1, 'apple', 85),
(2, 'banana', 92),
(3, 'orange', 78),
(4, 'grape', 90);

-- Test json_agg
SELECT json_agg(value) AS fruit_array FROM agg_test;
SELECT json_agg(score) AS score_array FROM agg_test WHERE score > 80;

-- Test jsonb_agg (should be identical to json_agg)
SELECT jsonb_agg(value) AS fruit_array_jsonb FROM agg_test;

-- Test json_object_agg and jsonb_object_agg
SELECT json_object_agg(value, score) AS fruit_scores FROM agg_test;
SELECT jsonb_object_agg(value, score) AS fruit_scores_jsonb FROM agg_test;

-- Test with mixed data types
SELECT json_object_agg(id, value) AS id_to_fruit FROM agg_test WHERE id <= 2;

-- Test empty result sets
SELECT json_agg(value) AS empty_array FROM agg_test WHERE id > 100;
SELECT json_object_agg(value, score) AS empty_object FROM agg_test WHERE id > 100;

-- Test row_to_json function
SELECT row_to_json(t) FROM (SELECT value, score FROM agg_test WHERE id = 1) t;
SELECT row_to_json(t) FROM (SELECT id, value, score FROM agg_test WHERE score > 85) t;

-- Test row_to_json with simple value
SELECT row_to_json('{"fruit_name": "banana", "rating": 92}') AS fruit_json;

-- Test row_to_json with simple query (using existing SQLite function)
SELECT row_to_json('{"name": "test", "score": 85}') AS simple_json;

-- Test json_each and json_each_text
SELECT key, value FROM json_each('{"name": "apple", "score": 85, "fresh": true}');
SELECT key, value FROM json_each_text('{"name": "apple", "score": 85, "fresh": true}');

-- Test jsonb_each and jsonb_each_text  
SELECT key, value FROM jsonb_each('{"user": "alice", "count": 42, "active": false}');
SELECT key, value FROM jsonb_each_text('{"user": "alice", "count": 42, "active": false}');

-- Test JSON manipulation functions
SELECT jsonb_insert('{"a": 1, "b": 2}', '{c}', '3') AS insert_test;
SELECT jsonb_delete('{"a": 1, "b": 2, "c": 3}', '{b}') AS delete_test;
SELECT jsonb_pretty('{"compact":{"json":["array","with","values"]}}') AS pretty_test;

-- Test JSON existence with json_extract
SELECT CASE WHEN json_extract('{"name": "test", "value": 123}', '$.name') IS NOT NULL THEN 1 ELSE 0 END AS has_name;
SELECT CASE WHEN json_extract('{"name": "test", "value": 123}', '$.missing') IS NOT NULL THEN 1 ELSE 0 END AS has_missing;

-- Test JSON record conversion functions
SELECT json_populate_record('null', '{"name": "Alice", "age": 30}') AS populate_test;
SELECT json_to_record('{"id": 1, "name": "Bob", "active": true}') AS record_test;
SELECT json_to_record('{"user": "Charlie", "score": 95, "verified": false}') AS complex_record;

-- Clean up aggregation test table
DROP TABLE agg_test;

-- ============================================
-- POSTGRESQL MATH FUNCTIONS COMPREHENSIVE TESTS
-- ============================================

-- Test basic rounding and truncation functions
SELECT trunc(3.7) AS trunc_positive, trunc(-3.7) AS trunc_negative;
SELECT trunc(3.789, 2) AS trunc_precision, trunc(1234.56789, 0) AS trunc_to_integer;
SELECT round(3.7) AS round_up, round(3.4) AS round_down, round(-3.7) AS round_negative;
SELECT round(3.789, 2) AS round_precision, round(1234.56789, 1) AS round_one_decimal;
SELECT ceil(3.2) AS ceil_positive, ceil(-3.7) AS ceil_negative, ceiling(4.1) AS ceiling_alias;
SELECT floor(3.7) AS floor_positive, floor(-3.2) AS floor_negative;

-- Test sign and absolute value functions
SELECT sign(5.5) AS sign_positive, sign(-3.2) AS sign_negative, sign(0) AS sign_zero;
SELECT abs(-123.45) AS abs_negative, abs(67.89) AS abs_positive, abs(0) AS abs_zero;

-- Test modulo function
SELECT mod(10, 3) AS mod_basic, mod(15, 4) AS mod_remainder, mod(-7, 3) AS mod_negative;

-- Test power and square root functions
SELECT power(2, 3) AS power_basic, pow(5, 2) AS pow_alias, power(2.5, 3.5) AS power_float;
SELECT sqrt(16) AS sqrt_perfect, sqrt(2) AS sqrt_irrational, sqrt(0.25) AS sqrt_decimal;

-- Test exponential and logarithmic functions
SELECT exp(1) AS exp_e, exp(0) AS exp_zero, exp(2) AS exp_two;
SELECT ln(exp(1)) AS ln_e_roundtrip, ln(10) AS ln_ten;
SELECT log(100) AS log10_hundred, log(1000) AS log10_thousand, log(10, 100) AS log_custom_base;

-- Test trigonometric functions
SELECT sin(0) AS sin_zero, sin(pi()/2) AS sin_pi_half, sin(pi()) AS sin_pi;
SELECT cos(0) AS cos_zero, cos(pi()/2) AS cos_pi_half, cos(pi()) AS cos_pi;
SELECT tan(0) AS tan_zero, tan(pi()/4) AS tan_pi_quarter;

-- Test inverse trigonometric functions
SELECT asin(0) AS asin_zero, asin(1) AS asin_one, acos(1) AS acos_one, acos(0) AS acos_zero;
SELECT atan(0) AS atan_zero, atan(1) AS atan_one, atan2(1, 1) AS atan2_45_degrees;

-- Test angle conversion functions
SELECT radians(180) AS pi_radians, radians(90) AS half_pi_radians;
SELECT degrees(pi()) AS degrees_180, degrees(pi()/2) AS degrees_90;

-- Test pi constant
SELECT pi() AS pi_value, 2 * pi() AS two_pi, pi() / 2 AS half_pi;

-- Test random function (should return values between 0 and 1)
SELECT random() AS random1, random() AS random2, random() AS random3;

-- Create table for math function testing with real data
CREATE TABLE math_test_data (
    id SERIAL PRIMARY KEY,
    value DECIMAL(10,3),
    angle DECIMAL(8,6)
);

INSERT INTO math_test_data (value, angle) VALUES 
(123.456, 0.0),
(-45.678, 1.570796),  -- π/2
(0.0, 3.141593),      -- π
(999.999, 0.785398),  -- π/4
(-0.001, 6.283185);   -- 2π

-- Test math functions on table data
SELECT 
    id,
    value,
    trunc(value) AS truncated,
    round(value, 1) AS rounded,
    abs(value) AS absolute,
    sign(value) AS sign_value,
    sqrt(abs(value)) AS square_root,
    power(abs(value), 0.5) AS power_half
FROM math_test_data;

-- Test trigonometric functions on table data
SELECT 
    id,
    angle,
    sin(angle) AS sine,
    cos(angle) AS cosine,
    tan(angle) AS tangent,
    degrees(angle) AS angle_degrees,
    radians(degrees(angle)) AS roundtrip_angle
FROM math_test_data;

-- Test complex math expressions
SELECT 
    id,
    value,
    sqrt(power(value, 2) + power(value * 0.5, 2)) AS hypotenuse,
    ln(exp(abs(value))) AS ln_exp_roundtrip,
    power(10, log(abs(value))) AS power_log_roundtrip
FROM math_test_data
WHERE value != 0;

-- Test math functions in aggregations
SELECT 
    COUNT(*) AS total_rows,
    AVG(value) AS avg_value,
    trunc(AVG(value), 2) AS avg_truncated,
    SUM(abs(value)) AS sum_absolute,
    sqrt(SUM(power(value, 2))) AS euclidean_norm,
    MIN(sin(angle)) AS min_sine,
    MAX(cos(angle)) AS max_cosine
FROM math_test_data;

-- Test math functions in WHERE clauses
SELECT * FROM math_test_data WHERE abs(value) > 50;
SELECT * FROM math_test_data WHERE sin(angle) > 0.5;
SELECT * FROM math_test_data WHERE power(value, 2) < 10000;

-- Test math functions in ORDER BY
SELECT id, value, sqrt(abs(value)) AS sqrt_abs 
FROM math_test_data 
ORDER BY sqrt(abs(value)) DESC;

-- Test edge cases and error handling
-- Note: Division by zero tests intentionally omitted as they cause SQLite errors
-- These would be: SELECT mod(10, 0) and SELECT 1/0

-- Clean up math test table
DROP TABLE math_test_data;

-- Test math functions with numeric precision
CREATE TABLE precision_test (
    id SERIAL PRIMARY KEY,
    precise_value NUMERIC(15,8)
);

INSERT INTO precision_test (precise_value) VALUES 
(3.14159265),
(2.71828182),
(1.41421356),
(1.73205080);

-- Test precision preservation in math functions
SELECT 
    id,
    precise_value,
    trunc(precise_value, 6) AS truncated_6,
    round(precise_value, 4) AS rounded_4,
    power(precise_value, 2) AS squared,
    sqrt(precise_value) AS square_root,
    ln(precise_value) AS natural_log
FROM precision_test;

-- Clean up precision test table  
DROP TABLE precision_test;

-- ============================================
-- POSTGRESQL STRING FUNCTIONS COMPREHENSIVE TESTS
-- ============================================

-- Test split_part function
SELECT split_part('apple,banana,cherry', ',', 1) AS first_part;
SELECT split_part('apple,banana,cherry', ',', 2) AS second_part;
SELECT split_part('apple,banana,cherry', ',', 3) AS third_part;
SELECT split_part('apple,banana,cherry', ',', 4) AS beyond_parts;
SELECT split_part('one|two|three|four', '|', 2) AS pipe_delimiter;
SELECT split_part('no-delimiter', ',', 1) AS no_delimiter_found;

-- Test string_agg function
CREATE TABLE string_agg_test (
    id INTEGER,
    category TEXT,
    value TEXT
);

INSERT INTO string_agg_test VALUES 
(1, 'fruits', 'apple'),
(2, 'fruits', 'banana'),
(3, 'fruits', 'cherry'),
(4, 'colors', 'red'),
(5, 'colors', 'blue'),
(6, 'colors', 'green');

SELECT category, string_agg(value, ', ') AS aggregated 
FROM string_agg_test 
GROUP BY category;

SELECT string_agg(value, ' | ') AS all_values FROM string_agg_test;

-- Test translate function
SELECT translate('Hello World', 'lo', 'xy') AS translated_basic;
SELECT translate('abcdef', 'ace', 'xyz') AS translate_multiple;
SELECT translate('Hello123', '123', 'ABC') AS translate_numbers;
SELECT translate('TEST', 'EST', '') AS translate_remove_chars;

-- Test ascii and chr functions
SELECT ascii('A') AS ascii_A, ascii('a') AS ascii_a, ascii('0') AS ascii_zero;
SELECT chr(65) AS chr_A, chr(97) AS chr_a, chr(48) AS chr_zero;
SELECT chr(ascii('Z')) AS roundtrip_Z;

-- Test repeat function
SELECT repeat('ha', 3) AS repeat_ha;
SELECT repeat('*', 5) AS repeat_stars;
SELECT repeat('test', 0) AS repeat_zero;
SELECT repeat('X', 1) AS repeat_once;

-- Test reverse function
SELECT reverse('hello') AS reverse_hello;
SELECT reverse('12345') AS reverse_numbers;
SELECT reverse('') AS reverse_empty;
SELECT reverse('a') AS reverse_single;

-- Test left and right functions
SELECT left('PostgreSQL', 4) AS left_four;
SELECT left('Hello World', 5) AS left_hello;
SELECT right('PostgreSQL', 3) AS right_three;
SELECT right('Hello World', 5) AS right_world;

-- Test lpad and rpad functions
SELECT lpad('123', 5, '0') AS lpad_zeros;
SELECT lpad('test', 8, '*') AS lpad_stars;
SELECT rpad('123', 5, '0') AS rpad_zeros;
SELECT rpad('test', 8, '*') AS rpad_stars;
SELECT lpad('toolong', 4, '0') AS lpad_truncate;
SELECT rpad('toolong', 4, '0') AS rpad_truncate;

-- Create table for string function testing with real data
CREATE TABLE string_test_data (
    id SERIAL PRIMARY KEY,
    full_name TEXT,
    email TEXT,
    phone TEXT,
    description TEXT
);

INSERT INTO string_test_data (full_name, email, phone, description) VALUES 
('John Smith', 'john.smith@example.com', '555-123-4567', 'Senior Developer'),
('Jane Doe', 'jane.doe@company.org', '555-987-6543', 'Project Manager'),
('Bob Wilson', 'bob@tech.net', '555-555-5555', 'Database Administrator'),
('Alice Brown', 'alice.brown@startup.io', '555-111-2222', 'Frontend Specialist');

-- Test string functions on table data
SELECT 
    id,
    full_name,
    split_part(full_name, ' ', 1) AS first_name,
    split_part(full_name, ' ', 2) AS last_name,
    left(full_name, 3) AS name_prefix,
    reverse(full_name) AS reversed_name,
    ascii(left(full_name, 1)) AS first_char_ascii
FROM string_test_data;

-- Test email processing with string functions
SELECT 
    id,
    email,
    split_part(email, '@', 1) AS username,
    split_part(email, '@', 2) AS domain,
    translate(email, '.@', '__') AS safe_filename,
    lpad(split_part(email, '@', 1), 10, '*') AS padded_username
FROM string_test_data;

-- Test phone number formatting
SELECT 
    id,
    phone,
    translate(phone, '-', '') AS no_dashes,
    left(phone, 3) AS area_code,
    right(phone, 4) AS last_four,
    repeat('*', 7) || right(phone, 4) AS masked_phone
FROM string_test_data;

-- Test string aggregation by category
SELECT string_agg(full_name, '|') AS all_names FROM string_test_data;
SELECT string_agg(description, ' ') AS all_roles FROM string_test_data;

-- Test complex string manipulations
SELECT 
    id,
    full_name,
    description,
    left(description, 10) || '...' AS truncated_desc,
    translate(upper(description), ' ', '_') AS identifier,
    chr(ascii(left(full_name, 1)) + 32) AS lowercase_initial,
    repeat(left(full_name, 1), 3) AS triple_initial
FROM string_test_data;

-- Test string functions in WHERE clauses
SELECT * FROM string_test_data WHERE ascii(left(full_name, 1)) BETWEEN 65 AND 90;
SELECT * FROM string_test_data WHERE split_part(email, '@', 2) LIKE '%.com';
SELECT * FROM string_test_data WHERE left(description, 5) = 'Senio';

-- Test string functions in ORDER BY
SELECT full_name, description 
FROM string_test_data 
ORDER BY reverse(full_name);

-- Clean up string test tables
DROP TABLE string_agg_test;
DROP TABLE string_test_data;