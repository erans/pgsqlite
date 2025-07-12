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