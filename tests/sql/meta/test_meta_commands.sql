-- Test file for psql meta-commands
-- This file contains various psql meta-commands to test catalog query functionality
-- Run with: psql -h localhost -p 5432 -d test.db -f test_meta_commands.sql

-- =============================================================================
-- DATABASE AND SCHEMA COMMANDS
-- =============================================================================

-- List all databases
-- Tests: pg_database catalog query
\l

-- List all schemas
-- Tests: pg_namespace catalog query
\dn

-- Show current database
-- Tests: current_database() function
\conninfo

-- =============================================================================
-- TABLE COMMANDS
-- =============================================================================

-- List all tables in current schema
-- Tests: pg_class catalog query with relkind='r'
\dt

-- List all tables in all schemas
-- Tests: pg_class with pg_namespace join
\dt *.*

-- List tables matching pattern
-- Tests: pg_class with LIKE pattern matching
\dt test*

-- Describe specific table structure
-- Tests: pg_attribute, pg_type, pg_class joins
\d test_table

-- Describe table with more details (indexes, constraints, etc)
-- Tests: Additional catalog joins for constraints and indexes
\d+ test_table

-- =============================================================================
-- VIEW COMMANDS
-- =============================================================================

-- List all views
-- Tests: pg_class with relkind='v'
\dv

-- List views matching pattern
-- Tests: pg_class with relkind='v' and pattern matching
\dv *_view

-- =============================================================================
-- INDEX COMMANDS
-- =============================================================================

-- List all indexes
-- Tests: pg_class with relkind='i'
\di

-- List indexes for specific table
-- Tests: pg_index catalog query
\di test_table*

-- =============================================================================
-- SEQUENCE COMMANDS
-- =============================================================================

-- List all sequences
-- Tests: pg_class with relkind='S'
\ds

-- =============================================================================
-- TYPE COMMANDS
-- =============================================================================

-- List all data types
-- Tests: pg_type catalog query
\dT

-- List user-defined types
-- Tests: pg_type with typtype filter
\dT+

-- List enum types specifically
-- Tests: pg_type with typtype='e'
\dT *.enum_*

-- =============================================================================
-- FUNCTION COMMANDS
-- =============================================================================

-- List all functions
-- Tests: pg_proc catalog query
\df

-- List functions matching pattern
-- Tests: pg_proc with pattern matching
\df *date*

-- List aggregate functions
-- Tests: pg_proc with proisagg=true
\da

-- =============================================================================
-- PERMISSION COMMANDS
-- =============================================================================

-- List table privileges
-- Tests: pg_class with ACL parsing
\dp

-- List default privileges
-- Tests: pg_default_acl catalog query
\ddp

-- =============================================================================
-- SETTINGS AND CONFIGURATION
-- =============================================================================

-- Show all settings
-- Tests: pg_settings view
\dconfig

-- Show specific setting
-- Tests: pg_settings with filter
\dconfig *timezone*

-- =============================================================================
-- EXTENDED DISPLAY COMMANDS
-- =============================================================================

-- List all relations (tables, views, sequences, etc)
-- Tests: pg_class with multiple relkind values
\d

-- List all relations with size information
-- Tests: pg_class with pg_stat_user_tables
\d+

-- List all relations matching pattern
-- Tests: Pattern matching across all relation types
\d test*

-- =============================================================================
-- SPECIAL COMMANDS
-- =============================================================================

-- Show SQL of last query
-- Tests: Query echo functionality
\p

-- Execute last query again
-- Tests: Query buffer functionality
\g

-- Show query buffer
-- Tests: Internal query buffer
\e

-- =============================================================================
-- INTROSPECTION COMMANDS
-- =============================================================================

-- List all casts
-- Tests: pg_cast catalog query
\dC

-- List all operators
-- Tests: pg_operator catalog query
\do

-- List all operator classes
-- Tests: pg_opclass catalog query
\dAc

-- List all operator families
-- Tests: pg_opfamily catalog query
\dAf

-- =============================================================================
-- ERROR CASES TO TEST
-- =============================================================================

-- Try to describe non-existent table (should show appropriate error)
\d nonexistent_table

-- Try to list tables in non-existent schema
\dt fake_schema.*

-- =============================================================================
-- COMBINED/COMPLEX QUERIES
-- =============================================================================

-- List tables with their sizes
-- Tests: Complex join with pg_stat_user_tables
\dt+

-- List all objects owned by current user
-- Tests: Ownership filtering across multiple catalogs
\do

-- Show table definition as CREATE TABLE statement
-- Tests: Reverse engineering from catalog data
\d test_table