-- Column result conformance integration tests
-- The runner executes this through psql; ON_ERROR_STOP makes assertion failures abort.
\echo '=== Column conformance: F1 quoted paren column ==='
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 ("price(usd)" INT);
INSERT INTO t1 VALUES (5);
-- Header should be exactly price(usd), not sanitized to price.
SELECT "price(usd)" FROM t1;
SELECT "price(usd)" AS f1_value FROM t1 \gset
SELECT (:'f1_value' = '5') AS f1_ok \gset
\if :f1_ok
\else
SELECT pgsqlite_column_conformance_f1_value_failed;
\endif

\echo '=== Column conformance: F2 max(timestamp) formats as timestamp ==='
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (created_at TIMESTAMP);
INSERT INTO t2 VALUES ('2023-11-14 22:13:20');
-- Header should be max and value should be 2023-11-14 22:13:20, not raw microseconds.
SELECT max(created_at) FROM t2;
\unset max
SELECT max(created_at) FROM t2 \gset
SELECT (:'max' = '2023-11-14 22:13:20') AS f2_ok \gset
\if :f2_ok
\else
SELECT pgsqlite_column_conformance_f2_format_failed;
\endif

\echo '=== Column conformance: F5 alias count remains text ==='
DROP TABLE IF EXISTS t3;
CREATE TABLE t3 (name TEXT);
INSERT INTO t3 VALUES ('alice');
-- Header should be count and value should decode as text ALICE (extended/binary covered by unit tests).
SELECT upper(name) AS count FROM t3;
SELECT upper(name) AS f5_value FROM t3 \gset
SELECT (:'f5_value' = 'ALICE') AS f5_ok \gset
\if :f5_ok
\else
SELECT pgsqlite_column_conformance_f5_text_failed;
\endif

\echo '=== Column conformance: F4 unnamed expression ==='
-- Header should be ?column? and value should be 2.
SELECT 1+1;
SELECT 1+1 AS f4_value \gset
SELECT (:'f4_value' = '2') AS f4_ok \gset
\if :f4_ok
\else
SELECT pgsqlite_column_conformance_f4_value_failed;
\endif

\echo '=== Column conformance: F-new catalog count and F7 wildcard ==='
-- Header should be count; value should be at least public + pg_catalog namespaces.
SELECT count(*) FROM pg_catalog.pg_namespace;
\unset count
SELECT count(*) FROM pg_catalog.pg_namespace \gset
SELECT (:'count' = '2') AS fnew_ok \gset
\if :fnew_ok
\else
SELECT pgsqlite_column_conformance_catalog_count_failed;
\endif
-- Expected columns include nspname, oid, oid (mid-list wildcard must not drop/rename columns incorrectly).
SELECT *, oid FROM pg_catalog.pg_namespace;

\echo '=== Column conformance: C2 legacy_result_columns alias casing ==='
DROP TABLE IF EXISTS t4;
CREATE TABLE t4 (id INT);
INSERT INTO t4 VALUES (7);
SET pgsqlite.legacy_result_columns = on;
-- Smoke: SET accepts the dotted GUC. This psql/simple sync path still resolves columns
-- with conformant defaults, so resolver/unit tests cover legacy header behavior.
SELECT id AS MyAlias FROM t4;
SET pgsqlite.legacy_result_columns = off;
-- Legacy off/conformant default: unquoted alias casing is lower-case myalias.
SELECT id AS MyAlias FROM t4;
