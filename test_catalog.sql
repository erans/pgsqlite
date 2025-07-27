-- Test catalog views
SELECT COUNT(*) FROM pg_class;
SELECT COUNT(*) FROM pg_namespace;
SELECT oid, relname, relkind FROM pg_class LIMIT 5;
SELECT oid, nspname FROM pg_namespace;