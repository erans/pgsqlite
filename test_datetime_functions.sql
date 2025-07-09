-- Test datetime functions to verify they return proper formatted strings
SELECT NOW();
SELECT CURRENT_TIMESTAMP;
SELECT NOW() AS my_timestamp;
SELECT CURRENT_TIMESTAMP AS ts;
SELECT CURRENT_TIME;
SELECT CURRENT_DATE;
EOF < /dev/null