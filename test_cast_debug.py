#!/usr/bin/env python3
"""Debug column cast detection issue"""

# Test the analyze_column_casts function behavior
test_query = """SELECT test_model.id AS test_model_id, test_model.name AS test_model_name, test_model.created_at AS test_model_created_at 
FROM test_model 
WHERE test_model.id = CAST($1 AS INTEGER)"""

print("Test query:")
print(test_query)
print()

# Manual parsing to see what should happen
select_pos = test_query.upper().find("SELECT")
after_select = test_query[select_pos + 6:]
from_pos = after_select.upper().find("FROM")

if from_pos != -1:
    select_list = after_select[:from_pos]
    print("SELECT list (what should be analyzed):")
    print(repr(select_list))
    print()
    
    # Check for :: casts in the SELECT list
    if "::" in select_list:
        print("Found :: casts in SELECT list")
    else:
        print("NO :: casts found in SELECT list - this should be empty!")
else:
    print("No FROM clause found")

print()
print("Full query after FROM:")
if from_pos != -1:
    remainder = after_select[from_pos:]
    print(repr(remainder))
    if "::" in remainder or "CAST" in remainder.upper():
        print("Found casts in WHERE/JOIN/ORDER BY - these should be IGNORED!")