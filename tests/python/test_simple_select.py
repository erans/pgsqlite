import sqlite3

# Connect directly to the SQLite database
conn = sqlite3.connect('test_debug.db')
cursor = conn.cursor()

# Check if data exists
cursor.execute("SELECT * FROM users")
rows = cursor.fetchall()

print(f"Total rows in users table: {len(rows)}")
for row in rows:
    print(f"Row: {row}")

# Now test the exact query the ultra-fast path should execute
cursor.execute("SELECT id, name, created_at FROM users WHERE id = ?", (1,))
result = cursor.fetchone()
print(f"\nResult for id=1: {result}")

# Also try as text
cursor.execute("SELECT id, name, created_at FROM users WHERE id = ?", ('1',))
result = cursor.fetchone()
print(f"Result for id='1': {result}")

conn.close()
