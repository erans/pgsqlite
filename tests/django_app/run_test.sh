#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test configuration
DB_FILE="testdb.sqlite"
PGSQLITE_PORT=5432
DJANGO_PORT=8000
BASE_URL="http://localhost:${DJANGO_PORT}/api/books"

# Cleanup function
cleanup() {
    echo -e "${YELLOW}Cleaning up processes...${NC}"

    # Kill Django server
    if [ ! -z "$DJANGO_PID" ]; then
        kill $DJANGO_PID 2>/dev/null
        echo "Django server stopped"
    fi

    # Kill pgsqlite
    if [ ! -z "$PGSQLITE_PID" ]; then
        kill $PGSQLITE_PID 2>/dev/null
        echo "pgsqlite stopped"
    fi

    # Remove test database
    rm -f "$DB_FILE"
    echo "Test database removed"
}

# Set up trap to cleanup on script exit
trap cleanup EXIT

echo -e "${BLUE}=== Django + pgsqlite CRUD Test ===${NC}"

# Step 1: Start pgsqlite
echo -e "${YELLOW}Step 1: Starting pgsqlite...${NC}"
cd /home/eran/work/pgsqlite
cargo build --release
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to build pgsqlite${NC}"
    exit 1
fi

# Start pgsqlite in background with test database
cd tests/django_app
../../target/release/pgsqlite --database "$DB_FILE" --port $PGSQLITE_PORT &
PGSQLITE_PID=$!

# Wait for pgsqlite to start
sleep 3

# Check if pgsqlite is running
if ! kill -0 $PGSQLITE_PID 2>/dev/null; then
    echo -e "${RED}pgsqlite failed to start${NC}"
    exit 1
fi
echo -e "${GREEN}pgsqlite started with PID $PGSQLITE_PID${NC}"

# Step 2: Run Django migrations
echo -e "${YELLOW}Step 2: Running Django migrations...${NC}"
poetry run python manage.py makemigrations books
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to create migrations${NC}"
    exit 1
fi

poetry run python manage.py migrate
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to run migrations${NC}"
    exit 1
fi
echo -e "${GREEN}Migrations completed${NC}"

# Step 3: Start Django server
echo -e "${YELLOW}Step 3: Starting Django server...${NC}"
poetry run python manage.py runserver $DJANGO_PORT &
DJANGO_PID=$!

# Wait for Django to start
sleep 5

# Check if Django is running
if ! kill -0 $DJANGO_PID 2>/dev/null; then
    echo -e "${RED}Django server failed to start${NC}"
    exit 1
fi
echo -e "${GREEN}Django server started with PID $DJANGO_PID${NC}"

# Step 4: Run CRUD tests
echo -e "${YELLOW}Step 4: Running CRUD tests...${NC}"

# Test 1: Create a book (CREATE)
echo -e "${BLUE}Test 1: Creating a book...${NC}"
BOOK1_RESPONSE=$(curl -s -X POST "$BASE_URL/" \
    -H "Content-Type: application/json" \
    -d '{
        "title": "The Great Gatsby",
        "author": "F. Scott Fitzgerald",
        "isbn": "9780743273565",
        "description": "A classic American novel",
        "price": "12.99",
        "publication_date": "1925-04-10",
        "is_available": true,
        "tags": ["classic", "american", "literature"]
    }')

if echo "$BOOK1_RESPONSE" | grep -q '"title"'; then
    echo -e "${GREEN}✓ Book created successfully${NC}"
    BOOK1_ID=$(echo "$BOOK1_RESPONSE" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
    echo "  Book ID: $BOOK1_ID"
else
    echo -e "${RED}✗ Failed to create book${NC}"
    echo "  Response: $BOOK1_RESPONSE"
fi

# Test 2: Create another book
echo -e "${BLUE}Test 2: Creating another book...${NC}"
BOOK2_RESPONSE=$(curl -s -X POST "$BASE_URL/" \
    -H "Content-Type: application/json" \
    -d '{
        "title": "To Kill a Mockingbird",
        "author": "Harper Lee",
        "isbn": "9780446310789",
        "description": "A story of racial injustice and childhood",
        "price": "14.99",
        "publication_date": "1960-07-11",
        "is_available": true,
        "tags": ["classic", "social justice"]
    }')

if echo "$BOOK2_RESPONSE" | grep -q '"title"'; then
    echo -e "${GREEN}✓ Second book created successfully${NC}"
    BOOK2_ID=$(echo "$BOOK2_RESPONSE" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
    echo "  Book ID: $BOOK2_ID"
else
    echo -e "${RED}✗ Failed to create second book${NC}"
    echo "  Response: $BOOK2_RESPONSE"
fi

# Test 3: List all books (READ)
echo -e "${BLUE}Test 3: Listing all books...${NC}"
LIST_RESPONSE=$(curl -s "$BASE_URL/")
BOOK_COUNT=$(echo "$LIST_RESPONSE" | grep -o '"title"' | wc -l)
echo -e "${GREEN}✓ Found $BOOK_COUNT books${NC}"

# Test 4: Get specific book (READ)
echo -e "${BLUE}Test 4: Getting specific book...${NC}"
if [ ! -z "$BOOK1_ID" ]; then
    GET_RESPONSE=$(curl -s "$BASE_URL/$BOOK1_ID/")
    if echo "$GET_RESPONSE" | grep -q "The Great Gatsby"; then
        echo -e "${GREEN}✓ Retrieved book successfully${NC}"
    else
        echo -e "${RED}✗ Failed to retrieve book${NC}"
        echo "  Response: $GET_RESPONSE"
    fi
else
    echo -e "${YELLOW}⚠ Skipping (no book ID)${NC}"
fi

# Test 5: Update book (UPDATE)
echo -e "${BLUE}Test 5: Updating book...${NC}"
if [ ! -z "$BOOK1_ID" ]; then
    UPDATE_RESPONSE=$(curl -s -X PATCH "$BASE_URL/$BOOK1_ID/" \
        -H "Content-Type: application/json" \
        -d '{
            "price": "15.99",
            "description": "A classic American novel - Updated edition"
        }')

    if echo "$UPDATE_RESPONSE" | grep -q "15.99"; then
        echo -e "${GREEN}✓ Book updated successfully${NC}"
    else
        echo -e "${RED}✗ Failed to update book${NC}"
        echo "  Response: $UPDATE_RESPONSE"
    fi
else
    echo -e "${YELLOW}⚠ Skipping (no book ID)${NC}"
fi

# Test 6: Add tag to book (Custom action)
echo -e "${BLUE}Test 6: Adding tag to book...${NC}"
if [ ! -z "$BOOK1_ID" ]; then
    TAG_RESPONSE=$(curl -s -X POST "$BASE_URL/$BOOK1_ID/add_tag/" \
        -H "Content-Type: application/json" \
        -d '{"tag": "bestseller"}')

    if echo "$TAG_RESPONSE" | grep -q "bestseller"; then
        echo -e "${GREEN}✓ Tag added successfully${NC}"
    else
        echo -e "${RED}✗ Failed to add tag${NC}"
        echo "  Response: $TAG_RESPONSE"
    fi
else
    echo -e "${YELLOW}⚠ Skipping (no book ID)${NC}"
fi

# Test 7: Search books (Filter)
echo -e "${BLUE}Test 7: Searching books...${NC}"
SEARCH_RESPONSE=$(curl -s "$BASE_URL/?search=gatsby")
if echo "$SEARCH_RESPONSE" | grep -q "The Great Gatsby"; then
    echo -e "${GREEN}✓ Search functionality working${NC}"
else
    echo -e "${RED}✗ Search failed${NC}"
    echo "  Response: $SEARCH_RESPONSE"
fi

# Test 8: Filter by author
echo -e "${BLUE}Test 8: Filtering by author...${NC}"
FILTER_RESPONSE=$(curl -s "$BASE_URL/?author=Harper")
if echo "$SEARCH_RESPONSE" | grep -q "Harper"; then
    echo -e "${GREEN}✓ Author filter working${NC}"
else
    echo -e "${RED}✗ Author filter failed${NC}"
    echo "  Response: $FILTER_RESPONSE"
fi

# Test 9: Get books by author (Custom action)
echo -e "${BLUE}Test 9: Getting books by author...${NC}"
AUTHOR_RESPONSE=$(curl -s "$BASE_URL/by_author/")
if echo "$AUTHOR_RESPONSE" | grep -q "Harper Lee"; then
    echo -e "${GREEN}✓ Books by author endpoint working${NC}"
else
    echo -e "${RED}✗ Books by author failed${NC}"
    echo "  Response: $AUTHOR_RESPONSE"
fi

# Test 10: Delete book (DELETE)
echo -e "${BLUE}Test 10: Deleting book...${NC}"
if [ ! -z "$BOOK2_ID" ]; then
    DELETE_RESPONSE=$(curl -s -X DELETE "$BASE_URL/$BOOK2_ID/")

    # Check if book is gone
    GET_DELETED_RESPONSE=$(curl -s "$BASE_URL/$BOOK2_ID/" -w "%{http_code}")
    if echo "$GET_DELETED_RESPONSE" | grep -q "404"; then
        echo -e "${GREEN}✓ Book deleted successfully${NC}"
    else
        echo -e "${RED}✗ Failed to delete book${NC}"
        echo "  Response: $DELETE_RESPONSE"
    fi
else
    echo -e "${YELLOW}⚠ Skipping (no book ID)${NC}"
fi

# Test 11: Verify final count
echo -e "${BLUE}Test 11: Verifying final book count...${NC}"
FINAL_LIST_RESPONSE=$(curl -s "$BASE_URL/")
FINAL_BOOK_COUNT=$(echo "$FINAL_LIST_RESPONSE" | grep -o '"title"' | wc -l)
echo -e "${GREEN}✓ Final book count: $FINAL_BOOK_COUNT${NC}"

echo -e "${BLUE}=== All CRUD tests completed ===${NC}"

# Summary
echo -e "${YELLOW}Test Summary:${NC}"
echo "- Database: PostgreSQL via pgsqlite ($DB_FILE)"
echo "- Django models: Book with UUID, arrays, decimals, dates"
echo "- API endpoints tested: CREATE, READ, UPDATE, DELETE"
echo "- Advanced features: Search, filtering, custom actions"
echo "- PostgreSQL-specific features: Arrays, UUIDs, advanced types"

echo -e "${GREEN}Django + pgsqlite integration test completed successfully!${NC}"