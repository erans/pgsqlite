#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Test configuration
DB_FILE="comprehensive_test.db"
PGSQLITE_PORT=5433
DJANGO_PORT=8001
BASE_URL="http://localhost:${DJANGO_PORT}/api"

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Helper function to run test
run_test() {
    local test_name="$1"
    local test_command="$2"
    local expected_pattern="$3"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -e "${BLUE}Test ${TOTAL_TESTS}: ${test_name}${NC}"

    local response
    response=$(eval "$test_command" 2>&1)
    local exit_code=$?

    if [ $exit_code -eq 0 ] && [[ "$response" =~ $expected_pattern ]]; then
        echo -e "${GREEN}✓ PASSED${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "${RED}✗ FAILED${NC}"
        echo -e "  Command: $test_command"
        echo -e "  Expected pattern: $expected_pattern"
        echo -e "  Response: $response"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    echo
}

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
    rm -f "$DB_FILE" "${DB_FILE}-shm" "${DB_FILE}-wal"
    echo "Test database removed"

    # Print final summary
    echo
    echo -e "${CYAN}=== COMPREHENSIVE TEST SUMMARY ===${NC}"
    echo -e "Total tests: $TOTAL_TESTS"
    echo -e "${GREEN}Passed: $PASSED_TESTS${NC}"
    echo -e "${RED}Failed: $FAILED_TESTS${NC}"

    if [ $FAILED_TESTS -eq 0 ]; then
        echo -e "${GREEN}🎉 ALL TESTS PASSED! 🎉${NC}"
        exit 0
    else
        echo -e "${RED}❌ SOME TESTS FAILED ❌${NC}"
        exit 1
    fi
}

# Set up trap to cleanup on script exit
trap cleanup EXIT

echo -e "${CYAN}=== Comprehensive Django + pgsqlite PostgreSQL Feature Test ===${NC}"
echo

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

echo
echo -e "${PURPLE}=== Starting Comprehensive Tests ===${NC}"

# ========================================
# AUTHOR TESTS
# ========================================
echo -e "${CYAN}--- Testing Authors ---${NC}"

# Create authors
run_test "Create Author 1 (J.K. Rowling)" \
    "curl -s -X POST '$BASE_URL/authors/' -H 'Content-Type: application/json' -d '{
        \"name\": \"J.K. Rowling\",
        \"bio\": \"British author, philanthropist, producer, and screenwriter.\",
        \"birth_date\": \"1965-07-31\",
        \"nationality\": \"British\",
        \"is_active\": true
    }'" \
    '"name":"J.K. Rowling"'

run_test "Create Author 2 (George Orwell)" \
    "curl -s -X POST '$BASE_URL/authors/' -H 'Content-Type: application/json' -d '{
        \"name\": \"George Orwell\",
        \"bio\": \"English novelist and essayist.\",
        \"birth_date\": \"1903-06-25\",
        \"nationality\": \"British\",
        \"is_active\": false
    }'" \
    '"name":"George Orwell"'

run_test "List Authors" \
    "curl -s '$BASE_URL/authors/'" \
    '"count".*"results"'

run_test "Search Authors by Nationality" \
    "curl -s '$BASE_URL/authors/?nationality=British'" \
    '"J.K. Rowling".*"George Orwell"'

# ========================================
# PUBLISHER TESTS
# ========================================
echo -e "${CYAN}--- Testing Publishers ---${NC}"

run_test "Create Publisher (Bloomsbury)" \
    "curl -s -X POST '$BASE_URL/publishers/' -H 'Content-Type: application/json' -d '{
        \"name\": \"Bloomsbury Publishing\",
        \"founded_year\": 1986,
        \"headquarters\": \"London, UK\",
        \"website\": \"https://www.bloomsbury.com\"
    }'" \
    '"name":"Bloomsbury Publishing"'

run_test "Create Publisher (Penguin)" \
    "curl -s -X POST '$BASE_URL/publishers/' -H 'Content-Type: application/json' -d '{
        \"name\": \"Penguin Books\",
        \"founded_year\": 1935,
        \"headquarters\": \"London, UK\",
        \"website\": \"https://www.penguin.co.uk\"
    }'" \
    '"name":"Penguin Books"'

# ========================================
# GENRE TESTS
# ========================================
echo -e "${CYAN}--- Testing Genres ---${NC}"

run_test "Create Genre (Fantasy)" \
    "curl -s -X POST '$BASE_URL/genres/' -H 'Content-Type: application/json' -d '{
        \"name\": \"Fantasy\",
        \"description\": \"Fantasy literature featuring magical elements\"
    }'" \
    '"name":"Fantasy"'

run_test "Create Genre (Dystopian Fiction)" \
    "curl -s -X POST '$BASE_URL/genres/' -H 'Content-Type: application/json' -d '{
        \"name\": \"Dystopian Fiction\",
        \"description\": \"Literature depicting imaginary societies where something is terribly wrong\"
    }'" \
    '"name":"Dystopian Fiction"'

run_test "Create Subgenre (Young Adult Fantasy)" \
    "curl -s -X POST '$BASE_URL/genres/' -H 'Content-Type: application/json' -d '{
        \"name\": \"Young Adult Fantasy\",
        \"description\": \"Fantasy literature targeted at young adults\",
        \"parent_genre\": 1
    }'" \
    '"name":"Young Adult Fantasy"'

# Get IDs for further testing (simplified - in real tests would parse JSON)
AUTHOR1_ID=1
AUTHOR2_ID=2
PUBLISHER1_ID=1
PUBLISHER2_ID=2
GENRE1_ID=1
GENRE2_ID=2
GENRE3_ID=3

# ========================================
# COMPREHENSIVE BOOK TESTS
# ========================================
echo -e "${CYAN}--- Testing Advanced Book Features ---${NC}"

run_test "Create Complex Book (Harry Potter)" \
    "curl -s -X POST '$BASE_URL/books/' -H 'Content-Type: application/json' -d '{
        \"title\": \"Harry Potter and the Philosophers Stone\",
        \"subtitle\": \"The Boy Who Lived\",
        \"isbn\": \"9780747532699\",
        \"isbn10\": \"0747532699\",
        \"description\": \"A young wizard discovers his magical heritage.\",
        \"summary\": \"Harry Potter, a young boy living with his cruel relatives, discovers on his 11th birthday that he is a wizard.\",
        \"excerpt\": \"Mr. and Mrs. Dursley of number four, Privet Drive, were proud to say that they were perfectly normal, thank you very much.\",
        \"price\": \"7.99\",
        \"discount_price\": \"5.99\",
        \"pages\": 223,
        \"weight_grams\": 350.5,
        \"publication_date\": \"1997-06-26\",
        \"first_published\": \"1997-06-26\",
        \"is_available\": true,
        \"is_featured\": true,
        \"is_bestseller\": true,
        \"has_ebook\": true,
        \"has_audiobook\": true,
        \"tags\": [\"magic\", \"wizards\", \"coming-of-age\", \"adventure\"],
        \"languages\": [\"en\", \"es\", \"fr\"],
        \"formats\": [\"hardcover\", \"paperback\", \"ebook\", \"audiobook\"],
        \"awards\": [\"Nestle Smarties Book Prize\", \"British Book Awards\"],
        \"chapter_page_counts\": [17, 22, 19, 25, 31],
        \"review_scores\": [4.5, 4.8, 4.6, 4.9, 4.7],
        \"metadata\": {
            \"series\": \"Harry Potter\",
            \"book_number\": 1,
            \"target_age\": \"8-12\",
            \"reading_level\": \"Middle Grade\"
        },
        \"sales_data\": {
            \"copies_sold\": 120000000,
            \"revenue\": 960000000
        },
        \"primary_author\": 1,
        \"co_authors\": [],
        \"publisher\": 1,
        \"genres\": [1, 3],
        \"status\": \"published\",
        \"condition\": \"new\"
    }'" \
    '"title":"Harry Potter and the Philosophers Stone"'

run_test "Create Second Book (1984)" \
    "curl -s -X POST '$BASE_URL/books/' -H 'Content-Type: application/json' -d '{
        \"title\": \"1984\",
        \"isbn\": \"9780451524935\",
        \"description\": \"A dystopian social science fiction novel.\",
        \"price\": \"8.99\",
        \"pages\": 328,
        \"publication_date\": \"1949-06-08\",
        \"is_available\": true,
        \"tags\": [\"dystopian\", \"surveillance\", \"totalitarianism\"],
        \"languages\": [\"en\"],
        \"formats\": [\"paperback\", \"ebook\"],
        \"metadata\": {
            \"themes\": [\"surveillance\", \"thought control\", \"propaganda\"],
            \"setting\": \"Oceania\",
            \"year_set\": 1984
        },
        \"primary_author\": 2,
        \"publisher\": 2,
        \"genres\": [2],
        \"status\": \"published\"
    }'" \
    '"title":"1984"'

# ========================================
# ARRAY FIELD TESTS
# ========================================
echo -e "${CYAN}--- Testing PostgreSQL Array Features ---${NC}"

run_test "Filter Books by Tag (Array Contains)" \
    "curl -s '$BASE_URL/books/?tag=magic'" \
    '"Harry Potter"'

run_test "Filter Books by Language" \
    "curl -s '$BASE_URL/books/?language=en'" \
    '"count":2'

run_test "Filter Books by Format" \
    "curl -s '$BASE_URL/books/?format=ebook'" \
    '"count":2'

run_test "Add Tag to Book" \
    "curl -s -X POST '$BASE_URL/books/1/add_tag/' -H 'Content-Type: application/json' -d '{\"tag\": \"bestseller\"}'" \
    '"message":"Tag.*added"'

run_test "Remove Tag from Book" \
    "curl -s -X DELETE '$BASE_URL/books/1/remove_tag/' -H 'Content-Type: application/json' -d '{\"tag\": \"bestseller\"}'" \
    '"message":"Tag.*removed"'

# ========================================
# JSON FIELD TESTS
# ========================================
echo -e "${CYAN}--- Testing PostgreSQL JSON Features ---${NC}"

run_test "Update Book Metadata (JSON Field)" \
    "curl -s -X POST '$BASE_URL/books/1/update_metadata/' -H 'Content-Type: application/json' -d '{
        \"metadata\": {
            \"film_adaptations\": 8,
            \"theme_park_attractions\": true,
            \"merchandise_revenue\": 7300000000
        }
    }'" \
    '"message":"Metadata updated"'

run_test "Search in JSON Fields" \
    "curl -s '$BASE_URL/books/search_advanced/?json_search=series'" \
    '"Harry Potter"'

# ========================================
# RELATIONSHIP TESTS
# ========================================
echo -e "${CYAN}--- Testing Complex Relationships ---${NC}"

run_test "Get Books by Author" \
    "curl -s '$BASE_URL/books/?author=Rowling'" \
    '"Harry Potter"'

run_test "Get Books by Publisher" \
    "curl -s '$BASE_URL/books/?publisher=Bloomsbury'" \
    '"Harry Potter"'

run_test "Get Books by Genre" \
    "curl -s '$BASE_URL/books/?genre=Fantasy'" \
    '"Harry Potter"'

run_test "Get Author's Books via Relationship" \
    "curl -s '$BASE_URL/authors/1/books/'" \
    '"Harry Potter"'

run_test "Get Related Books" \
    "curl -s '$BASE_URL/books/1/related_books/'" \
    '"count"'

# ========================================
# ADVANCED FILTERING TESTS
# ========================================
echo -e "${CYAN}--- Testing Advanced Filtering ---${NC}"

run_test "Price Range Filter" \
    "curl -s '$BASE_URL/books/?min_price=5&max_price=10'" \
    '"count"'

run_test "Pages Range Filter" \
    "curl -s '$BASE_URL/books/?min_pages=200&max_pages=400'" \
    '"count"'

run_test "Date Range Filter" \
    "curl -s '$BASE_URL/books/?published_after=1990-01-01&published_before=2000-01-01'" \
    '"Harry Potter"'

run_test "Boolean Filters (Available + Featured)" \
    "curl -s '$BASE_URL/books/?available=true&featured=true'" \
    '"Harry Potter"'

run_test "Status Filter" \
    "curl -s '$BASE_URL/books/?status=published'" \
    '"count":2'

run_test "Complex Search Query" \
    "curl -s '$BASE_URL/books/?q=wizard'" \
    '"Harry Potter"'

# ========================================
# AGGREGATION TESTS
# ========================================
echo -e "${CYAN}--- Testing Aggregations and Statistics ---${NC}"

run_test "Book Statistics" \
    "curl -s '$BASE_URL/books/statistics/'" \
    '"total_books".*"avg_price".*"publications_by_year"'

run_test "Books Grouped by Author" \
    "curl -s '$BASE_URL/books/by_author/'" \
    '"J.K. Rowling".*"George Orwell"'

run_test "Books Grouped by Genre" \
    "curl -s '$BASE_URL/books/by_genre/'" \
    '"Fantasy".*"count"'

# ========================================
# REVIEW TESTS (Additional Model)
# ========================================
echo -e "${CYAN}--- Testing Reviews (Additional Model) ---${NC}"

run_test "Create Review with JSON Metadata" \
    "curl -s -X POST '$BASE_URL/reviews/' -H 'Content-Type: application/json' -d '{
        \"book\": 1,
        \"reviewer_name\": \"Alice Smith\",
        \"reviewer_email\": \"alice@example.com\",
        \"rating\": 5,
        \"title\": \"Absolutely Magical!\",
        \"content\": \"This book transported me to a wonderful magical world. Highly recommended!\",
        \"is_verified_purchase\": true,
        \"is_featured\": true,
        \"helpful_votes\": 15,
        \"metadata\": {
            \"reading_time_hours\": 6,
            \"would_recommend\": true,
            \"age_when_read\": 25
        }
    }'" \
    '"title":"Absolutely Magical!"'

run_test "Filter Reviews by Rating" \
    "curl -s '$BASE_URL/reviews/?rating=5'" \
    '"rating":5'

run_test "Filter Reviews by Verified Purchase" \
    "curl -s '$BASE_URL/reviews/?verified=true'" \
    '"is_verified_purchase":true'

# ========================================
# DECIMAL AND NUMERIC TESTS
# ========================================
echo -e "${CYAN}--- Testing Decimal/Numeric Types ---${NC}"

run_test "Create Book with Precise Decimal Values" \
    "curl -s -X POST '$BASE_URL/books/' -H 'Content-Type: application/json' -d '{
        \"title\": \"Decimal Test Book\",
        \"isbn\": \"9781234567890\",
        \"price\": \"19.99\",
        \"discount_price\": \"15.49\",
        \"rating\": \"4.73\",
        \"pages\": 156,
        \"publication_date\": \"2023-01-01\",
        \"primary_author\": 1,
        \"review_scores\": [4.5, 4.8, 4.7],
        \"status\": \"published\"
    }'" \
    '"price":"19.99".*"discount_price":"15.49"'

run_test "Verify Decimal Precision Maintained" \
    "curl -s '$BASE_URL/books/3/'" \
    '"rating":"4.73"'

# ========================================
# UUID TESTS
# ========================================
echo -e "${CYAN}--- Testing UUID Primary Keys ---${NC}"

run_test "Verify UUID Generation" \
    "curl -s '$BASE_URL/books/' | head -c 500" \
    '"id":"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"'

# ========================================
# CONSTRAINT TESTS
# ========================================
echo -e "${CYAN}--- Testing Database Constraints ---${NC}"

run_test "Test Unique Constraint (Duplicate ISBN)" \
    "curl -s -X POST '$BASE_URL/books/' -H 'Content-Type: application/json' -d '{
        \"title\": \"Duplicate ISBN Test\",
        \"isbn\": \"9780747532699\",
        \"price\": \"10.00\",
        \"pages\": 100,
        \"publication_date\": \"2023-01-01\",
        \"primary_author\": 1,
        \"status\": \"draft\"
    }'" \
    '"isbn.*already exists|unique constraint|integrity error"'

run_test "Test Validation (Invalid Price)" \
    "curl -s -X POST '$BASE_URL/books/' -H 'Content-Type: application/json' -d '{
        \"title\": \"Invalid Price Test\",
        \"isbn\": \"9781111111111\",
        \"price\": \"-5.00\",
        \"pages\": 100,
        \"publication_date\": \"2023-01-01\",
        \"primary_author\": 1,
        \"status\": \"draft\"
    }'" \
    '"price.*positive|validation"'

echo
echo -e "${PURPLE}=== All Tests Completed! ===${NC}"

# The cleanup and summary will be handled by the trap