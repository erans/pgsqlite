# Go + GORM Application for pgsqlite Testing

This is a comprehensive Go application using GORM to test pgsqlite compatibility with Go applications that use PostgreSQL-specific features.

## Features

This Go application demonstrates full PostgreSQL compatibility with the following features:

### GORM + PostgreSQL Features Implemented
- **Auto-incrementing ID columns** - GORM's default ID behavior
- **UUID primary keys** - Using `github.com/google/uuid`
- **PostgreSQL arrays** - Using `pq.StringArray` and custom types
- **JSON/JSONB fields** - Using GORM's serializer support
- **Complex associations** - has many, belongs to, many-to-many
- **Database constraints** - Foreign keys, unique constraints, check constraints
- **GORM hooks** - BeforeCreate, AfterFind, etc.
- **Advanced queries** - Joins, preloading, raw SQL with PostgreSQL operators

### Models
1. **Author** - Authors with auto-increment ID and JSONB metadata
2. **Publisher** - Publishing companies with JSON contact information
3. **Genre** - Book genres with self-referential parent-child relationships
4. **Book** - Main model with UUID, arrays, JSONB, and comprehensive PostgreSQL features
5. **Review** - User reviews with ratings and JSON metadata
6. **BookInventory** - Inventory management with arrays and JSON supply chain data

### API Endpoints
- `GET /api/authors` - List authors with filtering and pagination
- `POST /api/authors` - Create new author
- `GET /api/books` - List books with complex filtering
- `POST /api/books` - Create new book with full PostgreSQL features
- `GET /api/books/search?q=query` - Full-text search simulation
- `GET /api/books/:id/reviews` - Book reviews
- `POST /api/books/:id/reviews` - Create review
- And many more...

### GORM Features Tested
- **Auto Migration** - GORM's automatic schema creation
- **Associations** - Preloading, joins, and relationship queries
- **Raw SQL** - PostgreSQL-specific queries with GORM
- **Transactions** - GORM transaction support
- **Hooks** - Model lifecycle hooks
- **Scopes** - Reusable query logic
- **Serializers** - JSON field handling

## Database Schema

The GORM models include:
- Auto-incrementing ID columns (GORM default)
- UUID columns with proper PostgreSQL generation
- PostgreSQL arrays using `pq.StringArray`
- JSON/JSONB fields using GORM serializers
- Complex foreign key relationships
- Check constraints for data validation
- Indexes optimized for PostgreSQL

## Testing with pgsqlite

To test this Go application with pgsqlite:

1. Start pgsqlite server:
   ```bash
   pgsqlite --database go_bookstore.db --port 5433
   ```

2. Run the Go application:
   ```bash
   cd tests/go_app
   go run main.go
   ```

3. Test the API endpoints:
   ```bash
   # List books
   curl http://localhost:8080/api/books

   # Create author
   curl -X POST http://localhost:8080/api/authors \
     -H "Content-Type: application/json" \
     -d '{"name": "Test Author", "email": "test@example.com"}'

   # Create book with PostgreSQL features
   curl -X POST http://localhost:8080/api/books \
     -H "Content-Type: application/json" \
     -d '{"title": "Test Book", "author_id": 1, "tags": ["test", "gorm"], "metadata": {"genre": "Technical"}}'
   ```

## GORM + PostgreSQL Feature Testing

This application tests all major GORM features that work with PostgreSQL:

- ✅ GORM Auto Migration with PostgreSQL types
- ✅ UUID primary keys with proper PostgreSQL generation
- ✅ PostgreSQL arrays with GORM custom types
- ✅ JSON/JSONB with GORM serializers
- ✅ Complex GORM associations and preloading
- ✅ GORM scopes and advanced querying
- ✅ GORM hooks and model lifecycle events
- ✅ GORM transactions and batch operations
- ✅ Raw SQL with PostgreSQL operators
- ✅ Database introspection and schema management

This ensures pgsqlite provides complete GORM compatibility for production Go applications.