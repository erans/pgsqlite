-- Full-Text Search (FTS) Test Suite
-- Tests PostgreSQL FTS functionality using SQLite FTS5 backend

-- ============================================
-- 1. CREATE TABLE with tsvector column
-- ============================================

DROP TABLE IF EXISTS documents;

CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    search_vector tsvector
);

-- ============================================
-- 2. INSERT with to_tsvector function
-- ============================================

INSERT INTO documents (title, content, search_vector) VALUES 
    ('First Document', 'This is the first document content', to_tsvector('english', 'This is the first document content')),
    ('Second Document', 'This is the second document with different content', to_tsvector('english', 'This is the second document with different content')),
    ('Third Document', 'Quick brown fox jumps over lazy dog', to_tsvector('english', 'Quick brown fox jumps over lazy dog'));

-- ============================================
-- 3. SELECT with FTS operators
-- ============================================

-- Basic @@ operator with to_tsquery
SELECT id, title FROM documents 
WHERE search_vector @@ to_tsquery('english', 'first');

-- @@ operator with plainto_tsquery  
SELECT id, title FROM documents 
WHERE search_vector @@ plainto_tsquery('english', 'quick fox');

-- @@ operator with phraseto_tsquery
SELECT id, title FROM documents 
WHERE search_vector @@ phraseto_tsquery('english', 'brown fox');

-- Complex query with AND operator
SELECT id, title FROM documents 
WHERE search_vector @@ to_tsquery('english', 'document & content');

-- Complex query with OR operator
SELECT id, title FROM documents 
WHERE search_vector @@ to_tsquery('english', 'quick | first');

-- ============================================
-- 4. FTS Function Tests
-- ============================================

-- Test to_tsvector function directly
SELECT to_tsvector('english', 'The quick brown fox jumps');

-- Test to_tsquery function directly
SELECT to_tsquery('english', 'quick & fox');

-- Test plainto_tsquery function
SELECT plainto_tsquery('english', 'quick brown fox');

-- Test phraseto_tsquery function
SELECT phraseto_tsquery('english', 'quick brown fox');

-- Test websearch_to_tsquery function
SELECT websearch_to_tsquery('english', 'quick OR fox');

-- ============================================
-- 5. FTS Ranking Functions
-- ============================================

-- Test ts_rank function
SELECT id, title, ts_rank(search_vector, to_tsquery('english', 'document')) as rank
FROM documents 
WHERE search_vector @@ to_tsquery('english', 'document')
ORDER BY rank DESC;

-- Test ts_rank_cd function
SELECT id, title, ts_rank_cd(search_vector, to_tsquery('english', 'content')) as rank_cd
FROM documents 
WHERE search_vector @@ to_tsquery('english', 'content')
ORDER BY rank_cd DESC;

-- ============================================
-- 6. Complex FTS Queries
-- ============================================

-- FTS with JOIN
SELECT d.id, d.title, d.content
FROM documents d
WHERE d.search_vector @@ to_tsquery('english', 'fox | dog');

-- FTS with LIMIT
SELECT id, title FROM documents 
WHERE search_vector @@ plainto_tsquery('english', 'document content')
LIMIT 2;

-- FTS with ORDER BY
SELECT id, title FROM documents 
WHERE search_vector @@ to_tsquery('english', 'document')
ORDER BY title;

-- ============================================
-- 7. Test Multiple Configurations
-- ============================================

-- Test with different language configurations
SELECT to_tsvector('simple', 'The quick brown fox');
SELECT to_tsquery('simple', 'quick & fox');

-- ============================================
-- 8. UPDATE and DELETE with FTS
-- ============================================

-- Update with to_tsvector
UPDATE documents 
SET content = 'Updated content with new text',
    search_vector = to_tsvector('english', 'Updated content with new text')
WHERE id = 1;

-- Verify update worked
SELECT id, title, content FROM documents 
WHERE search_vector @@ to_tsquery('english', 'updated');

-- Delete with FTS condition
DELETE FROM documents 
WHERE search_vector @@ to_tsquery('english', 'updated');

-- ============================================
-- 9. Cleanup
-- ============================================

DROP TABLE IF EXISTS documents;