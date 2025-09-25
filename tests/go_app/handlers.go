package main

import (
	"bookstore/models"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// Author handlers

func getAuthors(c *gin.Context) {
	var authors []models.Author

	query := db.Model(&models.Author{})

	// Filtering
	if active := c.Query("active"); active == "true" {
		query = query.Scopes(models.ScopeActive)
	}

	if nationality := c.Query("nationality"); nationality != "" {
		query = query.Scopes(models.ScopeByNationality(nationality))
	}

	if search := c.Query("search"); search != "" {
		query = query.Where("name ILIKE ?", "%"+search+"%")
	}

	// Pagination
	page := 1
	if p := c.Query("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	perPage := 20
	if pp := c.Query("per_page"); pp != "" {
		if parsed, err := strconv.Atoi(pp); err == nil && parsed > 0 && parsed <= 100 {
			perPage = parsed
		}
	}

	offset := (page - 1) * perPage

	var total int64
	query.Count(&total)

	if err := query.Offset(offset).Limit(perPage).Find(&authors).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"authors": authors,
		"meta": gin.H{
			"page":         page,
			"per_page":     perPage,
			"total":        total,
			"total_pages":  (total + int64(perPage) - 1) / int64(perPage),
		},
	})
}

func createAuthor(c *gin.Context) {
	var author models.Author
	if err := c.ShouldBindJSON(&author); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := db.Create(&author).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"author": author})
}

func getAuthor(c *gin.Context) {
	id, err := parseUint(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID"})
		return
	}

	var author models.Author
	if err := db.Preload("Books.Reviews").Preload("Books.BookInventory").First(&author, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Author not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"author": author})
}

func updateAuthor(c *gin.Context) {
	id, err := parseUint(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID"})
		return
	}

	var author models.Author
	if err := db.First(&author, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Author not found"})
		return
	}

	if err := c.ShouldBindJSON(&author); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := db.Save(&author).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"author": author})
}

func deleteAuthor(c *gin.Context) {
	id, err := parseUint(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID"})
		return
	}

	if err := db.Delete(&models.Author{}, id).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

func getAuthorStats(c *gin.Context) {
	var totalAuthors int64
	var activeAuthors int64

	db.Model(&models.Author{}).Count(&totalAuthors)
	db.Model(&models.Author{}).Where("is_active = ?", true).Count(&activeAuthors)

	// Authors by nationality
	type NationalityCount struct {
		Nationality string `json:"nationality"`
		Count       int64  `json:"count"`
	}
	var nationalityCounts []NationalityCount
	db.Model(&models.Author{}).
		Select("nationality, COUNT(*) as count").
		Group("nationality").
		Having("nationality IS NOT NULL").
		Scan(&nationalityCounts)

	// Top authors by book count
	type AuthorBookCount struct {
		Name      string `json:"name"`
		BookCount int64  `json:"book_count"`
	}
	var topAuthors []AuthorBookCount
	db.Model(&models.Author{}).
		Select("authors.name, COUNT(books.id) as book_count").
		Joins("LEFT JOIN books ON authors.id = books.author_id").
		Group("authors.id, authors.name").
		Order("book_count DESC").
		Limit(10).
		Scan(&topAuthors)

	c.JSON(http.StatusOK, gin.H{
		"stats": gin.H{
			"total_authors":          totalAuthors,
			"active_authors":         activeAuthors,
			"authors_by_nationality": nationalityCounts,
			"top_authors_by_books":   topAuthors,
		},
	})
}

// Book handlers

func getBooks(c *gin.Context) {
	var books []models.Book

	query := db.Model(&models.Book{})

	// Filters
	if available := c.Query("available"); available == "true" {
		query = query.Scopes(models.ScopeAvailable)
	}

	if published := c.Query("published"); published == "true" {
		query = query.Scopes(models.ScopePublished)
	}

	if featured := c.Query("featured"); featured == "true" {
		query = query.Scopes(models.ScopeFeatured)
	}

	if bestseller := c.Query("bestseller"); bestseller == "true" {
		query = query.Scopes(models.ScopeBestsellers)
	}

	if authorID := c.Query("author_id"); authorID != "" {
		if id, err := parseUint(authorID); err == nil {
			query = query.Scopes(models.ScopeByAuthor(id))
		}
	}

	if tag := c.Query("tag"); tag != "" {
		query = query.Scopes(models.ScopeWithTag(tag))
	}

	if genre := c.Query("genre"); genre != "" {
		query = query.Scopes(models.ScopeWithGenre(genre))
	}

	// Price range filter
	if minPrice := c.Query("min_price"); minPrice != "" {
		if maxPrice := c.Query("max_price"); maxPrice != "" {
			if min, err1 := strconv.ParseFloat(minPrice, 64); err1 == nil {
				if max, err2 := strconv.ParseFloat(maxPrice, 64); err2 == nil {
					query = query.Scopes(models.ScopeByPriceRange(min, max))
				}
			}
		}
	}

	// Sorting
	switch c.Query("sort") {
	case "title":
		query = query.Order("title")
	case "price":
		query = query.Order("price")
	case "rating":
		query = query.Order("average_rating DESC NULLS LAST")
	case "publication_date":
		query = query.Order("publication_date")
	case "popularity":
		query = query.Scopes(models.ScopePopular)
	default:
		query = query.Order("created_at DESC")
	}

	// Pagination
	page := 1
	if p := c.Query("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	perPage := 20
	if pp := c.Query("per_page"); pp != "" {
		if parsed, err := strconv.Atoi(pp); err == nil && parsed > 0 && parsed <= 100 {
			perPage = parsed
		}
	}

	offset := (page - 1) * perPage

	var total int64
	query.Count(&total)

	if err := query.Preload("Author").
		Preload("Publisher").
		Preload("BookInventory").
		Offset(offset).
		Limit(perPage).
		Find(&books).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"books": books,
		"meta": gin.H{
			"page":        page,
			"per_page":    perPage,
			"total":       total,
			"total_pages": (total + int64(perPage) - 1) / int64(perPage),
		},
	})
}

func createBook(c *gin.Context) {
	var book models.Book
	if err := c.ShouldBindJSON(&book); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Start transaction
	tx := db.Begin()

	if err := tx.Create(&book).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Create inventory if provided in request
	var inventory models.BookInventory
	if c.ShouldBindJSON(&inventory) == nil && inventory.CostPrice > 0 {
		inventory.BookID = book.ID
		tx.Create(&inventory)
	}

	tx.Commit()

	// Load with associations
	db.Preload("Author").Preload("Publisher").First(&book, book.ID)

	c.JSON(http.StatusCreated, gin.H{"book": book})
}

func getBook(c *gin.Context) {
	idStr := c.Param("id")

	var book models.Book
	if err := db.Preload("Author").
		Preload("Publisher").
		Preload("Reviews").
		Preload("BookInventory").
		Preload("Genres").
		Where("id = ?", idStr).
		First(&book).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Book not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"book": book})
}

func updateBook(c *gin.Context) {
	idStr := c.Param("id")

	var book models.Book
	if err := db.Where("id = ?", idStr).First(&book).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Book not found"})
		return
	}

	if err := c.ShouldBindJSON(&book); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := db.Save(&book).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"book": book})
}

func deleteBook(c *gin.Context) {
	idStr := c.Param("id")

	if err := db.Where("id = ?", idStr).Delete(&models.Book{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

func searchBooks(c *gin.Context) {
	query := c.Query("q")
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Query parameter 'q' is required"})
		return
	}

	var books []models.Book

	// Simple text search using ILIKE (PostgreSQL full-text search simulation)
	if err := db.Preload("Author").
		Preload("Publisher").
		Where("title ILIKE ? OR description ILIKE ?", "%"+query+"%", "%"+query+"%").
		Order("title").
		Limit(50).
		Find(&books).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"books": books,
		"query": query,
		"count": len(books),
	})
}

func addBookTag(c *gin.Context) {
	idStr := c.Param("id")

	var book models.Book
	if err := db.Where("id = ?", idStr).First(&book).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Book not found"})
		return
	}

	var request struct {
		Tag string `json:"tag" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	book.AddTag(request.Tag)

	if err := db.Save(&book).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"book":    book,
		"message": "Tag added successfully",
	})
}

// Review handlers

func getBookReviews(c *gin.Context) {
	idStr := c.Param("id")
	bookID := models.UUID{}
	if err := bookID.UnmarshalJSON([]byte(`"` + idStr + `"`)); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid book ID"})
		return
	}

	var reviews []models.Review

	query := db.Model(&models.Review{}).Where("book_id = ?", bookID)

	// Filters
	if verified := c.Query("verified"); verified == "true" {
		query = query.Scopes(models.ScopeVerifiedReviews)
	}

	if featured := c.Query("featured"); featured == "true" {
		query = query.Scopes(models.ScopeFeaturedReviews)
	}

	if rating := c.Query("rating"); rating != "" {
		if r, err := strconv.Atoi(rating); err == nil {
			query = query.Scopes(models.ScopeByRating(r))
		}
	}

	// Pagination
	page := 1
	if p := c.Query("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	perPage := 20
	offset := (page - 1) * perPage

	if err := query.Order("created_at DESC").
		Offset(offset).
		Limit(perPage).
		Find(&reviews).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"reviews": reviews})
}

func createReview(c *gin.Context) {
	idStr := c.Param("id")
	bookID := models.UUID{}
	if err := bookID.UnmarshalJSON([]byte(`"` + idStr + `"`)); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid book ID"})
		return
	}

	var review models.Review
	if err := c.ShouldBindJSON(&review); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	review.BookID = bookID

	if err := db.Create(&review).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"review": review})
}

func markReviewHelpful(c *gin.Context) {
	reviewIDStr := c.Param("id")
	reviewID := models.UUID{}
	if err := reviewID.UnmarshalJSON([]byte(`"` + reviewIDStr + `"`)); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid review ID"})
		return
	}

	var review models.Review
	if err := db.Where("id = ?", reviewID).First(&review).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Review not found"})
		return
	}

	if err := review.MarkHelpful(db); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"review": review,
		"message": "Review marked as helpful",
	})
}

// Publisher handlers

func getPublishers(c *gin.Context) {
	var publishers []models.Publisher

	query := db.Model(&models.Publisher{})

	if active := c.Query("active"); active == "true" {
		query = query.Scopes(models.ScopeActivePublishers)
	}

	if err := query.Find(&publishers).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"publishers": publishers})
}

func getPublisher(c *gin.Context) {
	id, err := parseUint(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID"})
		return
	}

	var publisher models.Publisher
	if err := db.Preload("Books").First(&publisher, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Publisher not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"publisher": publisher})
}

// Genre handlers

func getGenres(c *gin.Context) {
	var genres []models.Genre

	query := db.Model(&models.Genre{})

	if active := c.Query("active"); active == "true" {
		query = query.Scopes(models.ScopeActiveGenres)
	}

	if rootOnly := c.Query("root_only"); rootOnly == "true" {
		query = query.Scopes(models.ScopeRootGenres)
	}

	if withChildren := c.Query("with_children"); withChildren == "true" {
		query = query.Scopes(models.ScopeWithChildren)
	}

	if err := query.Find(&genres).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"genres": genres})
}

func getGenreBooks(c *gin.Context) {
	id, err := parseUint(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID"})
		return
	}

	var genre models.Genre
	if err := db.Preload("Books.Author").Preload("Books.Publisher").First(&genre, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Genre not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"genre": genre,
		"books": genre.Books,
	})
}

// Inventory handlers

func getInventory(c *gin.Context) {
	var inventory []models.BookInventory

	query := db.Model(&models.BookInventory{})

	if lowStock := c.Query("low_stock"); lowStock == "true" {
		query = query.Scopes(models.ScopeLowStock)
	}

	if outOfStock := c.Query("out_of_stock"); outOfStock == "true" {
		query = query.Scopes(models.ScopeOutOfStock)
	}

	if err := query.Preload("Book.Author").Find(&inventory).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"inventory": inventory})
}

func restockInventory(c *gin.Context) {
	id, err := parseUint(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid ID"})
		return
	}

	var inventory models.BookInventory
	if err := db.First(&inventory, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Inventory not found"})
		return
	}

	var request struct {
		Quantity int `json:"quantity" binding:"required,gt=0"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := inventory.Restock(request.Quantity, db); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"inventory": inventory,
		"message":   "Inventory restocked successfully",
	})
}