package main

import (
	"bookstore/models"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var db *gorm.DB

func main() {
	// Initialize database
	initDB()

	// Run migrations
	migrate()

	// Seed data
	seedData()

	// Setup router
	router := setupRouter()

	// Start server
	fmt.Println("🚀 Go + GORM Bookstore API starting on :8080")
	fmt.Println("📚 Testing PostgreSQL features with pgsqlite")
	log.Fatal(router.Run(":8080"))
}

func initDB() {
	// Database connection parameters for pgsqlite
	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "5434")
	user := getEnv("DB_USER", "postgres")
	dbname := getEnv("DB_NAME", "go_bookstore.db")

	dsn := fmt.Sprintf("host=%s user=%s dbname=%s port=%s sslmode=disable TimeZone=UTC",
		host, user, dbname, port)

	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error), // Reduce logging to avoid issues
		DisableForeignKeyConstraintWhenMigrating: true,
	})

	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	fmt.Println("✅ Connected to pgsqlite database")
}

func migrate() {
	fmt.Println("🔄 Running GORM auto migrations...")

	err := db.AutoMigrate(
		&models.Author{},
		&models.Publisher{},
		&models.Genre{},
		&models.Book{},
		&models.Review{},
		&models.BookInventory{},
	)

	if err != nil {
		log.Fatal("Failed to migrate database:", err)
	}

	fmt.Println("✅ Database migrations completed")
}

func seedData() {
	fmt.Println("🌱 Seeding initial data...")

	// Check if data already exists
	var authorCount int64
	db.Model(&models.Author{}).Count(&authorCount)
	if authorCount > 0 {
		fmt.Println("📊 Data already exists, skipping seed")
		return
	}

	// Create authors
	authors := []models.Author{
		{
			Name:        "Jane Austen",
			Email:       "jane@example.com",
			Bio:         stringPtr("English novelist known for social commentary and wit"),
			BirthDate:   timePtr(time.Date(1775, 12, 16, 0, 0, 0, 0, time.UTC)),
			Nationality: stringPtr("British"),
			IsActive:    false,
			Metadata: models.JSON{
				"birth_place":   "Steventon, Hampshire",
				"famous_works":  []string{"Pride and Prejudice", "Emma"},
				"literary_period": "Regency",
			},
			SocialLinks: pq.StringArray{"https://janeausten.org"},
		},
		{
			Name:        "Mark Twain",
			Email:       "mark@example.com",
			Bio:         stringPtr("American writer and humorist"),
			BirthDate:   timePtr(time.Date(1835, 11, 30, 0, 0, 0, 0, time.UTC)),
			Nationality: stringPtr("American"),
			IsActive:    false,
			Metadata: models.JSON{
				"birth_place": "Florida, Missouri",
				"real_name":   "Samuel Langhorne Clemens",
			},
			SocialLinks: pq.StringArray{"https://marktwainhouse.org"},
		},
	}

	db.Create(&authors)

	// Create publishers
	publishers := []models.Publisher{
		{
			Name:         "Penguin Random House",
			FoundedYear:  intPtr(1927),
			Headquarters: stringPtr("New York, USA"),
			WebsiteURL:   stringPtr("https://penguinrandomhouse.com"),
			IsActive:     true,
			ContactInfo: models.JSON{
				"phone": "+1-212-366-2000",
				"email": "contact@penguinrandomhouse.com",
			},
		},
		{
			Name:         "Vintage Books",
			FoundedYear:  intPtr(1954),
			Headquarters: stringPtr("New York, USA"),
			WebsiteURL:   stringPtr("https://vintagebooks.com"),
			IsActive:     true,
			ContactInfo: models.JSON{
				"phone": "+1-212-751-2600",
			},
		},
	}

	db.Create(&publishers)

	// Create genres
	genres := []models.Genre{
		{Name: "Fiction", Description: stringPtr("Literary fiction and novels")},
		{Name: "Non-Fiction", Description: stringPtr("Factual and educational books")},
	}
	db.Create(&genres)

	// Create child genres
	var fictionGenre models.Genre
	db.Where("name = ?", "Fiction").First(&fictionGenre)

	childGenres := []models.Genre{
		{Name: "Classic Literature", Description: stringPtr("Timeless literary works"), ParentID: &fictionGenre.ID},
		{Name: "Romance", Description: stringPtr("Romantic literature"), ParentID: &fictionGenre.ID},
	}
	db.Create(&childGenres)

	// Create books with comprehensive PostgreSQL features
	var classicGenre models.Genre
	db.Where("name = ?", "Classic Literature").First(&classicGenre)

	books := []models.Book{
		{
			Title:           "Pride and Prejudice",
			Subtitle:        stringPtr("A Novel of Manners"),
			ISBN13:          "9780141439518",
			ISBN10:          stringPtr("0141439513"),
			Description:     stringPtr("A romantic novel of manners written by Jane Austen in 1813"),
			Summary:         stringPtr("Elizabeth Bennet navigates love and social expectations"),
			Excerpt:         stringPtr("It is a truth universally acknowledged..."),
			Price:           12.99,
			DiscountPrice:   float64Ptr(9.99),
			Pages:           432,
			WeightGrams:     float64Ptr(340.5),
			PublicationDate: time.Date(1813, 1, 28, 0, 0, 0, 0, time.UTC),
			IsAvailable:     true,
			IsFeatured:      true,
			IsBestseller:    true,
			HasEbook:        true,
			HasAudiobook:    true,
			Tags:            pq.StringArray{"classic", "romance", "social commentary"},
			Languages:       pq.StringArray{"English"},
			Formats:         pq.StringArray{"hardcover", "paperback", "ebook", "audiobook"},
			Awards:          pq.StringArray{"BBC Big Read #2"},
			ChapterTitles:   pq.StringArray{"Volume I", "Volume II", "Volume III"},
			Metadata: models.JSON{
				"genre":       "Romance",
				"period":      "Regency Era",
				"adaptations": []string{"1995 BBC", "2005 Film"},
			},
			ReviewsSummary: models.JSON{
				"total_reviews":   1500,
				"average_rating":  4.5,
			},
			SalesData: models.JSON{
				"total_sold": 250000,
				"revenue":    3247500,
			},
			AuthorID:    authors[0].ID,
			PublisherID: &publishers[0].ID,
			Status:      models.BookStatusPublished,
			Condition:   models.BookConditionNew,
		},
		{
			Title:           "Adventures of Huckleberry Finn",
			ISBN13:          "9780486280615",
			Description:     stringPtr("Mark Twain's masterpiece about a boy's journey"),
			Price:           10.95,
			Pages:           366,
			PublicationDate: time.Date(1884, 12, 10, 0, 0, 0, 0, time.UTC),
			IsAvailable:     true,
			IsBestseller:    true,
			HasEbook:        true,
			Tags:            pq.StringArray{"classic", "adventure", "americana"},
			Languages:       pq.StringArray{"English"},
			Formats:         pq.StringArray{"paperback", "ebook"},
			Metadata: models.JSON{
				"genre":  "Adventure",
				"themes": []string{"freedom", "friendship"},
			},
			ReviewsSummary: models.JSON{
				"total_reviews":  890,
				"average_rating": 4.2,
			},
			AuthorID:  authors[1].ID,
			Status:    models.BookStatusPublished,
			Condition: models.BookConditionGood,
		},
	}

	db.Create(&books)

	// Associate books with genres
	var prideBook models.Book
	db.Where("title = ?", "Pride and Prejudice").First(&prideBook)
	db.Model(&prideBook).Association("Genres").Append(&classicGenre)

	// Create inventory records
	inventories := []models.BookInventory{
		{
			BookID:             prideBook.ID,
			QuantityInStock:    150,
			QuantityReserved:   25,
			QuantitySold:       2847,
			ReorderLevel:       50,
			MaxStockLevel:      500,
			CostPrice:          6.50,
			WholesalePrice:     float64Ptr(8.99),
			WarehouseLocations: pq.StringArray{"Warehouse A", "Warehouse C"},
			SupplierCodes:      pq.StringArray{"PEN001", "RND045"},
			SupplyChainData: models.JSON{
				"lead_time_days":      14,
				"supplier_rating":     4.8,
				"last_order_quantity": 200,
			},
			LastRestockedAt: timePtr(time.Now().AddDate(0, 0, -5)),
		},
	}

	db.Create(&inventories)

	// Create reviews
	reviews := []models.Review{
		{
			BookID:             prideBook.ID,
			ReviewerName:       "Sarah Johnson",
			ReviewerEmail:      "sarah@example.com",
			Rating:             5,
			Title:              "Timeless Classic",
			Content:            "Austen's wit remains as sharp today as 200 years ago.",
			IsVerifiedPurchase: true,
			IsFeatured:         true,
			HelpfulVotes:       45,
			TotalVotes:         52,
			ReviewMetadata: models.JSON{
				"reading_time": "3 days",
				"format":       "paperback",
			},
		},
	}

	db.Create(&reviews)

	fmt.Println("✅ Seed data created successfully")
}

func setupRouter() *gin.Engine {
	r := gin.Default()

	// Add CORS middleware
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// Health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "healthy", "service": "Go GORM Bookstore API"})
	})

	// API routes
	api := r.Group("/api")
	{
		// Authors
		api.GET("/authors", getAuthors)
		api.POST("/authors", createAuthor)
		api.GET("/authors/:id", getAuthor)
		api.PUT("/authors/:id", updateAuthor)
		api.DELETE("/authors/:id", deleteAuthor)
		api.GET("/authors/stats", getAuthorStats)

		// Books
		api.GET("/books", getBooks)
		api.POST("/books", createBook)
		api.GET("/books/:id", getBook)
		api.PUT("/books/:id", updateBook)
		api.DELETE("/books/:id", deleteBook)
		api.GET("/books/search", searchBooks)
		api.POST("/books/:id/tags", addBookTag)

		// Reviews (nested under books)
		api.GET("/books/:id/reviews", getBookReviews)
		api.POST("/books/:id/reviews", createReview)
		api.POST("/books/:book_id/reviews/:id/helpful", markReviewHelpful)

		// Publishers
		api.GET("/publishers", getPublishers)
		api.GET("/publishers/:id", getPublisher)

		// Genres
		api.GET("/genres", getGenres)
		api.GET("/genres/:id/books", getGenreBooks)

		// Inventory
		api.GET("/inventory", getInventory)
		api.POST("/inventory/:id/restock", restockInventory)
	}

	return r
}

// Helper functions
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func stringPtr(s string) *string {
	return &s
}

func intPtr(i int) *int {
	return &i
}

func float64Ptr(f float64) *float64 {
	return &f
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func parseUint(s string) (uint, error) {
	id, err := strconv.ParseUint(s, 10, 32)
	return uint(id), err
}