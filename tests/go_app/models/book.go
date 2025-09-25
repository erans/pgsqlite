package models

import (
	"time"

	"github.com/lib/pq"
	"gorm.io/gorm"
)

// Book represents a book with comprehensive PostgreSQL features
type Book struct {
	ID                 UUID           `json:"id" gorm:"type:uuid;primaryKey;default:gen_random_uuid()"`
	Title              string         `json:"title" gorm:"not null;size:500"`
	Subtitle           *string        `json:"subtitle,omitempty" gorm:"size:500"`
	ISBN13             string         `json:"isbn_13" gorm:"uniqueIndex;not null;size:13"`
	ISBN10             *string        `json:"isbn_10,omitempty" gorm:"size:10"`
	Description        *string        `json:"description,omitempty" gorm:"type:text"`
	Summary            *string        `json:"summary,omitempty" gorm:"type:text"`
	Excerpt            *string        `json:"excerpt,omitempty" gorm:"type:text"`
	Price              float64        `json:"price" gorm:"type:decimal(10,2);not null;check:price >= 0"`
	DiscountPrice      *float64       `json:"discount_price,omitempty" gorm:"type:decimal(10,2);check:discount_price >= 0"`
	Pages              int            `json:"pages" gorm:"not null;check:pages > 0"`
	WeightGrams        *float64       `json:"weight_grams,omitempty" gorm:"type:decimal(8,2);check:weight_grams > 0"`
	AverageRating      *float64       `json:"average_rating,omitempty" gorm:"type:decimal(3,2);check:average_rating >= 0 AND average_rating <= 5"`
	PublicationDate    time.Time      `json:"publication_date" gorm:"type:date;not null"`
	FirstPublishedDate *time.Time     `json:"first_published_date,omitempty" gorm:"type:date"`
	LastReprintDate    *time.Time     `json:"last_reprint_date,omitempty" gorm:"type:date"`
	IsAvailable        bool           `json:"is_available" gorm:"default:true"`
	IsFeatured         bool           `json:"is_featured" gorm:"default:false"`
	IsBestseller       bool           `json:"is_bestseller" gorm:"default:false"`
	HasEbook           bool           `json:"has_ebook" gorm:"default:false"`
	HasAudiobook       bool           `json:"has_audiobook" gorm:"default:false"`
	Tags               pq.StringArray `json:"tags" gorm:"type:text[];default:'{}'"`
	Languages          pq.StringArray `json:"languages" gorm:"type:text[];default:'{}'"`
	Formats            pq.StringArray `json:"formats" gorm:"type:text[];default:'{}'"`
	Awards             pq.StringArray `json:"awards" gorm:"type:text[];default:'{}'"`
	ChapterTitles      pq.StringArray `json:"chapter_titles" gorm:"type:text[];default:'{}'"`
	Metadata           JSON           `json:"metadata" gorm:"type:jsonb;default:'{}'"`
	ReviewsSummary     JSON           `json:"reviews_summary" gorm:"type:jsonb;default:'{}'"`
	SalesData          JSON           `json:"sales_data" gorm:"type:jsonb;default:'{}'"`
	SearchData         JSON           `json:"search_data" gorm:"type:jsonb;default:'{}'"`
	AuthorID           uint           `json:"author_id" gorm:"not null;constraint:OnDelete:CASCADE"`
	PublisherID        *uint          `json:"publisher_id,omitempty" gorm:"constraint:OnDelete:SET NULL"`
	Status             BookStatus     `json:"status" gorm:"type:varchar(20);default:'draft';check:status IN ('draft', 'review', 'published', 'archived')"`
	Condition          BookCondition  `json:"condition" gorm:"type:varchar(20);default:'new';check:condition IN ('new', 'like_new', 'good', 'fair', 'poor')"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`

	// Associations
	Author        Author         `json:"author,omitempty" gorm:"foreignKey:AuthorID"`
	Publisher     *Publisher     `json:"publisher,omitempty" gorm:"foreignKey:PublisherID"`
	Reviews       []Review       `json:"reviews,omitempty" gorm:"foreignKey:BookID"`
	BookInventory *BookInventory `json:"book_inventory,omitempty" gorm:"foreignKey:BookID"`
	Genres        []Genre        `json:"genres,omitempty" gorm:"many2many:book_genres;"`
}

// TableName specifies the table name for Book
func (Book) TableName() string {
	return "books"
}

// BeforeCreate GORM hook
func (b *Book) BeforeCreate(tx *gorm.DB) error {
	// Generate UUID if not set
	if b.ID == (UUID{}) {
		b.ID = NewUUID()
	}

	// Ensure JSON fields are initialized
	if b.Metadata == nil {
		b.Metadata = JSON{}
	}
	if b.ReviewsSummary == nil {
		b.ReviewsSummary = JSON{}
	}
	if b.SalesData == nil {
		b.SalesData = JSON{}
	}
	if b.SearchData == nil {
		b.SearchData = JSON{}
	}

	return nil
}

// AfterCreate GORM hook
func (b *Book) AfterCreate(tx *gorm.DB) error {
	// Update search data after creation
	b.SearchData = JSON{
		"keywords":    []string{b.Title},
		"search_rank": 50,
	}
	return tx.Save(b).Error
}

// Business logic methods

// IsDiscounted checks if the book has a discount price
func (b *Book) IsDiscounted() bool {
	return b.DiscountPrice != nil && *b.DiscountPrice > 0
}

// GetEffectivePrice returns the price to use (discount price or regular price)
func (b *Book) GetEffectivePrice() float64 {
	if b.IsDiscounted() {
		return *b.DiscountPrice
	}
	return b.Price
}

// GetDiscountPercentage calculates the discount percentage
func (b *Book) GetDiscountPercentage() float64 {
	if !b.IsDiscounted() {
		return 0
	}
	return ((b.Price - *b.DiscountPrice) / b.Price) * 100
}

// IsClassic checks if the book is a classic (published before 1950)
func (b *Book) IsClassic() bool {
	return b.PublicationDate.Year() < 1950
}

// IsContemporary checks if the book is contemporary (published after 2000)
func (b *Book) IsContemporary() bool {
	return b.PublicationDate.Year() >= 2000
}

// HasTag checks if the book has a specific tag
func (b *Book) HasTag(tag string) bool {
	for _, t := range b.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// AddTag adds a tag to the book if it doesn't already exist
func (b *Book) AddTag(tag string) {
	if !b.HasTag(tag) {
		b.Tags = append(b.Tags, tag)
	}
}

// GORM Scopes for common queries

// ScopeAvailable returns available books
func ScopeAvailable(db *gorm.DB) *gorm.DB {
	return db.Where("is_available = ?", true)
}

// ScopePublished returns published books
func ScopePublished(db *gorm.DB) *gorm.DB {
	return db.Where("status = ?", BookStatusPublished)
}

// ScopeFeatured returns featured books
func ScopeFeatured(db *gorm.DB) *gorm.DB {
	return db.Where("is_featured = ?", true)
}

// ScopeBestsellers returns bestselling books
func ScopeBestsellers(db *gorm.DB) *gorm.DB {
	return db.Where("is_bestseller = ?", true)
}

// ScopeByAuthor returns books by a specific author
func ScopeByAuthor(authorID uint) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("author_id = ?", authorID)
	}
}

// ScopeByPriceRange returns books within a price range
func ScopeByPriceRange(minPrice, maxPrice float64) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("price BETWEEN ? AND ?", minPrice, maxPrice)
	}
}

// ScopeWithTag returns books that have a specific tag (PostgreSQL array operator)
func ScopeWithTag(tag string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("tags @> ?", pq.Array([]string{tag}))
	}
}

// ScopeWithGenre returns books with a specific genre in metadata (JSONB operator)
func ScopeWithGenre(genre string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("metadata->>'genre' = ?", genre)
	}
}

// ScopePopular returns books ordered by rating and review count
func ScopePopular(db *gorm.DB) *gorm.DB {
	return db.Order("average_rating DESC NULLS LAST").
		Order("(reviews_summary->>'total_reviews')::int DESC NULLS LAST")
}

// ScopeWithAssociations preloads common associations
func ScopeWithAssociations(db *gorm.DB) *gorm.DB {
	return db.Preload("Author").
		Preload("Publisher").
		Preload("Reviews").
		Preload("BookInventory").
		Preload("Genres")
}