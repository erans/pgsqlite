package models

import (
	"time"

	"gorm.io/gorm"
)

// Review represents a book review
type Review struct {
	ID                   UUID      `json:"id" gorm:"type:uuid;primaryKey;default:gen_random_uuid()"`
	BookID               UUID      `json:"book_id" gorm:"type:uuid;not null;constraint:OnDelete:CASCADE"`
	ReviewerName         string    `json:"reviewer_name" gorm:"not null;size:255"`
	ReviewerEmail        string    `json:"reviewer_email" gorm:"not null;size:255"`
	Rating               int       `json:"rating" gorm:"not null;check:rating >= 1 AND rating <= 5"`
	Title                string    `json:"title" gorm:"not null;size:500"`
	Content              string    `json:"content" gorm:"not null;type:text"`
	IsVerifiedPurchase   bool      `json:"is_verified_purchase" gorm:"default:false"`
	IsFeatured           bool      `json:"is_featured" gorm:"default:false"`
	IsHelpful            bool      `json:"is_helpful" gorm:"default:false"`
	HelpfulVotes         int       `json:"helpful_votes" gorm:"default:0;check:helpful_votes >= 0"`
	TotalVotes           int       `json:"total_votes" gorm:"default:0;check:total_votes >= 0"`
	ReviewMetadata       JSON      `json:"review_metadata" gorm:"type:jsonb;default:'{}'"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`

	// Associations
	Book Book `json:"book,omitempty" gorm:"foreignKey:BookID"`
}

// TableName specifies the table name for Review
func (Review) TableName() string {
	return "reviews"
}

// BeforeCreate GORM hook
func (r *Review) BeforeCreate(tx *gorm.DB) error {
	// Generate UUID if not set
	if r.ID == (UUID{}) {
		r.ID = NewUUID()
	}

	// Ensure review_metadata is initialized
	if r.ReviewMetadata == nil {
		r.ReviewMetadata = JSON{}
	}

	return nil
}

// AfterCreate GORM hook - Update book's review summary
func (r *Review) AfterCreate(tx *gorm.DB) error {
	return r.updateBookReviewSummary(tx)
}

// AfterUpdate GORM hook - Update book's review summary
func (r *Review) AfterUpdate(tx *gorm.DB) error {
	return r.updateBookReviewSummary(tx)
}

// AfterDelete GORM hook - Update book's review summary
func (r *Review) AfterDelete(tx *gorm.DB) error {
	return r.updateBookReviewSummary(tx)
}

// updateBookReviewSummary updates the book's review summary
func (r *Review) updateBookReviewSummary(tx *gorm.DB) error {
	var reviewCount int64
	var avgRating float64

	// Count reviews for this book
	tx.Model(&Review{}).Where("book_id = ?", r.BookID).Count(&reviewCount)

	// Calculate average rating
	type Result struct {
		AvgRating float64
	}
	var result Result
	tx.Model(&Review{}).
		Select("AVG(rating) as avg_rating").
		Where("book_id = ?", r.BookID).
		Scan(&result)

	avgRating = result.AvgRating

	// Update book's review summary and average rating
	reviewsSummary := JSON{
		"total_reviews":   reviewCount,
		"average_rating":  avgRating,
		"last_updated":    time.Now(),
	}

	return tx.Model(&Book{}).
		Where("id = ?", r.BookID).
		Updates(map[string]interface{}{
			"reviews_summary": reviewsSummary,
			"average_rating":  avgRating,
		}).Error
}

// GetHelpfulPercentage calculates the percentage of helpful votes
func (r *Review) GetHelpfulPercentage() float64 {
	if r.TotalVotes == 0 {
		return 0
	}
	return (float64(r.HelpfulVotes) / float64(r.TotalVotes)) * 100
}

// IsVerified checks if this is a verified purchase review
func (r *Review) IsVerified() bool {
	return r.IsVerifiedPurchase
}

// MarkHelpful increments the helpful votes
func (r *Review) MarkHelpful(db *gorm.DB) error {
	return db.Model(r).Updates(map[string]interface{}{
		"helpful_votes": gorm.Expr("helpful_votes + 1"),
		"total_votes":   gorm.Expr("total_votes + 1"),
	}).Error
}

// MarkNotHelpful increments only the total votes
func (r *Review) MarkNotHelpful(db *gorm.DB) error {
	return db.Model(r).Update("total_votes", gorm.Expr("total_votes + 1")).Error
}

// Scopes for common queries
func ScopeVerifiedReviews(db *gorm.DB) *gorm.DB {
	return db.Where("is_verified_purchase = ?", true)
}

func ScopeFeaturedReviews(db *gorm.DB) *gorm.DB {
	return db.Where("is_featured = ?", true)
}

func ScopeHelpfulReviews(db *gorm.DB) *gorm.DB {
	return db.Where("is_helpful = ?", true)
}

func ScopeByRating(rating int) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("rating = ?", rating)
	}
}

func ScopeRecentReviews(db *gorm.DB) *gorm.DB {
	return db.Order("created_at DESC")
}

func ScopeByBook(bookID UUID) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("book_id = ?", bookID)
	}
}