package models

import (
	"time"

	"github.com/lib/pq"
	"gorm.io/gorm"
)

// Author represents an author with GORM auto-increment ID and PostgreSQL features
type Author struct {
	ID          uint           `json:"id" gorm:"primaryKey;autoIncrement"`
	Name        string         `json:"name" gorm:"not null;size:255"`
	Email       string         `json:"email" gorm:"uniqueIndex;not null;size:255"`
	Bio         *string        `json:"bio,omitempty" gorm:"type:text"`
	BirthDate   *time.Time     `json:"birth_date,omitempty" gorm:"type:date"`
	Nationality *string        `json:"nationality,omitempty" gorm:"size:100"`
	IsActive    bool           `json:"is_active" gorm:"default:true"`
	Metadata    JSON           `json:"metadata" gorm:"type:jsonb;default:'{}'"`
	SocialLinks pq.StringArray `json:"social_links" gorm:"type:text[];default:'{}'"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`

	// Associations
	Books []Book `json:"books,omitempty" gorm:"foreignKey:AuthorID"`
}

// TableName specifies the table name for Author
func (Author) TableName() string {
	return "authors"
}

// BeforeCreate GORM hook
func (a *Author) BeforeCreate(tx *gorm.DB) error {
	// Ensure metadata is initialized
	if a.Metadata == nil {
		a.Metadata = JSON{}
	}
	return nil
}

// AfterFind GORM hook
func (a *Author) AfterFind(tx *gorm.DB) error {
	// Post-processing after loading from database
	return nil
}

// GetFullName returns the author's full name
func (a *Author) GetFullName() string {
	return a.Name
}

// GetBookCount returns the number of books by this author
func (a *Author) GetBookCount(db *gorm.DB) int64 {
	var count int64
	db.Model(&Book{}).Where("author_id = ?", a.ID).Count(&count)
	return count
}

// Scopes for common queries
func ScopeActive(db *gorm.DB) *gorm.DB {
	return db.Where("is_active = ?", true)
}

func ScopeByNationality(nationality string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("nationality = ?", nationality)
	}
}

func ScopeWithBooks(db *gorm.DB) *gorm.DB {
	return db.Preload("Books")
}