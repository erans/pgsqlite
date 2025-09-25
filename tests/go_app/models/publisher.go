package models

import (
	"time"

	"gorm.io/gorm"
)

// Publisher represents a publishing company
type Publisher struct {
	ID           uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	Name         string    `json:"name" gorm:"not null;size:255"`
	FoundedYear  *int      `json:"founded_year,omitempty"`
	Headquarters *string   `json:"headquarters,omitempty" gorm:"size:255"`
	WebsiteURL   *string   `json:"website_url,omitempty" gorm:"size:500"`
	IsActive     bool      `json:"is_active" gorm:"default:true"`
	ContactInfo  JSON      `json:"contact_info" gorm:"type:jsonb;default:'{}'"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`

	// Associations
	Books []Book `json:"books,omitempty" gorm:"foreignKey:PublisherID"`
}

// TableName specifies the table name for Publisher
func (Publisher) TableName() string {
	return "publishers"
}

// BeforeCreate GORM hook
func (p *Publisher) BeforeCreate(tx *gorm.DB) error {
	// Ensure contact_info is initialized
	if p.ContactInfo == nil {
		p.ContactInfo = JSON{}
	}
	return nil
}

// GetAge returns the age of the publisher
func (p *Publisher) GetAge() *int {
	if p.FoundedYear == nil {
		return nil
	}
	age := time.Now().Year() - *p.FoundedYear
	return &age
}

// GetBookCount returns the number of books published by this publisher
func (p *Publisher) GetBookCount(db *gorm.DB) int64 {
	var count int64
	db.Model(&Book{}).Where("publisher_id = ?", p.ID).Count(&count)
	return count
}

// Scopes for common queries
func ScopeActivePublishers(db *gorm.DB) *gorm.DB {
	return db.Where("is_active = ?", true)
}

func ScopeFoundedAfter(year int) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("founded_year > ?", year)
	}
}