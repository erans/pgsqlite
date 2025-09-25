package models

import (
	"time"

	"gorm.io/gorm"
)

// Genre represents a book genre with hierarchical structure
type Genre struct {
	ID          uint      `json:"id" gorm:"primaryKey;autoIncrement"`
	Name        string    `json:"name" gorm:"uniqueIndex;not null;size:255"`
	Description *string   `json:"description,omitempty" gorm:"type:text"`
	ParentID    *uint     `json:"parent_id,omitempty" gorm:"constraint:OnDelete:SET NULL"`
	IsActive    bool      `json:"is_active" gorm:"default:true"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Self-referential associations
	Parent   *Genre  `json:"parent,omitempty" gorm:"foreignKey:ParentID"`
	Children []Genre `json:"children,omitempty" gorm:"foreignKey:ParentID"`

	// Many-to-many with books
	Books []Book `json:"books,omitempty" gorm:"many2many:book_genres;"`
}

// TableName specifies the table name for Genre
func (Genre) TableName() string {
	return "genres"
}

// IsRoot checks if this is a root genre (no parent)
func (g *Genre) IsRoot() bool {
	return g.ParentID == nil
}

// IsLeaf checks if this is a leaf genre (no children)
func (g *Genre) IsLeaf(db *gorm.DB) bool {
	var count int64
	db.Model(&Genre{}).Where("parent_id = ?", g.ID).Count(&count)
	return count == 0
}

// GetPath returns the path from root to this genre
func (g *Genre) GetPath(db *gorm.DB) string {
	if g.IsRoot() {
		return g.Name
	}

	var parent Genre
	if err := db.First(&parent, g.ParentID).Error; err != nil {
		return g.Name
	}

	return parent.GetPath(db) + " > " + g.Name
}

// GetBookCount returns the number of books in this genre
func (g *Genre) GetBookCount(db *gorm.DB) int64 {
	var count int64
	db.Table("book_genres").Where("genre_id = ?", g.ID).Count(&count)
	return count
}

// Scopes for common queries
func ScopeActiveGenres(db *gorm.DB) *gorm.DB {
	return db.Where("is_active = ?", true)
}

func ScopeRootGenres(db *gorm.DB) *gorm.DB {
	return db.Where("parent_id IS NULL")
}

func ScopeChildGenres(db *gorm.DB) *gorm.DB {
	return db.Where("parent_id IS NOT NULL")
}

func ScopeWithChildren(db *gorm.DB) *gorm.DB {
	return db.Preload("Children")
}

func ScopeWithParent(db *gorm.DB) *gorm.DB {
	return db.Preload("Parent")
}