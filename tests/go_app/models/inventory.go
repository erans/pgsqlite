package models

import (
	"time"

	"github.com/lib/pq"
	"gorm.io/gorm"
)

// BookInventory represents inventory management for books
type BookInventory struct {
	ID                  uint           `json:"id" gorm:"primaryKey;autoIncrement"`
	BookID              UUID           `json:"book_id" gorm:"type:uuid;uniqueIndex;not null;constraint:OnDelete:CASCADE"`
	QuantityInStock     int            `json:"quantity_in_stock" gorm:"default:0;check:quantity_in_stock >= 0"`
	QuantityReserved    int            `json:"quantity_reserved" gorm:"default:0;check:quantity_reserved >= 0"`
	QuantitySold        int64          `json:"quantity_sold" gorm:"default:0;check:quantity_sold >= 0"`
	ReorderLevel        int            `json:"reorder_level" gorm:"default:10;check:reorder_level >= 0"`
	MaxStockLevel       int            `json:"max_stock_level" gorm:"default:1000;check:max_stock_level >= 0"`
	CostPrice           float64        `json:"cost_price" gorm:"type:decimal(10,2);not null;check:cost_price >= 0"`
	WholesalePrice      *float64       `json:"wholesale_price,omitempty" gorm:"type:decimal(10,2);check:wholesale_price >= 0"`
	WarehouseLocations  pq.StringArray `json:"warehouse_locations" gorm:"type:text[];default:'{}'"`
	SupplierCodes       pq.StringArray `json:"supplier_codes" gorm:"type:text[];default:'{}'"`
	SupplyChainData     JSON           `json:"supply_chain_data" gorm:"type:jsonb;default:'{}'"`
	InventoryNotes      *string        `json:"inventory_notes,omitempty" gorm:"type:text"`
	LastRestockedAt     *time.Time     `json:"last_restocked_at,omitempty"`
	LastSoldAt          *time.Time     `json:"last_sold_at,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`

	// Associations
	Book Book `json:"book,omitempty" gorm:"foreignKey:BookID"`
}

// TableName specifies the table name for BookInventory
func (BookInventory) TableName() string {
	return "book_inventories"
}

// BeforeCreate GORM hook
func (bi *BookInventory) BeforeCreate(tx *gorm.DB) error {
	// Ensure supply_chain_data is initialized
	if bi.SupplyChainData == nil {
		bi.SupplyChainData = JSON{}
	}
	return nil
}

// BeforeUpdate GORM hook - Add validation
func (bi *BookInventory) BeforeUpdate(tx *gorm.DB) error {
	// Validate that reserved quantity doesn't exceed stock
	if bi.QuantityReserved > bi.QuantityInStock {
		return gorm.ErrInvalidData
	}
	return nil
}

// GetAvailableStock returns the stock available for sale
func (bi *BookInventory) GetAvailableStock() int {
	return bi.QuantityInStock - bi.QuantityReserved
}

// NeedsReorder checks if inventory needs to be reordered
func (bi *BookInventory) NeedsReorder() bool {
	return bi.QuantityInStock <= bi.ReorderLevel
}

// GetStockStatus returns the stock status
func (bi *BookInventory) GetStockStatus() string {
	if bi.QuantityInStock == 0 {
		return "out_of_stock"
	}
	if bi.NeedsReorder() {
		return "low_stock"
	}
	return "in_stock"
}

// GetProfitMargin calculates the profit margin based on book price
func (bi *BookInventory) GetProfitMargin(db *gorm.DB) float64 {
	var book Book
	if err := db.First(&book, bi.BookID).Error; err != nil {
		return 0
	}

	if bi.CostPrice == 0 {
		return 0
	}

	return ((book.Price - bi.CostPrice) / bi.CostPrice) * 100
}

// Reserve reserves inventory for a sale
func (bi *BookInventory) Reserve(quantity int, db *gorm.DB) error {
	if bi.GetAvailableStock() < quantity {
		return gorm.ErrInvalidData
	}

	return db.Model(bi).Update("quantity_reserved", gorm.Expr("quantity_reserved + ?", quantity)).Error
}

// Sell processes a sale and updates inventory
func (bi *BookInventory) Sell(quantity int, db *gorm.DB) error {
	if bi.QuantityReserved < quantity {
		return gorm.ErrInvalidData
	}

	now := time.Now()
	return db.Model(bi).Updates(map[string]interface{}{
		"quantity_reserved": gorm.Expr("quantity_reserved - ?", quantity),
		"quantity_sold":     gorm.Expr("quantity_sold + ?", quantity),
		"last_sold_at":      &now,
	}).Error
}

// Restock adds inventory
func (bi *BookInventory) Restock(quantity int, db *gorm.DB) error {
	now := time.Now()
	return db.Model(bi).Updates(map[string]interface{}{
		"quantity_in_stock":  gorm.Expr("quantity_in_stock + ?", quantity),
		"last_restocked_at": &now,
	}).Error
}

// Scopes for common queries
func ScopeLowStock(db *gorm.DB) *gorm.DB {
	return db.Where("quantity_in_stock <= reorder_level")
}

func ScopeOutOfStock(db *gorm.DB) *gorm.DB {
	return db.Where("quantity_in_stock = 0")
}

func ScopeInStock(db *gorm.DB) *gorm.DB {
	return db.Where("quantity_in_stock > 0")
}

func ScopeByWarehouse(warehouse string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("warehouse_locations @> ?", pq.Array([]string{warehouse}))
	}
}

func ScopeNeedsReorder(db *gorm.DB) *gorm.DB {
	return db.Where("quantity_in_stock <= reorder_level")
}