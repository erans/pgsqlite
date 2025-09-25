package models

import (
	"database/sql/driver"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

// JSON is a custom GORM type for handling JSON/JSONB fields
type JSON map[string]interface{}

// Scan implements the sql.Scanner interface for JSON
func (j *JSON) Scan(value interface{}) error {
	if value == nil {
		*j = JSON{}
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	if len(bytes) == 0 {
		*j = JSON{}
		return nil
	}

	return json.Unmarshal(bytes, j)
}

// Value implements the driver.Valuer interface for JSON
func (j JSON) Value() (driver.Value, error) {
	if j == nil {
		return "{}", nil
	}
	return json.Marshal(j)
}

// UUID is a custom type for UUID handling with GORM
type UUID uuid.UUID

// Scan implements the sql.Scanner interface for UUID
func (u *UUID) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case string:
		parsed, err := uuid.Parse(v)
		if err != nil {
			return err
		}
		*u = UUID(parsed)
	case []byte:
		parsed, err := uuid.Parse(string(v))
		if err != nil {
			return err
		}
		*u = UUID(parsed)
	default:
		return errors.New("cannot scan UUID from this type")
	}

	return nil
}

// Value implements the driver.Valuer interface for UUID
func (u UUID) Value() (driver.Value, error) {
	if u == (UUID{}) {
		return nil, nil
	}
	return uuid.UUID(u).String(), nil
}

// String returns string representation of UUID
func (u UUID) String() string {
	return uuid.UUID(u).String()
}

// MarshalJSON implements json.Marshaler
func (u UUID) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.String())
}

// UnmarshalJSON implements json.Unmarshaler
func (u *UUID) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}

	parsed, err := uuid.Parse(str)
	if err != nil {
		return err
	}

	*u = UUID(parsed)
	return nil
}

// NewUUID generates a new UUID
func NewUUID() UUID {
	return UUID(uuid.New())
}

// BookStatus represents the status enum for books
type BookStatus string

const (
	BookStatusDraft     BookStatus = "draft"
	BookStatusReview    BookStatus = "review"
	BookStatusPublished BookStatus = "published"
	BookStatusArchived  BookStatus = "archived"
)

// BookCondition represents the condition enum for books
type BookCondition string

const (
	BookConditionNew     BookCondition = "new"
	BookConditionLikeNew BookCondition = "like_new"
	BookConditionGood    BookCondition = "good"
	BookConditionFair    BookCondition = "fair"
	BookConditionPoor    BookCondition = "poor"
)