package paging

import (
	"fmt"

	"github.com/jinzhu/gorm"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// -----------------------------------------------------------------------------
// Interfaces
// -----------------------------------------------------------------------------

// Store is a store.
type Store interface {
	PaginateOffset(limit, offset int64) (int64, error)
	PaginateCursor(limit int64, cursor interface{}, fieldName string, reverse bool) error
	GetItems() interface{}
}

// -----------------------------------------------------------------------------
// GORM Store
// -----------------------------------------------------------------------------

// GORMStore is the store for GORM ORM.
type GORMStore struct {
	db    *gorm.DB
	items interface{}
}

// NewGORMStore returns a new GORM store instance.
func NewGORMStore(db *gorm.DB, items interface{}) (*GORMStore, error) {
	return &GORMStore{
		db:    db,
		items: items,
	}, nil
}

// GetItems return the current result
func (s *GORMStore) GetItems() interface{} {
	return s.items
}

// PaginateOffset paginates items from the store and update page instance.
func (s *GORMStore) PaginateOffset(limit, offset int64) (int64, error) {
	q := s.db
	q = q.Limit(int(limit))
	q = q.Offset(int(offset))
	q = q.Find(s.items)
	q = q.Limit(-1)
	q = q.Offset(-1)

	var count int64
	if err := q.Count(&count).Error; err != nil {
		return count, err
	}

	return count, nil
}

// PaginateCursor paginates items from the store and update page instance for cursor pagination system.
// cursor can be an ID or a date (time.Time)
func (s *GORMStore) PaginateCursor(limit int64, cursor interface{}, fieldName string, reverse bool) error {
	q := s.db

	q = q.Limit(int(limit))

	if reverse {
		q = q.Where(fmt.Sprintf("%s < ?", fieldName), cursor)
	} else {
		q = q.Where(fmt.Sprintf("%s > ?", fieldName), cursor)
	}

	q = q.Find(s.items)
	return q.Error
}

// -----------------------------------------------------------------------------
// Mongo Store
// -----------------------------------------------------------------------------

// MGOStore is the store for mongodb
type MGOStore struct {
	collection *mgo.Collection
	filter     bson.M
	items      interface{}
}

// NewMGOStore returns a new mongo store
func NewMGOStore(collection *mgo.Collection, items interface{}, filter bson.M) (*MGOStore, error) {
	return &MGOStore{
		collection,
		filter,
		items,
	}, nil
}

// GetItems return the current result
func (m *MGOStore) GetItems() interface{} {
	return m.items
}

// PaginateOffset paginates items from the store and update page instance.
func (m *MGOStore) PaginateOffset(limit, offset int64) (int64, error) {
	q := m.collection.Find(m.filter)

	recordCount, _ := q.Count()
	c := int64(recordCount)

	q.Limit(int(limit))
	q.Skip(int(offset))
	return c, q.All(m.items)
}

// PaginateCursor paginates items from the store and update page instance for cursor pagination system.
// cursor can be an ID or a date (time.Time)
func (m *MGOStore) PaginateCursor(limit int64, cursor interface{}, fieldName string, reverse bool) error {
	operator := "$gt"
	if reverse {
		operator = "$lt"
	}

	m.filter[fieldName] = bson.M{operator: cursor}
	q := m.collection.Find(m.filter)
	q.Limit(int(limit))

	return q.All(m.items)
}
