package indexer

import (
	"bytes"

	"github.com/google/btree"
	"github.com/saint-yellow/baradb/data"
)

// Indexer Abstract interface of an indexer
type Indexer interface {
	// Put stores location of the corresponding data of the key in the indexer
	Put(key []byte, position *data.LogRecordPosition) bool

	// Get gets the location of the corresponding data af the key in the indexer
	Get(key []byte) *data.LogRecordPosition

	// Delete deletes the location of the corresponding data of the key in the indexer
	Delete(key []byte) bool

	// Iterator returns an iterator
	Iterator(reverse bool) Iterator
}

// Item
type Item struct {
	Key      []byte
	Position *data.LogRecordPosition
}

func (x *Item) Less(y btree.Item) bool {
	return bytes.Compare(x.Key, y.(*Item).Key) == -1
}

// IndexerType enum
type IndexerType = int8

const (
	Btree  IndexerType = iota + 1 // Btree B Tree indexer
	ARtree                        // ARtree Adaptive Radix Tree indexer
)

// NewIndexer A simple factory menthod for creating an indexer
func NewIndexer(t IndexerType) Indexer {
	switch t {
	case Btree:
		return NewBTree()
	case ARtree:
		return nil
	default:
		panic("Unsupported index type")
	}
}
