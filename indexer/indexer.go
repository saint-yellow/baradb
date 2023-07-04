package indexer

import (
	"bytes"

	"github.com/google/btree"

	"github.com/saint-yellow/baradb/data"
)

// Indexer Abstract interface of an indexer
type Indexer interface {
	// Put stores location of the corresponding data of the key in the indexer
	Put([]byte, *data.LogRecordPosition) *data.LogRecordPosition

	// Get gets the location of the corresponding data af the key in the indexer
	Get([]byte) *data.LogRecordPosition

	// Delete deletes the location of the corresponding data of the key in the indexer
	Delete([]byte) (*data.LogRecordPosition, bool)

	// Size returns how many key/value pairs in the indexer
	Size() int

	// Iterator returns an iterator
	Iterator(bool) Iterator

	// Close closes the indexer
	Close() error
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
	BPtree                        // BPtree B+ Tree indexer
)

// NewIndexer A simple factory menthod for creating an indexer
func NewIndexer(t IndexerType, d string, syncWrites bool) Indexer {
	switch t {
	case Btree:
		return newBTree()
	case ARtree:
		return newARTree()
	case BPtree:
		return newBPlusTree(d, syncWrites)
	default:
		panic("Unsupported index type")
	}
}
