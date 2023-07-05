package indexer

import (
	"github.com/saint-yellow/baradb/data"
)

// Indexer repersents abstract interface of an in-memory indexer
//
// An indexer stores key/value pairs.
// A key is the key of a log record, and a value is a log record's position in a data file in a disk.
type Indexer interface {
	// Put stores a given key and its value in the indexer
	//
	// It may replaces the old value if the given already exists
	//
	// It returns the old value if the given key already exists and nil otherwise
	Put([]byte, *data.LogRecordPosition) *data.LogRecordPosition

	// Get returns the value of the given key from the indexer
	//
	// It returns a value if the given key already exists and nil otherwise
	Get([]byte) *data.LogRecordPosition

	// Delete deletes the value of the given key in the indexer
	//
	// It returns the deleted value and a boolean value true if the key exists
	// Otherwise, it returns nil and a boolean value false
	Delete([]byte) (*data.LogRecordPosition, bool)

	// Size returns how many key/value pairs in the indexer
	Size() int

	// Iterator creates an iterator of the indexer
	//
	// The iterator supports reversed iteration if the given boolean value is true
	Iterator(bool) Iterator

	// Close closes the indexer
	Close() error
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
