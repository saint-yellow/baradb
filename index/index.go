package index

import (
	"github.com/saint-yellow/baradb/data"
)

// Index repersents abstract interface of an in-memory index
//
// An index stores key/value pairs.
// A key is the key of a log record, and a value is a log record's position in a data file in a disk.
type Index interface {
	// Put stores a given key and its value in the index
	//
	// It may replaces the old value if the given already exists
	//
	// It returns the old value if the given key already exists and nil otherwise
	Put([]byte, *data.LogRecordPosition) *data.LogRecordPosition

	// Get returns the value of the given key from the index
	//
	// It returns a value if the given key already exists and nil otherwise
	Get([]byte) *data.LogRecordPosition

	// Delete deletes the value of the given key in the index
	//
	// It returns the deleted value and a boolean value true if the key exists
	// Otherwise, it returns nil and a boolean value false
	Delete([]byte) (*data.LogRecordPosition, bool)

	// Size returns how many key/value pairs in the index
	Size() int

	// Iterator creates an iterator of the index
	//
	// The iterator supports reversed iteration if the given boolean value is true
	Iterator(bool) Iterator

	// Close closes the index
	Close() error
}

// IndexType enum
type IndexType = int8

const (
	Btree  IndexType = iota + 1 // Btree B Tree index
	ARtree                        // ARtree Adaptive Radix Tree index
	BPtree                        // BPtree B+ Tree index
)

// New A simple factory menthod for creating an index
func New(t IndexType, d string, syncWrites bool) Index {
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
