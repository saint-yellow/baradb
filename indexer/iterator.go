package indexer

import "github.com/saint-yellow/baradb/data"

// An abstract interator of an indexer
type Iterator interface {
	Rewind() // Rewind resets the iteration

	Seek([]byte)

	Next()

	Valid() bool

	Key() []byte

	Value() *data.LogRecordPosition

	Close() // Close closes the iterator
}
