package index

import (
	"github.com/saint-yellow/baradb/data"
)

// An abstract interator of an index
type Iterator interface {
	Rewind() // Rewind resets the iteration

	Seek([]byte)

	Next()

	Valid() bool

	Key() []byte

	Value() *data.LogRecordPosition

	Close() // Close closes the iterator
}
