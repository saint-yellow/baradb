package index

import (
	"bytes"

	"github.com/google/btree"
	"github.com/saint-yellow/baradb/data"
)

// Indexer Abstract interface of an indexer
type Indexer interface {
	Put(key []byte, position *data.LogRecordPosition) bool
	Get(key []byte) *data.LogRecordPosition
	Delete(key []byte) bool
}

// Item
type Item struct {
	key      []byte
	position *data.LogRecordPosition
}

func (x *Item) Less(y btree.Item) bool {
	return bytes.Compare(x.key, y.(*Item).key) == -1
}

type IndexerType = int8

const (
	Btree  IndexerType = iota + 1 // Btree BTree indexer
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
