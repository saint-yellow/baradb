package indexer

import (
	"bytes"
	"sort"

	"github.com/google/btree"
	"github.com/saint-yellow/baradb/data"
)

// An iterator of a B Tree indexer
type BTreeIterator struct {
	currentIndex int     // current iterated location
	reverse      bool    // whether enable reverse iteration
	values       []*Item // locations
}

func NewBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {
	var index int
	values := make([]*Item, tree.Len())

	saveValues := func(item btree.Item) bool {
		values[index] = item.(*Item)
		index++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	bti := &BTreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
	return bti
}

func (bti *BTreeIterator) Rewind() {
	bti.currentIndex = 0
}

func (bti *BTreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currentIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].Key, key) <= 0
		})
	} else {
		bti.currentIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].Key, key) >= 0
		})
	}
}

func (bti *BTreeIterator) Next() {
	bti.currentIndex += 1
}

func (bti *BTreeIterator) Valid() bool {
	return bti.currentIndex >= 0 && bti.currentIndex < len(bti.values)
}

func (bti *BTreeIterator) Key() []byte {
	return bti.values[bti.currentIndex].Key
}

func (bti *BTreeIterator) Value() *data.LogRecordPosition {
	return bti.values[bti.currentIndex].Position
}

func (bti *BTreeIterator) Size() int {
	return len(bti.values)
}

func (bti *BTreeIterator) Close() {
	bti.values = nil
}
