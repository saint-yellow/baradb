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

	iter := &BTreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
	return iter
}

func (iter *BTreeIterator) Rewind() {
	iter.currentIndex = 0
}

func (iter *BTreeIterator) Seek(key []byte) {
	if iter.reverse {
		iter.currentIndex = sort.Search(len(iter.values), func(i int) bool {
			return bytes.Compare(iter.values[i].Key, key) <= 0
		})
	} else {
		iter.currentIndex = sort.Search(len(iter.values), func(i int) bool {
			return bytes.Compare(iter.values[i].Key, key) >= 0
		})
	}
}

func (iter *BTreeIterator) Next() {
	iter.currentIndex += 1
}

func (iter *BTreeIterator) Valid() bool {
	return iter.currentIndex >= 0 && iter.currentIndex < len(iter.values)
}

func (iter *BTreeIterator) Key() []byte {
	return iter.values[iter.currentIndex].Key
}

func (iter *BTreeIterator) Value() *data.LogRecordPosition {
	return iter.values[iter.currentIndex].Position
}

func (iter *BTreeIterator) Size() int {
	return len(iter.values)
}

func (iter *BTreeIterator) Close() {
	iter.values = nil
}
