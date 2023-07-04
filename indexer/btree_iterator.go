package indexer

import (
	"bytes"
	"sort"

	"github.com/google/btree"

	"github.com/saint-yellow/baradb/data"
)

// An iterator of a B Tree indexer
type bTreeIterator struct {
	currentIndex int     // current iterated location
	reverse      bool    // whether enable reverse iteration
	values       []*Item // locations
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *bTreeIterator {
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

	iter := &bTreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
	return iter
}

func (iter *bTreeIterator) Rewind() {
	iter.currentIndex = 0
}

func (iter *bTreeIterator) Seek(key []byte) {
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

func (iter *bTreeIterator) Next() {
	iter.currentIndex += 1
}

func (iter *bTreeIterator) Valid() bool {
	return iter.currentIndex >= 0 && iter.currentIndex < len(iter.values)
}

func (iter *bTreeIterator) Key() []byte {
	return iter.values[iter.currentIndex].Key
}

func (iter *bTreeIterator) Value() *data.LogRecordPosition {
	return iter.values[iter.currentIndex].Position
}

func (iter *bTreeIterator) Size() int {
	return len(iter.values)
}

func (iter *bTreeIterator) Close() {
	iter.values = nil
}
