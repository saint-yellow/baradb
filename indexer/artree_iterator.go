package indexer

import (
	"bytes"
	"sort"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/saint-yellow/baradb/data"
)

// An iterator of a Adaptive Radix Tree indexer
type arTreeIterator struct {
	currentIndex int     // current iterated location
	reverse      bool    // whether enable reverse iteration
	values       []*Item // locations
}

func newARTreeIterator(tree art.Tree, reverse bool) *arTreeIterator {
	var index int
	if reverse {
		index = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())

	saveValues := func(node art.Node) bool {
		item := &Item{
			Key:      node.Key(),
			Position: node.Value().(*data.LogRecordPosition),
		}

		values[index] = item

		if reverse {
			index--
		} else {
			index++
		}

		return true
	}

	tree.ForEach(saveValues)

	iter := &arTreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
	return iter
}

func (iter *arTreeIterator) Rewind() {
	iter.currentIndex = 0
}

func (iter *arTreeIterator) Seek(key []byte) {
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

func (iter *arTreeIterator) Next() {
	iter.currentIndex += 1
}

func (iter *arTreeIterator) Valid() bool {
	return iter.currentIndex >= 0 && iter.currentIndex < len(iter.values)
}

func (iter *arTreeIterator) Key() []byte {
	return iter.values[iter.currentIndex].Key
}

func (iter *arTreeIterator) Value() *data.LogRecordPosition {
	return iter.values[iter.currentIndex].Position
}

func (iter *arTreeIterator) Close() {
	iter.values = nil
}
