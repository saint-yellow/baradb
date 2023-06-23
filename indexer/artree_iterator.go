package indexer

import (
	"bytes"
	"sort"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/saint-yellow/baradb/data"
)

// An iterator of a Adaptive Radix Tree indexer
type ARTreeIterator struct {
	currentIndex int     // current iterated location
	reverse      bool    // whether enable reverse iteration
	values       []*Item // locations
}

func NewARTreeIterator(tree art.Tree, reverse bool) *ARTreeIterator {
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

	iter := &ARTreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
	return iter
}

func (iter *ARTreeIterator) Rewind() {
	iter.currentIndex = 0
}

func (iter *ARTreeIterator) Seek(key []byte) {
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

func (iter *ARTreeIterator) Next() {
	iter.currentIndex += 1
}

func (iter *ARTreeIterator) Valid() bool {
	return iter.currentIndex >= 0 && iter.currentIndex < len(iter.values)
}

func (iter *ARTreeIterator) Key() []byte {
	return iter.values[iter.currentIndex].Key
}

func (iter *ARTreeIterator) Value() *data.LogRecordPosition {
	return iter.values[iter.currentIndex].Position
}

func (iter *ARTreeIterator) Close() {
	iter.values = nil
}
