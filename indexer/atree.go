package indexer

import (
	"sync"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/saint-yellow/baradb/data"
)

// ARTree Adaptive Ratio Tree
type ARTree struct {
	tree art.Tree
	lock *sync.RWMutex
}

// NewARTree initializes an Adaptive Radix Tree indexer
func NewARTree() *ARTree {
	t := &ARTree{
		tree: art.New(),
		lock: new(sync.RWMutex),
	}
	return t
}

// Put stores location of the corresponding data of the key in the indexer
func (t *ARTree) Put(key []byte, position *data.LogRecordPosition) bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.tree.Insert(key, position)
	return true
}

// Get gets the location of the corresponding data af the key in the indexer
func (t *ARTree) Get(key []byte) *data.LogRecordPosition {
	t.lock.RLock()
	defer t.lock.RUnlock()

	value, found := t.tree.Search(key)
	if !found {
		return nil
	}

	return value.(*data.LogRecordPosition)
}

// Delete deletes the location of the corresponding data of the key in the indexer
func (t *ARTree) Delete(key []byte) bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	_, deleted := t.tree.Delete(key)
	return deleted
}

// Size returns how many key/value pairs in the indexer
func (t *ARTree) Size() int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.tree.Size()
}

// Iterator returns an iterator
func (t *ARTree) Iterator(reverse bool) Iterator {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return NewARTreeIterator(t.tree, reverse)
}

func (t *ARTree) Close() error {
	return nil
}
