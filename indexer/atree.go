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
func (t *ARTree) Put(key []byte, position *data.LogRecordPosition) *data.LogRecordPosition {
	t.lock.Lock()
	oldValue, updated := t.tree.Insert(key, position)
	t.lock.Unlock()

	var lrp *data.LogRecordPosition
	if updated {
		lrp = oldValue.(*data.LogRecordPosition)
	}
	return lrp
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
func (t *ARTree) Delete(key []byte) (*data.LogRecordPosition, bool) {
	t.lock.Lock()
	oldValue, deleted := t.tree.Delete(key)
	t.lock.Unlock()

	var lrp *data.LogRecordPosition
	if deleted {
		lrp = oldValue.(*data.LogRecordPosition)
	}
	return lrp, deleted
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
