package indexer

import (
	"sync"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/saint-yellow/baradb/data"
)

// arTree Adaptive Ratio Tree
type arTree struct {
	tree art.Tree
	lock *sync.RWMutex
}

// NewarTree initializes an Adaptive Radix Tree indexer
func newARTree() *arTree {
	t := &arTree{
		tree: art.New(),
		lock: new(sync.RWMutex),
	}
	return t
}

// Put stores location of the corresponding data of the key in the indexer
func (t *arTree) Put(key []byte, position *data.LogRecordPosition) *data.LogRecordPosition {
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
func (t *arTree) Get(key []byte) *data.LogRecordPosition {
	t.lock.RLock()
	defer t.lock.RUnlock()

	value, found := t.tree.Search(key)
	if !found {
		return nil
	}

	return value.(*data.LogRecordPosition)
}

// Delete deletes the location of the corresponding data of the key in the indexer
func (t *arTree) Delete(key []byte) (*data.LogRecordPosition, bool) {
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
func (t *arTree) Size() int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.tree.Size()
}

// Iterator returns an iterator
func (t *arTree) Iterator(reverse bool) Iterator {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return newARTreeIterator(t.tree, reverse)
}

func (t *arTree) Close() error {
	return nil
}
