package indexer

import (
	"sync"

	"github.com/google/btree"
	"github.com/saint-yellow/baradb/data"
)

// BTree A concrete indexer that implements Indexer interface by ecapsulating Google's BTree library
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// Constructor of BTree
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

// Put data to BTree
func (bt *BTree) Put(key []byte, position *data.LogRecordPosition) bool {
	x := &Item{
		key:      key,
		position: position,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(x)
	return true
}

// Get data from BTree
func (bt *BTree) Get(key []byte) *data.LogRecordPosition {
	x := &Item{
		key: key,
	}
	y := bt.tree.Get(x)
	if y == nil {
		return nil
	}
	return y.(*Item).position
}

// Delete data from BTree
func (bt *BTree) Delete(key []byte) bool {
	x := &Item{
		key: key,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	y := bt.tree.Delete(x)
	return y != nil
}
