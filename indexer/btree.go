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

// Put puts data to B tree indexer
func (bt *BTree) Put(key []byte, position *data.LogRecordPosition) *data.LogRecordPosition {
	x := &Item{
		Key:      key,
		Position: position,
	}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(x)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).Position
}

// Get data from BTree
func (bt *BTree) Get(key []byte) *data.LogRecordPosition {
	x := &Item{
		Key: key,
	}
	y := bt.tree.Get(x)
	if y == nil {
		return nil
	}
	return y.(*Item).Position
}

// Size returns how many key/value pairs in the BTree
func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// Delete deletes a key from the B tree indexer
func (bt *BTree) Delete(key []byte) (*data.LogRecordPosition, bool) {
	x := &Item{
		Key: key,
	}
	bt.lock.Lock()
	y := bt.tree.Delete(x)
	bt.lock.Unlock()
	if y == nil {
		return nil, false
	}
	return y.(*Item).Position, true
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}

	bt.lock.Lock()
	defer bt.lock.Unlock()

	return NewBTreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
}
