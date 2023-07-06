package index

import (
	"bytes"
	"sync"

	"github.com/google/btree"

	"github.com/saint-yellow/baradb/data"
)

// bTreeItem represents a single object in a B tree
//
// It combines a log record's key and position.
// And it implements [btree.Item](github.com/google/btree) interface
type bTreeItem struct {
	key      []byte                  // The key of the log record
	position *data.LogRecordPosition // The position of the log record
}

func (x *bTreeItem) Less(y btree.Item) bool {
	return bytes.Compare(x.key, y.(*bTreeItem).key) == -1
}

// BTree A concrete index that implements Index interface by ecapsulating Google's BTree library
type bTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// Constructor of BTree
func newBTree() *bTree {
	bt := &bTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
	return bt
}

// Put puts a given key and its corresponding position to a B tree index
//
// It returns the old postion if the given key already exists in the index and nil otherwise.
func (bt *bTree) Put(key []byte, position *data.LogRecordPosition) *data.LogRecordPosition {
	x := &bTreeItem{
		key:      key,
		position: position,
	}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(x)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*bTreeItem).position
}

// Get returns corresponding position of the given key from the index
func (bt *bTree) Get(key []byte) *data.LogRecordPosition {
	x := &bTreeItem{
		key: key,
	}
	y := bt.tree.Get(x)
	if y == nil {
		return nil
	}
	return y.(*bTreeItem).position
}

// Size returns how many key/value pairs in the BTree
func (bt *bTree) Size() int {
	return bt.tree.Len()
}

// Delete deletes a key from the B tree index
func (bt *bTree) Delete(key []byte) (*data.LogRecordPosition, bool) {
	x := &bTreeItem{
		key: key,
	}
	bt.lock.Lock()
	y := bt.tree.Delete(x)
	bt.lock.Unlock()
	if y == nil {
		return nil, false
	}
	return y.(*bTreeItem).position, true
}

func (bt *bTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}

	bt.lock.Lock()
	defer bt.lock.Unlock()

	return newBTreeIterator(bt.tree, reverse)
}

func (bt *bTree) Close() error {
	return nil
}
