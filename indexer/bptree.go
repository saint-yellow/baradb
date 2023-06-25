package indexer

import (
	"path/filepath"

	"go.etcd.io/bbolt"

	"github.com/saint-yellow/baradb/data"
)

const BPlusTreeIndexFileName = "bplustree-index"

var IndexBucketName = []byte("baradb-index")

// BPlusTree represents a B+ Tree indexer
type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(directory string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	db, err := bbolt.Open(filepath.Join(directory, BPlusTreeIndexFileName), 0644, opts)
	if err != nil {
		panic("Failed to open a B+ tree")
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(IndexBucketName)
		return err
	})
	if err != nil {
		panic("Failed to create a bucket in a B+ tree")
	}

	t := &BPlusTree{
		tree: db,
	}
	return t
}

// Put stores location of the corresponding data of the key in the indexer
func (t *BPlusTree) Put(key []byte, position *data.LogRecordPosition) bool {
	err := t.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(IndexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPosition(position))
	})
	if err != nil {
		panic("Failed to put a value to a B+ tree")
	}
	return true
}

// Get gets the location of the corresponding data af the key in the indexer
func (t *BPlusTree) Get(key []byte) *data.LogRecordPosition {
	var lrp *data.LogRecordPosition
	err := t.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(IndexBucketName)
		value := bucket.Get(key)
		if len(value) > 0 {
			lrp = data.DecodeLogRecordPosition(value)
		}
		return nil
	})
	if err != nil {
		panic("Failed to get a value from a B+ tree")
	}
	return lrp
}

// Delete deletes the location of the corresponding data of the key in the indexer
func (t *BPlusTree) Delete(key []byte) bool {
	var ok bool
	err := t.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(IndexBucketName)
		if value := bucket.Get(key); len(value) > 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	})
	if err != nil {
		panic("Failed to delete a value from a B+ tree")
	}
	return ok
}

// Size returns how many key/value pairs in the indexer
func (t *BPlusTree) Size() int {
	var size int
	err := t.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(IndexBucketName)
		size = bucket.Stats().KeyN
		return nil
	})
	if err != nil {
		panic("Failed to get size of a B+ tree")
	}
	return size
}

// Iterator returns an iterator
func (t *BPlusTree) Iterator(reverse bool) Iterator {
	return NewBPlusTreeIterator(t.tree, reverse)
}

func (t *BPlusTree) Close() error {
	return t.tree.Close()
}