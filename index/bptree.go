package index

import (
	"path/filepath"

	"go.etcd.io/bbolt"

	"github.com/saint-yellow/baradb/data"
)

const BPlusTreeIndexFileName = "bplustree-index"

var IndexBucketName = []byte("baradb-index")

// bplusTree represents a B+ Tree index
type bplusTree struct {
	tree *bbolt.DB
}

func newBPlusTree(directory string, syncWrites bool) *bplusTree {
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

	t := &bplusTree{
		tree: db,
	}
	return t
}

// Put stores location of the corresponding data of the key in the index
func (t *bplusTree) Put(key []byte, position *data.LogRecordPosition) *data.LogRecordPosition {
	var oldValue []byte
	err := t.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(IndexBucketName)
		oldValue = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPosition(position))
	})
	if err != nil {
		panic("Failed to put a value to a B+ tree")
	}

	if len(oldValue) > 0 {
		return data.DecodeLogRecordPosition(oldValue)
	}
	return nil
}

// Get gets the location of the corresponding data af the key in the index
func (t *bplusTree) Get(key []byte) *data.LogRecordPosition {
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

// Delete deletes the location of the corresponding data of the key in the index
func (t *bplusTree) Delete(key []byte) (*data.LogRecordPosition, bool) {
	var ok bool
	var oldValue []byte
	err := t.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(IndexBucketName)
		oldValue = bucket.Get(key)
		if len(oldValue) > 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	})
	if err != nil {
		panic("Failed to delete a value from a B+ tree")
	}

	var lrp *data.LogRecordPosition
	if ok {
		lrp = data.DecodeLogRecordPosition(oldValue)
	}
	return lrp, ok
}

// Size returns how many key/value pairs in the index
func (t *bplusTree) Size() int {
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
func (t *bplusTree) Iterator(reverse bool) Iterator {
	return newBPlusTreeIterator(t.tree, reverse)
}

func (t *bplusTree) Close() error {
	return t.tree.Close()
}
