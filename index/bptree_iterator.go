package index

import (
	"go.etcd.io/bbolt"

	"github.com/saint-yellow/baradb/data"
)

type bplusTreeIterator struct {
	tx           *bbolt.Tx
	cursor       *bbolt.Cursor
	reverse      bool
	currentKey   []byte
	currentValue []byte
}

func newBPlusTreeIterator(tree *bbolt.DB, reverse bool) *bplusTreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("Failed to begin a transaction")
	}
	cursor := tx.Bucket(IndexBucketName).Cursor()
	iter := &bplusTreeIterator{
		tx:      tx,
		cursor:  cursor,
		reverse: reverse,
	}
	iter.Rewind()
	return iter
}

func (iter *bplusTreeIterator) Rewind() {
	if iter.reverse {
		iter.currentKey, iter.currentValue = iter.cursor.Last()
	} else {
		iter.currentKey, iter.currentValue = iter.cursor.First()
	}
}

func (iter *bplusTreeIterator) Seek(key []byte) {
	iter.currentKey, iter.currentValue = iter.cursor.Seek(key)
}

func (iter *bplusTreeIterator) Next() {
	if iter.reverse {
		iter.currentKey, iter.currentValue = iter.cursor.Prev()
	} else {
		iter.currentKey, iter.currentValue = iter.cursor.Next()
	}
}

func (iter *bplusTreeIterator) Valid() bool {
	return len(iter.currentKey) != 0
}

func (iter *bplusTreeIterator) Key() []byte {
	return iter.currentKey
}

func (iter *bplusTreeIterator) Value() *data.LogRecordPosition {
	return data.DecodeLogRecordPosition(iter.currentValue)
}

func (iter *bplusTreeIterator) Close() {
	err := iter.tx.Rollback()
	if err != nil {
		panic("Failed to rollback a transaction")
	}
}
