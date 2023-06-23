package indexer

import (
	"go.etcd.io/bbolt"

	"github.com/saint-yellow/baradb/data"
)

type BPlusTreeIterator struct {
	tx           *bbolt.Tx
	cursor       *bbolt.Cursor
	reverse      bool
	currentKey   []byte
	currentValue []byte
}

func NewBPlusTreeIterator(tree *bbolt.DB, reverse bool) *BPlusTreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("Failed to begin a transaction")
	}
	cursor := tx.Bucket(IndexBucketName).Cursor()
	iter := &BPlusTreeIterator{
		tx:      tx,
		cursor:  cursor,
		reverse: reverse,
	}
	iter.Rewind()
	return iter
}

func (iter *BPlusTreeIterator) Rewind() {
	if iter.reverse {
		iter.currentKey, iter.currentValue = iter.cursor.Last()
	} else {
		iter.currentKey, iter.currentValue = iter.cursor.First()
	}
}

func (iter *BPlusTreeIterator) Seek(key []byte) {
	iter.currentKey, iter.currentValue = iter.cursor.Seek(key)
}

func (iter *BPlusTreeIterator) Next() {
	if iter.reverse {
		iter.currentKey, iter.currentValue = iter.cursor.Prev()
	} else {
		iter.currentKey, iter.currentValue = iter.cursor.Next()
	}
}

func (iter *BPlusTreeIterator) Valid() bool {
	return len(iter.currentKey) != 0
}

func (iter *BPlusTreeIterator) Key() []byte {
	return iter.currentKey
}

func (iter *BPlusTreeIterator) Value() *data.LogRecordPosition {
	return data.DecodeLogRecordPosition(iter.currentValue)
}

func (iter *BPlusTreeIterator) Close() {
	err := iter.tx.Rollback()
	if err != nil {
		panic("Failed to rollback a transaction")
	}
}
