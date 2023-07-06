package baradb

import (
	"bytes"

	"github.com/saint-yellow/baradb/index"
)

// Iterator an iterator of DB
type Iterator struct {
	indexIterator index.Iterator        // iterator of an index
	db            *DB                     // DB engine
	options       index.IteratorOptions // options of an iterator of an index
}

// NewItrerator initializes an iterator of DB engine
func (db *DB) NewItrerator(options index.IteratorOptions) *Iterator {
	iterator := &Iterator{
		indexIterator: db.index.Iterator(options.Reverse),
		db:            db,
		options:       options,
	}

	return iterator
}

func (it *Iterator) Rewind() {
	it.indexIterator.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIterator.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.indexIterator.Next()
	it.skipToNext()
}

func (it *Iterator) Valid() bool {
	return it.indexIterator.Valid()
}

func (it *Iterator) Key() []byte {
	return it.indexIterator.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	lrp := it.indexIterator.Value()

	it.db.mu.RLock()
	defer it.db.mu.RUnlock()

	return it.db.getValueByPosition(lrp)
}

func (it *Iterator) Close() {
	it.indexIterator.Close()
}

// skipToNext skips keys if Prefix of IteratorOptions is not nil
func (it *Iterator) skipToNext() {
	prefixLength := len(it.options.Prefix)
	if prefixLength == 0 {
		return
	}

	for ; it.indexIterator.Valid(); it.indexIterator.Next() {
		key := it.indexIterator.Key()
		if prefixLength <= len(key) && bytes.Equal(it.options.Prefix, key[:prefixLength]) {
			break
		}
	}
}
