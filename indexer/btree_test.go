package indexer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb/data"
	"github.com/saint-yellow/baradb/utils"
)

func TestBTree_Put(t *testing.T) {
	bt := newBTree()

	var lrp *data.LogRecordPosition

	// Put a nil key
	lrp = bt.Put(nil, &data.LogRecordPosition{FileID: 1, Offset: 100})
	assert.Nil(t, lrp)

	// Put a non-nil key and a value
	lrp = bt.Put([]byte("a"), &data.LogRecordPosition{FileID: 2, Offset: 10})
	assert.Nil(t, lrp)

	// Update the value of a key
	lrp = bt.Put([]byte("a"), &data.LogRecordPosition{FileID: 3, Offset: 30})
	assert.True(t, lrp.FileID == 2)
	assert.True(t, lrp.Offset == 10)
}

func TestBTree_Get(t *testing.T) {
	bt := newBTree()

	res1 := bt.Put(nil, &data.LogRecordPosition{FileID: 1, Offset: 100})
	assert.Nil(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.FileID)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPosition{FileID: 2, Offset: 10})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPosition{FileID: 2, Offset: 99})
	assert.NotNil(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(2), pos2.FileID)
	assert.Equal(t, int64(99), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := newBTree()

	res1 := bt.Put(nil, &data.LogRecordPosition{FileID: 114, Offset: 514})
	assert.Nil(t, res1)

	res2, ok2 := bt.Delete(nil)
	assert.NotNil(t, res2)
	assert.True(t, ok2)

	res3 := bt.Put([]byte("homo"), &data.LogRecordPosition{FileID: 114, Offset: 1919})
	assert.Nil(t, res3)

	res4, ok4 := bt.Delete([]byte("homo"))
	assert.NotNil(t, res4)
	assert.True(t, ok4)
}

func TestBTreeIterator_New(t *testing.T) {
	bt1 := newBTree()

	// The indexer has no key
	bti1 := bt1.Iterator(false)
	assert.False(t, bti1.Valid())

	// The indexer has one key
	bt1.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 114})
	bti2 := bt1.Iterator(false)
	assert.True(t, bti2.Valid())
	assert.NotNil(t, bti2.Key())
	assert.NotNil(t, bti2.Value())
	bti2.Next()
	assert.False(t, bti2.Valid())

	// The indexe has more keys
	for i := 1; i < 20; i++ {
		bt1.Put(utils.NewKey(i), &data.LogRecordPosition{FileID: 114, Offset: 114})
	}
	bti3 := bt1.Iterator(false)
	for bti3.Rewind(); bti3.Valid(); bti3.Next() {
		assert.NotNil(t, bti3.Key())
		assert.NotNil(t, bti3.Value())
	}
	bti4 := bt1.Iterator(true)
	for bti4.Rewind(); bti4.Valid(); bti4.Next() {
		assert.NotNil(t, bti4.Key())
		assert.NotNil(t, bti4.Value())
	}
}

func TestBTreeIterator_Seek(t *testing.T) {
	bt1 := newBTree()
	for i := 1; i <= 10; i++ {
		bt1.Put(utils.NewKey(i), &data.LogRecordPosition{FileID: 114, Offset: 514})
	}

	bti1 := bt1.Iterator(false)
	var index int = 1
	for bti1.Seek([]byte("aaa")); bti1.Valid(); bti1.Next() {
		// From 1 to 10
		assert.True(t, strings.HasSuffix(string(bti1.Key()), fmt.Sprintf("%d", index)))
		if index < 10 {
			index++
		}
	}

	bti2 := bt1.Iterator(true)
	for bti2.Seek([]byte("zzz")); bti2.Valid(); bti2.Next() {
		// From 10 to 1
		assert.True(t, strings.HasSuffix(string(bti2.Key()), fmt.Sprintf("%d", index)))
		index--
	}
}
