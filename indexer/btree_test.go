package indexer

import (
	"testing"

	"github.com/saint-yellow/baradb/data"
	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPosition{FileID: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPosition{FileID: 2, Offset: 10})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPosition{FileID: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.FileID)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPosition{FileID: 2, Offset: 10})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPosition{FileID: 2, Offset: 99})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(2), pos2.FileID)
	assert.Equal(t, int64(99), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPosition{FileID: 114, Offset: 514})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("homo"), &data.LogRecordPosition{FileID: 114, Offset: 1919})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("homo"))
	assert.True(t, res4)
}
