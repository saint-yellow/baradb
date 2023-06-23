package indexer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb/data"
	"github.com/saint-yellow/baradb/utils"
)

func TestARTree_New(t *testing.T) {
	tree := NewARTree()
	assert.NotNil(t, tree)
}

func TestARTree_Put(t *testing.T) {
	tree := NewARTree()

	var ok bool
	ok = tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 114})
	assert.True(t, ok)
}

func TestARTree_Get(t *testing.T) {
	tree := NewARTree()

	tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 114})
	lrp := tree.Get([]byte("114"))
	assert.True(t, lrp.FileID == uint32(114) && lrp.Offset == int64(114))

	tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 1140, Offset: 1140})
	lrp = tree.Get([]byte("114"))
	assert.True(t, lrp.FileID == uint32(1140) && lrp.Offset == int64(1140))

	lrp = tree.Get([]byte("514"))
	assert.Nil(t, lrp)
}

func TestARTree_Delete(t *testing.T) {
	tree := NewARTree()

	tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 114})
	lrp := tree.Get([]byte("114"))
	assert.NotNil(t, lrp)
	ok := tree.Delete([]byte("114"))
	assert.True(t, ok)
	ok = tree.Delete([]byte("114"))
	assert.False(t, ok)
}

func TestARTree_Size(t *testing.T) {
	tree := NewARTree()
	assert.Zero(t, tree.Size())

	tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 114})
	assert.Positive(t, tree.Size())
	assert.Equal(t, 1, tree.Size())

	tree.Delete([]byte("114"))
	assert.Zero(t, tree.Size())
}

func TestARTreeIterator_New(t *testing.T) {
	tree := NewARTree()

	// The indexer has no key
	iter1 := tree.Iterator(false)
	assert.False(t, iter1.Valid())

	// The indexer has one key
	tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 114})
	iter2 := tree.Iterator(false)
	assert.True(t, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.False(t, iter2.Valid())

	// The indexe has more keys
	for i := 1; i < 20; i++ {
		tree.Put(utils.NewKey(i), &data.LogRecordPosition{FileID: 114, Offset: 114})
	}
	iter3 := tree.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
		assert.NotNil(t, iter3.Value())
	}
	iter4 := tree.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
		assert.NotNil(t, iter4.Value())
	}
}

func TestARTreeIterator_Seek(t *testing.T) {
	tree := NewARTree()

	for i := 1; i <= 10; i++ {
		tree.Put(utils.NewKey(i), &data.LogRecordPosition{FileID: 114, Offset: 514})
	}

	iter1 := tree.Iterator(false)
	defer iter1.Close()
	var index int = 1
	for iter1.Seek([]byte("aaa")); iter1.Valid(); iter1.Next() {
		// From 1 to 10
		assert.True(t, strings.HasSuffix(string(iter1.Key()), fmt.Sprintf("%d", index)))
		if index < 10 {
			index++
		}
	}

	iter2 := tree.Iterator(true)
	defer iter2.Close()
	for iter2.Seek([]byte("zzz")); iter2.Valid(); iter2.Next() {
		// From 10 to 1
		assert.True(t, strings.HasSuffix(string(iter2.Key()), fmt.Sprintf("%d", index)))
		index--
	}
}
