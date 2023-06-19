package indexer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/saint-yellow/baradb/data"
	"github.com/saint-yellow/baradb/utils"
	"github.com/stretchr/testify/assert"
)

func TestBTreeIterator_New(t *testing.T) {
	bt1 := NewBTree()

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
	bt1 := NewBTree()
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
