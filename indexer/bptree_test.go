package indexer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb/data"
	"github.com/saint-yellow/baradb/utils"
)

var directory string = filepath.Join(os.TempDir(), "baradb-bptree")

func makeDirectory() {
	_ = os.MkdirAll(directory, os.ModePerm)
}

func removeDirectory() {
	_ = os.RemoveAll(directory)
}

func TestBPlusTree_New(t *testing.T) {
	makeDirectory()
	defer removeDirectory()

	tree := NewBPlusTree(directory, false)
	assert.NotNil(t, tree)
}

func TestBPlusTree_Put(t *testing.T) {
	makeDirectory()
	defer removeDirectory()

	tree := NewBPlusTree(directory, false)

	var ok bool
	ok = tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 0})
	assert.True(t, ok)
	ok = tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 1})
	assert.True(t, ok)
}

func TestBPlusTree_Get(t *testing.T) {
	makeDirectory()
	defer removeDirectory()

	tree := NewBPlusTree(directory, false)

	var lrp *data.LogRecordPosition
	lrp = tree.Get([]byte("114"))
	assert.Nil(t, lrp)
	tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 2})
	lrp = tree.Get([]byte("114"))
	assert.True(t, lrp.FileID == uint32(114) && lrp.Offset == int64(2))
	tree.Put([]byte("144"), &data.LogRecordPosition{FileID: 114, Offset: 3})
	lrp = tree.Get([]byte("114"))
	assert.True(t, lrp.FileID == uint32(114) && lrp.Offset == int64(2))
}

func TestBPlusTree_Delete(t *testing.T) {
	makeDirectory()
	defer removeDirectory()

	tree := NewBPlusTree(directory, false)

	var ok bool
	ok = tree.Delete([]byte("114"))
	assert.False(t, ok)
	tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 4})
	ok = tree.Delete([]byte("114"))
	assert.True(t, ok)
	ok = tree.Delete([]byte("114"))
	assert.False(t, ok)
}

func TestBPlusTree_Size(t *testing.T) {
	makeDirectory()
	defer removeDirectory()

	tree := NewBPlusTree(directory, false)
	assert.Zero(t, tree.Size())

	tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 5})
	assert.True(t, tree.Size() == 1)

	tree.Delete([]byte("114"))
	assert.Zero(t, tree.Size())
}

func TestBPlusTreeIterator_New1(t *testing.T) {
	makeDirectory()
	defer removeDirectory()

	// The indexer has no key
	tree := NewBPlusTree(directory, false)

	iter1 := tree.Iterator(false)
	defer iter1.Close()
	assert.False(t, iter1.Valid())
}

func TestBPlusTreeIterator_New2(t *testing.T) {
	makeDirectory()
	defer removeDirectory()

	// The indexer has one key
	tree := NewBPlusTree(directory, false)
	tree.Put([]byte("114"), &data.LogRecordPosition{FileID: 114, Offset: 114})

	iter2 := tree.Iterator(false)
	defer iter2.Close()

	assert.True(t, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.False(t, iter2.Valid())
}

func TestBPlusTreeIterator_New3(t *testing.T) {
	makeDirectory()
	defer removeDirectory()

	tree := NewBPlusTree(directory, false)

	// The indexer has more keys
	var count int = 20
	for i := 1; i <= count; i++ {
		tree.Put(utils.NewKey(i), &data.LogRecordPosition{FileID: 114, Offset: int64(i)})
	}

	var index int = 1

	iter3 := tree.Iterator(false)
	defer iter3.Close()
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.True(t, strings.HasSuffix(string(iter3.Key()), fmt.Sprintf("%d", index)))
		assert.Equal(t, int64(index), iter3.Value().Offset)
		if index < count {
			index++
		}
	}
	iter4 := tree.Iterator(true)
	defer iter4.Close()
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.True(t, strings.HasSuffix(string(iter4.Key()), fmt.Sprintf("%d", index)))
		assert.Equal(t, int64(index), iter4.Value().Offset)
		index--
	}
}

func TestBPlusTreeIterator_Seek(t *testing.T) {
	makeDirectory()
	defer removeDirectory()

	tree := NewBPlusTree(directory, false)

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
