package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb"
)

func TestMetadata_Decode(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	ds.HSet([]byte("key-1"), []byte("field-1"), []byte("value-1"))
	md1, err := ds.getMetadata([]byte("key-1"), Hash)
	assert.Nil(t, err)
	assert.Equal(t, Hash, md1.dataType)
	assert.Zero(t, md1.expire)
	assert.Positive(t, md1.size)
	assert.Positive(t, md1.version)
	assert.Zero(t, md1.head)
	assert.Zero(t, md1.tail)

	buf := encodeMetadata(md1)
	md2 := decodeMetadata(buf)
	assert.EqualValues(t, md1, md2)
}

func TestMetadata_Encode(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	ds.HSet([]byte("key-1"), []byte("field-1"), []byte("value-1"))
	md, err := ds.getMetadata([]byte("key-1"), Hash)
	assert.Nil(t, err)
	assert.Equal(t, Hash, md.dataType)
	assert.Zero(t, md.expire)
	assert.True(t, md.size == 1)
	assert.Positive(t, md.version)
	assert.Zero(t, md.head)
	assert.Zero(t, md.tail)

	ds.HSet([]byte("key-1"), []byte("field-2"), []byte("value-2"))
	md, err = ds.getMetadata([]byte("key-1"), Hash)
	assert.Nil(t, err)
	assert.True(t, md.size == 2)

	ds.HDel([]byte("key-1"), []byte("field-2"))
	md, err = ds.getMetadata([]byte("key-1"), Hash)
	assert.Nil(t, err)
	assert.True(t, md.size == 1)

	buf := encodeMetadata(md)
	assert.Equal(t, Hash, buf[0])
}
