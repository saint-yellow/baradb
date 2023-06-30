package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb"
	"github.com/saint-yellow/baradb/utils"
)

func TestDS_LPush(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var size uint32
	var err error

	key := []byte("list-1")

	size, err = ds.LPush(key, []byte("element-1"))
	assert.Equal(t, uint32(1), size)
	assert.Nil(t, err)

	size, err = ds.LPush(key, []byte("element-2"))
	assert.Equal(t, uint32(2), size)
	assert.Nil(t, err)
}

func TestDS_RPush(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var size uint32
	var err error

	key := []byte("list-1")

	size, err = ds.RPush(key, []byte("element-1"))
	assert.Equal(t, uint32(1), size)
	assert.Nil(t, err)

	size, err = ds.RPush(key, []byte("element-2"))
	assert.Equal(t, uint32(2), size)
	assert.Nil(t, err)
}

func TestDS_LPop(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var element []byte
	var err error

	key := []byte("list-1")

	element, err = ds.LPop(key)
	assert.Nil(t, element)
	assert.Nil(t, err)

	for i := 1; i <= 10; i++ {
		ds.LPush(key, utils.NewKey(i))
		ds.RPush(key, utils.NewKey(i))
	}

	var size uint32
	size, err = ds.LLen(key)
	assert.Equal(t, uint32(20), size)
	assert.Nil(t, err)

	for i := 10; i >= 1; i-- {
		element, err = ds.LPop(key)
		assert.EqualValues(t, utils.NewKey(i), element)
		assert.Nil(t, err)
	}

	size, err = ds.LLen(key)
	assert.Equal(t, uint32(10), size)
	assert.Nil(t, err)

	for i := 1; i <= 10; i++ {
		element, err = ds.LPop(key)
		assert.EqualValues(t, utils.NewKey(i), element)
		assert.Nil(t, err)
	}

	size, err = ds.LLen(key)
	assert.Equal(t, uint32(0), size)
	assert.Nil(t, err)
}

func TestDS_RPop(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var element []byte
	var err error

	key := []byte("list-1")

	element, err = ds.LPop(key)
	assert.Nil(t, element)
	assert.Nil(t, err)

	for i := 1; i <= 10; i++ {
		ds.LPush(key, utils.NewKey(i))
		ds.RPush(key, utils.NewKey(i))
	}

	var size uint32
	size, err = ds.LLen(key)
	assert.Equal(t, uint32(20), size)
	assert.Nil(t, err)

	for i := 10; i >= 1; i-- {
		element, err = ds.RPop(key)
		assert.EqualValues(t, utils.NewKey(i), element)
		assert.Nil(t, err)
	}

	size, err = ds.LLen(key)
	assert.Equal(t, uint32(10), size)
	assert.Nil(t, err)

	for i := 1; i <= 10; i++ {
		element, err = ds.RPop(key)
		assert.EqualValues(t, utils.NewKey(i), element)
		assert.Nil(t, err)
	}

	size, err = ds.LLen(key)
	assert.Equal(t, uint32(0), size)
	assert.Nil(t, err)
}

func TestDS_LLen(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var size uint32
	var err error

	key := []byte("list-1")

	size, err = ds.LLen(key)
	assert.Equal(t, uint32(0), size)
	assert.Nil(t, err)

	number := 10
	for i := 1; i <= number; i++ {
		ds.LPush(key, utils.NewKey(i))
		size, err = ds.LLen(key)
		assert.Equal(t, uint32(i), size)
		assert.Nil(t, err)
	}
	for i := number; i >= 1; i-- {
		ds.RPop(key)
		size, err = ds.LLen(key)
		assert.Equal(t, uint32(i-1), size)
		assert.Nil(t, err)
	}
}
