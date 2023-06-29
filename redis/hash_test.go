package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb"
	"github.com/saint-yellow/baradb/utils"
)

func TestDS_HSet(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var isNewKey bool
	var err error

	// add a new key
	isNewKey, err = ds.HSet(utils.NewKey(1), utils.NewKey(2), utils.NewKey(3))
	assert.True(t, isNewKey)
	assert.Nil(t, err)

	// add another new key
	isNewKey, err = ds.HSet(utils.NewKey(2), utils.NewKey(3), utils.NewKey(3))
	assert.True(t, isNewKey)
	assert.Nil(t, err)

	// change the value of the filed of an old key
	isNewKey, err = ds.HSet(utils.NewKey(1), utils.NewKey(2), utils.NewKey(4))
	assert.False(t, isNewKey)
	assert.Nil(t, err)
}

func TestDS_HGet(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var value []byte
	var err error

	// unknown key
	value, err = ds.HGet(utils.NewKey(0), utils.NewKey(0))
	assert.Nil(t, err)
	assert.Nil(t, value)

	// add a new key
	ds.HSet(utils.NewKey(1), utils.NewKey(2), utils.NewKey(3))
	value, err = ds.HGet(utils.NewKey(1), utils.NewKey(2))
	assert.Nil(t, err)
	assert.EqualValues(t, utils.NewKey(3), value)

	// change the value of the filed of an old key
	ds.HSet(utils.NewKey(1), utils.NewKey(2), utils.NewKey(4))
	value, err = ds.HGet(utils.NewKey(1), utils.NewKey(2))
	assert.Nil(t, err)
	assert.EqualValues(t, utils.NewKey(4), value)

	// unknown field
	value, err = ds.HGet(utils.NewKey(1), utils.NewKey(0))
	assert.Equal(t, baradb.ErrKeyNotFound, err)
	assert.Nil(t, value)
}

func TestDS_HDel(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var exist bool
	var err error
	var value []byte

	// unknown key
	exist, err = ds.HDel(utils.NewKey(0), utils.NewKey(0))
	assert.False(t, exist)
	assert.Nil(t, err)

	ds.HSet(utils.NewKey(1), utils.NewKey(1), utils.NewKey(1))
	value, err = ds.HGet(utils.NewKey(1), utils.NewKey(1))
	assert.EqualValues(t, utils.NewKey(1), value)
	assert.Nil(t, err)
	exist, err = ds.HDel(utils.NewKey(1), utils.NewKey(1))
	assert.True(t, exist)
	assert.Nil(t, err)
	value, err = ds.HGet(utils.NewKey(1), utils.NewKey(1))
	assert.Nil(t, value)
	assert.Nil(t, err)

	// unknown field
	exist, err = ds.HDel(utils.NewKey(1), utils.NewKey(2))
	assert.False(t, exist)
	assert.Nil(t, err)
}
