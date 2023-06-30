package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb"
	"github.com/saint-yellow/baradb/utils"
)

func TestDS_Del(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	ds.Set(utils.NewKey(2), utils.NewRandomValue(3), 0)
	value, err := ds.Get(utils.NewKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, value)
	err = ds.Del(utils.NewKey(3))
	assert.Nil(t, err)
	err = ds.Del(utils.NewKey(2))
	assert.Nil(t, err)
	value, err = ds.Get(utils.NewKey(2))
	assert.Equal(t, baradb.ErrKeyNotFound, err)
	assert.Nil(t, value)
}

func TestDS_Type(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var dt dataType
	var err error

	dt, err = ds.Type([]byte("unknown"))
	assert.Equal(t, byte(0), dt)
	assert.ErrorIs(t, err, baradb.ErrKeyNotFound)

	ds.Set([]byte("string-1"), []byte("value-1"), 0)
	dt, err = ds.Type([]byte("string-1"))
	assert.Nil(t, err)
	assert.Equal(t, String, dt)

	ds.HSet([]byte("hash-1"), []byte("field-1"), []byte("value-1"))
	dt, err = ds.Type([]byte("hash-1"))
	assert.Equal(t, Hash, dt)
	assert.Nil(t, err)

	ds.LPush([]byte("list-1"), []byte("element-1"))
	dt, err = ds.Type([]byte("list-1"))
	assert.Equal(t, List, dt)
	assert.Nil(t, err)

	ds.SAdd([]byte("set-1"), []byte("member-1"))
	dt, err = ds.Type([]byte("set-1"))
	assert.Equal(t, Set, dt)
	assert.Nil(t, err)

	ds.ZAdd([]byte("zset-1"), 0, []byte("member-1"))
	dt, err = ds.Type([]byte("zset-1"))
	assert.Equal(t, ZSet, dt)
	assert.Nil(t, err)
}
