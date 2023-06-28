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

	ds.Set(utils.NewKey(4), utils.NewRandomValue(4), 0)

	dt, err := ds.Type(utils.NewKey(3))
	assert.Zero(t, dt)
	assert.Equal(t, baradb.ErrKeyNotFound, err)
	dt, err = ds.Type(utils.NewKey(4))
	assert.Nil(t, err)
	assert.Equal(t, String, dt)
}
