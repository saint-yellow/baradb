package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb"
)

func TestDS_ZAdd(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var exist bool
	var err error

	key := []byte("languages")

	exist, err = ds.ZAdd(key, 100, []byte("go"))
	assert.True(t, exist)
	assert.Nil(t, err)

	exist, err = ds.ZAdd(key, 101, []byte("go"))
	assert.False(t, exist)
	assert.Nil(t, err)
}

func TestDS_ZScore(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var value float64
	var err error

	key := []byte("languages")

	// unknown key
	value, err = ds.ZScore(key, []byte("chinese"))
	assert.Equal(t, float64(-1), value)
	assert.Nil(t, err)

	ds.ZAdd(key, 75.4, []byte("japanese"))

	// unknown member
	value, err = ds.ZScore(key, []byte("chinese"))
	assert.Equal(t, float64(-1), value)
	assert.ErrorIs(t, err, baradb.ErrKeyNotFound)

	value, err = ds.ZScore(key, []byte("japanese"))
	assert.Equal(t, float64(75.4), value)
	assert.Nil(t, err)

	// change the score of the member
	ds.ZAdd(key, 114.514, []byte("japanese"))
	value, err = ds.ZScore(key, []byte("japanese"))
	assert.Equal(t, float64(114.514), value)
	assert.Nil(t, err)
}
