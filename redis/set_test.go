package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb"
)

func TestDS_SAdd(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var ok bool
	var err error
	var md *metadata

	ok, err = ds.SAdd([]byte("set-1"), []byte("member-1"))
	assert.True(t, ok)
	assert.Nil(t, err)
	md, err = ds.getMetadata([]byte("set-1"), Set)
	assert.Nil(t, err)
	assert.True(t, md.dataType == Set && md.size == 1)

	ok, err = ds.SAdd([]byte("set-1"), []byte("member-1"))
	assert.False(t, ok)
	assert.Nil(t, err)
	md, err = ds.getMetadata([]byte("set-1"), Set)
	assert.Nil(t, err)
	assert.True(t, md.dataType == Set && md.size == 1)

	ok, err = ds.SAdd([]byte("set-1"), []byte("member-2"))
	assert.True(t, ok)
	assert.Nil(t, err)
	md, err = ds.getMetadata([]byte("set-1"), Set)
	assert.Nil(t, err)
	assert.True(t, md.dataType == Set && md.size == 2)
}

func TestDS_SMembers(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	// TODO: to be implemented
}

func TestDS_SIsMember(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var ok bool
	var err error

	ok, err = ds.SIsMember([]byte("set-0"), []byte("member-0"))
	assert.False(t, ok)
	assert.Equal(t, nil, err)

	ds.SAdd([]byte("set-1"), []byte("member-1"))
	ok, err = ds.SIsMember([]byte("set-1"), []byte("member-1"))
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = ds.SIsMember([]byte("set-1"), []byte("member-2"))
	assert.False(t, ok)
	assert.Nil(t, err)

	ds.SAdd([]byte("set-"), []byte("member-2"))
	ok, err = ds.SIsMember([]byte("set-1"), []byte("member-2"))
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestDS_SRem(t *testing.T) {
	ds, _ := NewDS(baradb.DefaultDBOptions)
	defer destroyDS(ds, baradb.DefaultDBOptions.Directory)

	var ok bool
	var err error

	ok, err = ds.SRem([]byte("set-0"), []byte("member-0"))
	assert.False(t, ok)
	assert.Nil(t, err)

	ds.SAdd([]byte("set-1"), []byte("member-1"))

	ok, err = ds.SRem([]byte("set-1"), []byte("member-0"))
	assert.False(t, ok)
	assert.Nil(t, err)

	ok, err = ds.SRem([]byte("set-1"), []byte("member-1"))
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = ds.SIsMember([]byte("set-1"), []byte("member-1"))
	assert.False(t, ok)
	assert.Nil(t, err)
}
