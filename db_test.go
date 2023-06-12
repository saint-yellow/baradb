package baradb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_Launch(t *testing.T) {
	db, err := LaunchDB(DefaultOptions)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	db, _ := LaunchDB(DefaultOptions)
	err := db.Put([]byte("114"), []byte("514"))
	assert.Nil(t, err)
}

func TestDB_Get(t *testing.T) {
	db, _ := LaunchDB(DefaultOptions)
	db.Put([]byte("114"), []byte("514"))
	b, err := db.Get([]byte("114"))
	assert.Nil(t, err)
	assert.Equal(t, "514", string(b))
}

func TestDB_Delete(t *testing.T) {
	db, _ := LaunchDB(DefaultOptions)
	db.Put([]byte("114"), []byte("514"))
	err := db.Delete([]byte("114"))
	assert.Nil(t, nil)
	b, err := db.Get([]byte("114"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, b)
}
