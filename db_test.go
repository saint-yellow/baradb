package baradb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destroyDB(db *DB) {
	if db != nil {
		db.activeFile.Close()
		err := os.RemoveAll(db.options.Directory)
		if err != nil {
			panic(err)
		}
	}
}

func TestDB_Launch(t *testing.T) {
	db, err := LaunchDB(DefaultOptions)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	db, _ := LaunchDB(DefaultOptions)
	defer destroyDB(db)
	err := db.Put([]byte("114"), []byte("514"))
	assert.Nil(t, err)
}

func TestDB_Get(t *testing.T) {
	db, _ := LaunchDB(DefaultOptions)
	defer destroyDB(db)
	db.Put([]byte("114"), []byte("514"))
	b, err := db.Get([]byte("114"))
	assert.Nil(t, err)
	assert.Equal(t, "514", string(b))
}

func TestDB_Delete(t *testing.T) {
	db, _ := LaunchDB(DefaultOptions)
	defer destroyDB(db)
	db.Put([]byte("114"), []byte("514"))
	err := db.Delete([]byte("114"))
	assert.Nil(t, nil)
	b, err := db.Get([]byte("114"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, b)
}
