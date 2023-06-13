package baradb

import (
	"fmt"
	"os"
	"testing"

	"github.com/saint-yellow/baradb/utils"
	"github.com/stretchr/testify/assert"
)

// destroyDB a teardown method for clearing resources after testing
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			db.activeFile.Close()
		}
		err := os.RemoveAll(db.options.Directory)
		if err != nil {
			panic(err)
		}
	}
}

func TestDB_Launch(t *testing.T) {
	db, err := LaunchDB(DefaultOptions)
	defer destroyDB(db)

	// Launch a DB engine with no any data file
	assert.Nil(t, err)
	assert.NotNil(t, db)
	assert.DirExists(t, db.options.Directory)
	assert.Nil(t, db.activeFile)
	assert.Equal(t, 0, len(db.inactiveFiles))

	// Put a key/value pair to generate an active data file
	db.Put(utils.RandomKey(0), utils.RandonValue(64))
	assert.NotNil(t, db.activeFile)

	// Put many key/value pairs to generate inactive data files
	for i := 1; i <= 10000; i++ {
		db.Put(utils.RandomKey(i), utils.RandonValue(256))
	}
	assert.True(t, len(db.inactiveFiles) > 0)
	t.Log(db.activeFile.FileID)
	for i := range db.inactiveFiles {
		file := db.inactiveFiles[i]
		size, _ := file.IOHandler.Size()
		assert.True(t,
			size <= db.options.MaxDataFileSize,
			fmt.Sprintf("The size of inactive data file (ID: %d) is greater than the cofigured maximum size", i))
	}
	size, _ := db.activeFile.IOHandler.Size()
	assert.True(t,
		size <= db.options.MaxDataFileSize,
		fmt.Sprintf("The size of active data file (ID: %d) is greater than the configured maximum size", db.activeFile.FileID))

	// Relaunch the DB engine to test its all data files
	db.activeFile.Close()
	db, err = LaunchDB(DefaultOptions)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	assert.DirExists(t, db.options.Directory)
	assert.NotNil(t, db.activeFile)
	assert.True(t, len(db.inactiveFiles) > 0)
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

	// Use a nil key
	b, err := db.Get(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)
	assert.Nil(t, b)

	// Use an empty key
	b, err = db.Get([]byte(""))
	assert.Equal(t, ErrKeyIsEmpty, err)
	assert.Nil(t, b)

	// Use an unknown key
	b, err = db.Get([]byte("unknown-key"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, b)

	// Append a key/value pair
	db.Put([]byte("1919"), []byte("810"))

	// Append another key/value pair
	db.Put([]byte("114"), []byte("514"))
	b, err = db.Get([]byte("114"))
	assert.Nil(t, err)
	assert.Equal(t, "514", string(b))

	// Modify the corresponding value of an exist key
	db.Put([]byte("114"), []byte("114514"))
	b, err = db.Get([]byte("114"))
	assert.Nil(t, err)
	assert.Equal(t, "114514", string(b))

	// Delete an exist key/value pair
	db.Delete([]byte("114"))
	b, err = db.Get([]byte("114"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, b)

	// Put many key/value pairs to generate inactive data inactiveFiles
	for i := 1; i <= 10000; i++ {
		db.Put(utils.RandomKey(i), utils.RandonValue(256))
	}

	// Get a key/value pair in an inactive data file
	b, err = db.Get([]byte("1919"))
	assert.Nil(t, nil)
	assert.Equal(t, "810", string(b))

	// Relaunch the DB engine and a stored value
	db.activeFile.Close()
	db, _ = LaunchDB(DefaultOptions)
	b, err = db.Get([]byte("1919"))
	assert.Nil(t, err)
	assert.Equal(t, "810", string(b))
}

func TestDB_Delete(t *testing.T) {
	db, _ := LaunchDB(DefaultOptions)
	defer destroyDB(db)

	var err error

	// Delete an empty key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)
	err = db.Delete([]byte(""))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// Delete an non-exist key
	err = db.Delete([]byte("unknown-key"))
	assert.Nil(t, err)

	// Delete a key normally
	db.Put([]byte("114"), []byte("514"))
	err = db.Delete([]byte("114"))
	assert.Nil(t, nil)
	b, err := db.Get([]byte("114"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, b)

	// Put the same key after it is deleted
	db.Put([]byte("114"), []byte("114514"))

	// Relaunch the DB engine and check the exist key
	db.activeFile.Close()
	db, _ = LaunchDB(DefaultOptions)
	b, err = db.Get([]byte("114"))
	assert.Nil(t, err)
	assert.Equal(t, "114514", string(b))
	err = db.Delete([]byte("114"))
	assert.Nil(t, err)
	b, err = db.Get([]byte("114"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, b)
}
