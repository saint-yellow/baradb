package baradb

import (
	"fmt"
	"os"
	"testing"

	"github.com/saint-yellow/baradb/indexer"
	"github.com/saint-yellow/baradb/utils"
	"github.com/stretchr/testify/assert"
)

// destroyDB a teardown method for clearing resources after testing
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			db.Close()
		}
		err := os.RemoveAll(db.options.Directory)
		if err != nil {
			panic(err)
		}
	}
}

func TestDB_Launch(t *testing.T) {
	db, err := LaunchDB(DefaultDBOptions)
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
	db.Close()
	db, err = LaunchDB(DefaultDBOptions)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	assert.DirExists(t, db.options.Directory)
	assert.NotNil(t, db.activeFile)
	assert.True(t, len(db.inactiveFiles) > 0)
}

func TestDB_Put(t *testing.T) {
	db, _ := LaunchDB(DefaultDBOptions)
	defer destroyDB(db)
	err := db.Put([]byte("114"), []byte("514"))
	assert.Nil(t, err)
}

func TestDB_Get(t *testing.T) {
	db, _ := LaunchDB(DefaultDBOptions)
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
	db.Close()
	db, _ = LaunchDB(DefaultDBOptions)
	b, err = db.Get([]byte("1919"))
	assert.Nil(t, err)
	assert.Equal(t, "810", string(b))
}

func TestDB_Delete(t *testing.T) {
	db, _ := LaunchDB(DefaultDBOptions)
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
	db.Close()
	db, _ = LaunchDB(DefaultDBOptions)
	b, err = db.Get([]byte("114"))
	assert.Nil(t, err)
	assert.Equal(t, "114514", string(b))
	err = db.Delete([]byte("114"))
	assert.Nil(t, err)
	b, err = db.Get([]byte("114"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, b)
}

func TestDB_NewIterator(t *testing.T) {
	db, _ := LaunchDB(DefaultDBOptions)
	defer destroyDB(db)

	// The DB engine has no key
	iter1 := db.NewItrerator(indexer.DefaultIteratorOptions)
	assert.NotNil(t, iter1)
	assert.False(t, iter1.Valid())

	// The DB engine has one key
	db.Put([]byte("114"), []byte("514"))
	iter2 := db.NewItrerator(indexer.DefaultIteratorOptions)
	assert.NotNil(t, iter2)
	assert.True(t, iter2.Valid())
	assert.Equal(t, "114", string(iter2.Key()))
	b, err := iter2.Value()
	assert.Nil(t, err)
	assert.Equal(t, "514", string(b))
	db.Delete([]byte("114"))

	// The DB engine has more keys
	keysCount := 20
	for i := 1; i <= keysCount; i++ {
		db.Put([]byte(fmt.Sprintf("%02d", i)), utils.RandonValue(10))
	}

	// Forward iteration
	iter3 := db.NewItrerator(indexer.DefaultIteratorOptions)
	assert.NotNil(t, iter3)
	assert.True(t, iter3.Valid())
	var index int = 1
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.Equal(t, fmt.Sprintf("%02d", index), string(iter3.Key()))
		b, err = iter3.Value()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		if index < keysCount {
			index++
		}
	}

	// Forward seek
	iter3.Rewind()
	index = 10
	for iter3.Seek([]byte(fmt.Sprintf("%02d", index))); iter3.Valid(); iter3.Next() {
		assert.Equal(t, fmt.Sprintf("%02d", index), string(iter3.Key()))
		b, err = iter3.Value()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		index++
	}

	// Reversed iteration
	opts := indexer.DefaultIteratorOptions
	opts.Reverse = true
	iter4 := db.NewItrerator(opts)
	index = keysCount
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.Equal(t, fmt.Sprintf("%02d", index), string(iter4.Key()))
		b, err = iter4.Value()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		index--
	}

	// Reversed seek
	iter4.Rewind()
	index = 5
	for iter4.Seek([]byte("05")); iter4.Valid(); iter4.Next() {
		assert.Equal(t, fmt.Sprintf("%02d", index), string(iter4.Key()))
		b, err = iter4.Value()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		index--
	}

	// Specify a prefix
	opts.Prefix = []byte("1")
	index = 19
	iter5 := db.NewItrerator(opts)
	for iter5.Rewind(); iter5.Valid(); iter5.Next() {
		assert.Equal(t, fmt.Sprintf("%02d", index), string(iter5.Key()))
		b, err = iter5.Value()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		index--
	}
}

func TestDB_ListKeys(t *testing.T) {
	db, _ := LaunchDB(DefaultDBOptions)
	defer destroyDB(db)

	var keys [][]byte

	// The DB engine has no key
	keys = db.ListKeys()
	assert.Equal(t, 0, len(keys))

	// The DB engine has one key
	db.Put([]byte("114"), []byte("514"))
	keys = db.ListKeys()
	assert.Equal(t, 1, len(keys))

	// The DB engine deletes the only Key
	db.Delete([]byte("114"))
	keys = db.ListKeys()
	assert.Equal(t, 0, len(keys))

	// The DB engine has more than one key
	keysCount := 20
	for i := 11; i <= 30; i++ {
		db.Put([]byte(fmt.Sprintf("%02d", i)), utils.RandonValue(8))
	}
	keys = db.ListKeys()
	assert.Equal(t, keysCount, len(keys))
	var index int = 11
	for _, key := range keys {
		assert.Equal(t, fmt.Sprintf("%02d", index), string(key))
		index++
	}
}

func TestDB_Fold(t *testing.T) {
	db, _ := LaunchDB(DefaultDBOptions)
	defer destroyDB(db)

	var err error

	// Put some data
	for i := 11; i <= 30; i++ {
		db.Put([]byte(fmt.Sprintf("%02d", i)), utils.RandonValue(8))
	}

	// Execure an uder-defined function
	udf := func(key, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	}
	err = db.Fold(udf)
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	db, _ := LaunchDB(DefaultDBOptions)
	defer destroyDB(db)

	var err error

	// The DB engine has no data file
	assert.Nil(t, db.activeFile)
	assert.Zero(t, len(db.inactiveFiles))
	err = db.Close()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db, _ = LaunchDB(DefaultDBOptions)
	assert.Nil(t, db.activeFile)
	assert.Zero(t, len(db.inactiveFiles))

	// The DB engine only has the active data file
	db.Put(utils.RandomKey(1), utils.RandonValue(32))
	assert.Zero(t, db.activeFile.FileID)
	assert.Zero(t, len(db.inactiveFiles))
	err = db.Close()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db, _ = LaunchDB(DefaultDBOptions)
	assert.Zero(t, db.activeFile.FileID)
	assert.Zero(t, len(db.inactiveFiles))

	// The DB engine has an active data file and at least one inactive data file
	for i := 100; i <= 100000; i++ {
		db.Put(utils.RandomKey(i), utils.RandonValue(32))
	}
	assert.Positive(t, db.activeFile.FileID)
	assert.Positive(t, len(db.inactiveFiles))
	err = db.Close()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db, _ = LaunchDB(DefaultDBOptions)
	assert.Positive(t, db.activeFile.FileID)
	assert.Positive(t, len(db.inactiveFiles))
}

func TestDB_Sync(t *testing.T) {
	db, _ := LaunchDB(DefaultDBOptions)
	defer destroyDB(db)

	var err error

	// The DB engine has no data file
	assert.Nil(t, db.activeFile)
	assert.Zero(t, len(db.inactiveFiles))
	err = db.Sync()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db.Close()
	db, _ = LaunchDB(DefaultDBOptions)
	assert.Nil(t, db.activeFile)
	assert.Zero(t, len(db.inactiveFiles))

	// The DB engine only has the active data file
	db.Put(utils.RandomKey(1), utils.RandonValue(32))
	assert.Zero(t, db.activeFile.FileID)
	assert.Zero(t, len(db.inactiveFiles))
	err = db.Sync()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db.Close()
	db, _ = LaunchDB(DefaultDBOptions)
	assert.Zero(t, db.activeFile.FileID)
	assert.Zero(t, len(db.inactiveFiles))

	// The DB engine has an active data file and at least one inactive data file
	for i := 100; i <= 100000; i++ {
		db.Put(utils.RandomKey(i), utils.RandonValue(32))
	}
	assert.Positive(t, db.activeFile.FileID)
	assert.Positive(t, len(db.inactiveFiles))
	err = db.Sync()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db.Close()
	db, _ = LaunchDB(DefaultDBOptions)
	assert.Positive(t, db.activeFile.FileID)
	assert.Positive(t, len(db.inactiveFiles))
}
