package baradb

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb/index"
	"github.com/saint-yellow/baradb/utils"
)

var testingDBOptions = DefaultDBOptions

// preparations for tests
func init() {
	testingDBOptions.MaxDataFileSize = 16 * 1024 * 1024
	os.RemoveAll(testingDBOptions.Directory)
}

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
	db, err := Launch(testingDBOptions)
	defer destroyDB(db)

	// Launch a DB engine with no any data file
	assert.Nil(t, err)
	assert.NotNil(t, db)
	assert.DirExists(t, db.options.Directory)
	assert.Nil(t, db.activeFile)
	assert.Equal(t, 0, len(db.inactiveFiles))

	// Put a key/value pair to generate an active data file
	db.Put(utils.NewKey(0), utils.NewRandomValue(64))
	assert.NotNil(t, db.activeFile)

	// Put many key/value pairs to generate inactive data files
	for i := 1; i <= 10000; i++ {
		db.Put(utils.NewKey(i), utils.NewRandomValue(4096))
	}
	assert.True(t, len(db.inactiveFiles) > 0)
	for i := range db.inactiveFiles {
		file := db.inactiveFiles[i]
		size, _ := file.Size()
		assert.True(
			t,
			size <= db.options.MaxDataFileSize,
			fmt.Sprintf(
				"The size of inactive data file (ID: %d) is greater than the cofigured maximum size",
				i,
			),
		)
	}
	size, _ := db.activeFile.Size()
	assert.True(
		t,
		size <= db.options.MaxDataFileSize,
		fmt.Sprintf(
			"The size of active data file (ID: %d) is greater than the configured maximum size",
			db.activeFile.FileID,
		),
	)

	// Relaunch the DB engine to test its all data files
	db.Close()
	db, err = Launch(testingDBOptions)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	assert.DirExists(t, db.options.Directory)
	assert.NotNil(t, db.activeFile)
	assert.True(t, len(db.inactiveFiles) > 0)
}

func TestDB_Put(t *testing.T) {
	db, _ := Launch(testingDBOptions)
	defer destroyDB(db)

	err := db.Put([]byte("114"), []byte("514"))
	assert.Nil(t, err)
	val, _ := db.Get([]byte("114"))
	assert.Equal(t, "514", string(val))

	db.Close()
	db, _ = Launch(testingDBOptions)
	val, _ = db.Get([]byte("114"))
	assert.Equal(t, "514", string(val))
}

func TestDB_Get(t *testing.T) {
	db, _ := Launch(testingDBOptions)
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
		db.Put(utils.NewKey(i), utils.NewRandomValue(4096))
	}

	// Get a key/value pair in an inactive data file
	b, err = db.Get([]byte("1919"))
	assert.Nil(t, err)
	assert.Equal(t, "810", string(b))

	// Relaunch the DB engine and a stored value
	db.Close()
	db, _ = Launch(testingDBOptions)
	b, err = db.Get([]byte("1919"))
	assert.Nil(t, err)
	assert.Equal(t, "810", string(b))
}

func TestDB_Delete(t *testing.T) {
	db, _ := Launch(testingDBOptions)
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
	assert.Nil(t, err)
	b, err := db.Get([]byte("114"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, b)

	// Put the same key after it is deleted
	db.Put([]byte("114"), []byte("114514"))

	// Relaunch the DB engine and check the exist key
	db.Close()
	db, _ = Launch(testingDBOptions)
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
	db, _ := Launch(testingDBOptions)
	defer destroyDB(db)

	// The DB engine has no key
	iter1 := db.NewItrerator(index.DefaultIteratorOptions)
	defer iter1.Close()
	assert.NotNil(t, iter1)
	assert.False(t, iter1.Valid())

	// The DB engine has one key
	db.Put([]byte("114"), []byte("514"))
	iter2 := db.NewItrerator(index.DefaultIteratorOptions)
	defer iter2.Close()
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
		db.Put([]byte(fmt.Sprintf("%02d", i)), utils.NewRandomValue(10))
	}

	// Forward iteration
	iter3 := db.NewItrerator(index.DefaultIteratorOptions)
	defer iter3.Close()
	assert.NotNil(t, iter3)
	assert.True(t, iter3.Valid())
	counter := 1
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.Equal(t, fmt.Sprintf("%02d", counter), string(iter3.Key()))
		b, err = iter3.Value()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		if counter < keysCount {
			counter++
		}
	}

	// Forward seek
	iter3.Rewind()
	counter = 10
	for iter3.Seek([]byte(fmt.Sprintf("%02d", counter))); iter3.Valid(); iter3.Next() {
		assert.Equal(t, fmt.Sprintf("%02d", counter), string(iter3.Key()))
		b, err = iter3.Value()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		counter++
	}

	// Reversed iteration
	opts := index.DefaultIteratorOptions
	opts.Reverse = true
	iter4 := db.NewItrerator(opts)
	defer iter4.Close()
	counter = keysCount
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.Equal(t, fmt.Sprintf("%02d", counter), string(iter4.Key()))
		b, err = iter4.Value()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		counter--
	}

	// Reversed seek
	iter4.Rewind()
	counter = 5
	for iter4.Seek([]byte("05")); iter4.Valid(); iter4.Next() {
		assert.Equal(t, fmt.Sprintf("%02d", counter), string(iter4.Key()))
		b, err = iter4.Value()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		counter--
	}

	// Specify a prefix
	opts.Prefix = []byte("1")
	counter = 19
	iter5 := db.NewItrerator(opts)
	defer iter5.Close()
	for iter5.Rewind(); iter5.Valid(); iter5.Next() {
		assert.Equal(t, fmt.Sprintf("%02d", counter), string(iter5.Key()))
		b, err = iter5.Value()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		counter--
	}
}

func TestDB_ListKeys(t *testing.T) {
	db, _ := Launch(testingDBOptions)
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
		db.Put([]byte(fmt.Sprintf("%02d", i)), utils.NewRandomValue(8))
	}
	keys = db.ListKeys()
	assert.Equal(t, keysCount, len(keys))
	index := 11
	for _, key := range keys {
		assert.Equal(t, fmt.Sprintf("%02d", index), string(key))
		index++
	}
}

func TestDB_Fold(t *testing.T) {
	db, _ := Launch(testingDBOptions)
	defer destroyDB(db)

	var err error

	// Put some data
	for i := 11; i <= 30; i++ {
		db.Put([]byte(fmt.Sprintf("%02d", i)), utils.NewRandomValue(8))
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
	db, _ := Launch(testingDBOptions)
	defer destroyDB(db)

	var err error

	// The DB engine has no data file
	assert.Nil(t, db.activeFile)
	assert.Zero(t, len(db.inactiveFiles))
	err = db.Close()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db, _ = Launch(testingDBOptions)
	assert.Nil(t, db.activeFile)
	assert.Zero(t, len(db.inactiveFiles))

	// The DB engine only has the active data file
	db.Put(utils.NewKey(1), utils.NewRandomValue(32))
	assert.Zero(t, db.activeFile.FileID)
	assert.Zero(t, len(db.inactiveFiles))
	err = db.Close()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db, _ = Launch(testingDBOptions)
	assert.Zero(t, db.activeFile.FileID)
	assert.Zero(t, len(db.inactiveFiles))

	// The DB engine has an active data file and at least one inactive data file
	for i := 1; i <= 10000; i++ {
		db.Put(utils.NewKey(i), utils.NewRandomValue(4096))
	}
	assert.Positive(t, db.activeFile.FileID)
	assert.Positive(t, len(db.inactiveFiles))
	err = db.Close()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db, _ = Launch(testingDBOptions)
	assert.Positive(t, db.activeFile.FileID)
	assert.Positive(t, len(db.inactiveFiles))
}

func TestDB_Sync(t *testing.T) {
	db, _ := Launch(testingDBOptions)
	defer destroyDB(db)

	var err error

	// The DB engine has no data file
	assert.Nil(t, db.activeFile)
	assert.Zero(t, len(db.inactiveFiles))
	err = db.Sync()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db.Close()
	db, _ = Launch(testingDBOptions)
	assert.Nil(t, db.activeFile)
	assert.Zero(t, len(db.inactiveFiles))

	// The DB engine only has the active data file
	db.Put(utils.NewKey(1), utils.NewRandomValue(32))
	assert.Zero(t, db.activeFile.FileID)
	assert.Zero(t, len(db.inactiveFiles))
	err = db.Sync()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db.Close()
	db, _ = Launch(testingDBOptions)
	assert.Zero(t, db.activeFile.FileID)
	assert.Zero(t, len(db.inactiveFiles))

	// The DB engine has an active data file and at least one inactive data file
	for i := 1; i <= 10000; i++ {
		db.Put(utils.NewKey(i), utils.NewRandomValue(4096))
	}
	assert.Positive(t, db.activeFile.FileID)
	assert.Positive(t, len(db.inactiveFiles))
	err = db.Sync()
	assert.Nil(t, err)

	// Relaunch the DB engine
	db.Close()
	db, _ = Launch(testingDBOptions)
	assert.Positive(t, db.activeFile.FileID)
	assert.Positive(t, len(db.inactiveFiles))
}

func TestDB_NewWriteBatch(t *testing.T) {
	db, _ := Launch(testingDBOptions)
	defer destroyDB(db)

	var (
		err error
		wb  *WriteBatch
	)

	// Write data, no commit
	wb = db.NewWriteBatch(DefaultWriteBatchOptions)
	assert.NotNil(t, wb)
	err = wb.Put([]byte("114"), []byte("514"))
	assert.Nil(t, err)
	err = wb.Delete([]byte("1919"))
	assert.Nil(t, err)

	// No data found in DB because of no commit
	val, err := db.Get([]byte("114"))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	// Write and commit normally
	err = wb.Commit()
	assert.Nil(t, err)

	// The DB has data
	val, err = db.Get([]byte("114"))
	assert.Nil(t, err)
	assert.Equal(t, "514", string(val))

	assert.Equal(t, 1, int(db.tranNo))

	// Relaunch DB to get the same data
	db.Close()
	db, _ = Launch(testingDBOptions)
	val, err = db.Get(utils.NewKey(2))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	val, err = db.Get([]byte("114"))
	assert.Nil(t, err)
	assert.Equal(t, "514", string(val))
}

func TestDB_FileLock(t *testing.T) {
	db1, _ := Launch(testingDBOptions)
	defer destroyDB(db1)

	assert.NotNil(t, db1.fileLock)

	var err error

	db2, err := Launch(testingDBOptions)
	assert.Nil(t, db2)
	assert.Equal(t, ErrDatabaseIsUsed, err)

	err = db1.Close()
	assert.Nil(t, err)

	db2, err = Launch(testingDBOptions)
	assert.Nil(t, err)
	assert.NotNil(t, db2.fileLock)
	err = db2.Close()
	assert.Nil(t, err)
}

func TestDB_MMap(t *testing.T) {
	db, _ := Launch(testingDBOptions)
	defer destroyDB(db)

	for i := 1; i <= 10000; i++ {
		db.Put(utils.NewKey(i), utils.NewRandomValue(4096))
	}
	db.Close()

	logTemplate := "use memory mapping at startup: %v, time elapsed: %v\n"

	now := time.Now()
	db, _ = Launch(testingDBOptions)
	t.Logf(fmt.Sprintf(logTemplate, DefaultDBOptions.MMapAtStartup, time.Since(now)))
	db.Close()

	now = time.Now()
	DefaultDBOptions.MMapAtStartup = !DefaultDBOptions.MMapAtStartup
	db, _ = Launch(testingDBOptions)
	t.Logf(fmt.Sprintf(logTemplate, DefaultDBOptions.MMapAtStartup, time.Since(now)))
	db.Close()
}

func TestDB_Stat(t *testing.T) {
	db, _ := Launch(testingDBOptions)
	defer destroyDB(db)

	var stat *Stat

	// No key
	stat = db.Stat()
	assert.Zero(t, stat.KeyNumber)
	assert.Zero(t, stat.DataFileNumber)
	assert.Zero(t, stat.ReclaimableSize)
	assert.Zero(t, stat.DiskSize)
	t.Log(stat)

	// So many keys
	keyNumber := 10000
	for i := 1; i <= keyNumber; i++ {
		db.Put(utils.NewKey(i), utils.NewRandomValue(4096))
	}

	stat = db.Stat()
	assert.Equal(t, keyNumber, int(stat.KeyNumber))
	entries, _ := os.ReadDir(db.options.Directory)
	assert.Equal(t, len(entries)-1, int(stat.DataFileNumber))
	assert.Equal(t, len(db.inactiveFiles)+1, int(stat.DataFileNumber))
	assert.True(t, stat.DiskSize <= DefaultDBOptions.MaxDataFileSize*int64(stat.DataFileNumber))
	assert.Positive(t, stat.DiskSize)
	t.Log(stat)
}

func TestDB_Backup(t *testing.T) {
	db1, _ := Launch(testingDBOptions)
	defer destroyDB(db1)

	keyNumber := 10000
	for i := 1; i <= keyNumber; i++ {
		db1.Put(utils.NewKey(i), utils.NewRandomValue(4096))
	}

	dir := "/tmp/baradb-backup"
	err := db1.Backup(dir)
	assert.Nil(t, err)

	n1 := db1.Stat().DataFileNumber
	entries, err := os.ReadDir(dir)
	assert.Nil(t, err)
	n2 := len(entries)
	assert.Equal(t, int(n1), n2)

	s1, err := utils.DirSize(db1.options.Directory)
	assert.Nil(t, err)
	s2, err := utils.DirSize(dir)
	assert.Nil(t, err)
	assert.Equal(t, s1, s2)

	opts2 := testingDBOptions
	opts2.Directory = dir
	db2, _ := Launch(opts2)
	defer destroyDB(db2)

	assert.EqualValues(t, db1.Stat(), db2.Stat())
}

func TestDB_Fork(t *testing.T) {
	db1, _ := Launch(testingDBOptions)
	defer destroyDB(db1)

	db2, err := db1.Fork("/tmp/baradb-fork")
	defer destroyDB(db2)

	assert.Nil(t, err)
	assert.NotNil(t, db2)
}
