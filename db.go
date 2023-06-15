package baradb

import (
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/saint-yellow/baradb/data"
	"github.com/saint-yellow/baradb/indexer"
)

// DB baradb engine instance
type DB struct {
	mu            *sync.RWMutex
	options       DBOptions
	fileIDs       []int
	activeFile    *data.DataFile
	inactiveFiles map[uint32]*data.DataFile
	indexer       indexer.Indexer
}

// LaunchDB launches a DB engine instance
func LaunchDB(options DBOptions) (*DB, error) {
	// make sure that options are valid
	if err := checkDBOptions(options); err != nil {
		return nil, err
	}

	// make sure the existance of the directory in options
	if _, err := os.Stat(options.Directory); os.IsNotExist(err) {
		if err := os.Mkdir(options.Directory, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// initialize DB instance
	db := &DB{
		mu:            new(sync.RWMutex),
		options:       options,
		activeFile:    nil,
		inactiveFiles: make(map[uint32]*data.DataFile),
		indexer:       indexer.NewIndexer(options.IndexerType),
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if err := db.loadIndex(); err != nil {
		return nil, err
	}

	return db, nil
}

// Put Writes data to the DB engine
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	lr := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.NormalLogRecord,
	}

	position, err := db.appendLogRecord(lr)
	if err != nil {
		return err
	}

	if ok := db.indexer.Put(key, position); !ok {
		return ErrIndexUpdateFailed
	}

	return nil

}

// Get Reads data from the DB engine by a given key
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	position := db.indexer.Get(key)
	if position == nil {
		return nil, ErrKeyNotFound
	}

	// Get value from a data file
	return db.getValueByPosition(position)
}

// getValueByPosition gets corresponding value by given position
func (db *DB) getValueByPosition(lrp *data.LogRecordPosition) ([]byte, error) {
	// Confirm which data file the keys is stored in
	var file *data.DataFile
	if lrp.FileID == db.activeFile.FileID {
		file = db.activeFile
	} else {
		file = db.inactiveFiles[lrp.FileID]
	}
	if file == nil {
		return nil, ErrFileNotFound
	}

	lr, _, err := file.ReadLogRecord(lrp.Offset)
	if err != nil {
		return nil, nil
	}

	if lr.Type == data.DeletedLogRecord {
		return nil, ErrKeyNotFound
	}

	return lr.Value, nil
}

// Delete Delete data by the given key
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// Maybe the data never exist, or it has been deleted before
	if p := db.indexer.Get(key); p == nil {
		return nil
	}

	lr := &data.LogRecord{
		Key:  key,
		Type: data.DeletedLogRecord,
	}

	if _, err := db.appendLogRecord(lr); err != nil {
		return err
	}

	if ok := db.indexer.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// appendLogRecord appends a log record to the current active data file in DB
func (db *DB) appendLogRecord(lr *data.LogRecord) (*data.LogRecordPosition, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	encodedRecord, encodedLength := data.EncodeLogRecord(lr)
	if db.activeFile.WriteOffset+encodedLength > db.options.MaxDataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.inactiveFiles[db.activeFile.FileID] = db.activeFile

		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	writeOffset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encodedRecord); err != nil {
		return nil, err
	}

	if db.options.WriteSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	position := &data.LogRecordPosition{FileID: db.activeFile.FileID, Offset: writeOffset}
	return position, nil
}

// setActiveFile sets an active data file in DB
// The caller must have a mutex lock before calling this function
func (db *DB) setActiveFile() error {
	var fileID uint32 = 0
	if db.activeFile != nil {
		fileID = db.activeFile.FileID + 1
	}

	file, err := data.OpenDataFile(db.options.Directory, fileID)
	if err != nil {
		return err
	}
	db.activeFile = file
	return nil
}

// loadDataFiles Load data files to the DB engine
func (db *DB) loadDataFiles() error {
	// Read the directory of the DB to get all the data files
	entries, err := os.ReadDir(db.options.Directory)
	if err != nil {
		return err
	}

	// Collect file ID from every single data file with a name like 000000001.data
	var fileIDs []int
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitParts := strings.Split(entry.Name(), ".")
			fileID, err := strconv.Atoi(splitParts[0])
			if err != nil {
				return ErrDirectoryCorrupted
			}
			fileIDs = append(fileIDs, fileID)
		}
	}
	// Sort the collected file IDs
	sort.Ints(fileIDs)
	db.fileIDs = fileIDs

	// Open every single data file sequantially
	for i, fileID := range fileIDs {
		file, err := data.OpenDataFile(db.options.Directory, uint32(fileID))
		if err != nil {
			return err
		}

		// The last data file shoule be the current active one of the DB engine
		if i == len(fileIDs)-1 {
			db.activeFile = file
		} else {
			db.inactiveFiles[uint32(fileID)] = file
		}
	}

	return nil
}

// loadIndex Build index from data files on the disk and load the built index to the memory
func (db *DB) loadIndex() error {
	if len(db.fileIDs) == 0 {
		return nil
	}

	for i, fid := range db.fileIDs {
		fileID := uint32(fid)
		var dataFile *data.DataFile
		if fileID == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.inactiveFiles[fileID]
		}

		var offset int64 = 0
		for {
			lr, n, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			lrp := &data.LogRecordPosition{FileID: fileID, Offset: offset}
			var ok bool
			if lr.Type == data.DeletedLogRecord {
				ok = db.indexer.Delete(lr.Key)
			} else {
				ok = db.indexer.Put(lr.Key, lrp)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}

			offset += n
		}

		if i == len(db.fileIDs)-1 {
			db.activeFile.WriteOffset = offset
		}
	}

	return nil
}

// NewItrerator initializes an iterator of DB engine
func (db *DB) NewItrerator(options indexer.IteratorOptions) *Iterator {
	iterator := &Iterator{
		indexIterator: db.indexer.Iterator(options.Reverse),
		db:            db,
		options:       options,
	}

	return iterator
}

// ListKeys gets all keys in the DB engine
func (db *DB) ListKeys() [][]byte {
	iter := db.indexer.Iterator(false)
	keys := make([][]byte, db.indexer.Size())

	var index int = 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		keys[index] = iter.Key()
		index++
	}

	return keys
}

// userOperationFunc User-defined function
type userOperationFunc = func(key, value []byte) bool

// Fold retrieves all the data and iteratively executes an user-specified operation (UDF)
// Once the UDF failed, the iteration will stop intermediatelly
func (db *DB) Fold(fn userOperationFunc) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iter := db.indexer.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := db.getValueByPosition(iter.Value())
		if err != nil {
			return err
		}

		if ok := fn(iter.Key(), value); !ok {
			break
		}
	}

	return nil
}

// Close closes the DB engine
func (db *DB) Close() error {
	// There is nothing to do if the DB engine has no data file
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	var err error

	// Close the current active data file
	err = db.activeFile.Close()
	if err != nil {
		return err
	}

	// Close all inactive data files
	for _, file := range db.inactiveFiles {
		err = file.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// Sync persistence of data in active data file of the DB engine
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// The inactive data file was already been synced before
	// So the current active data file is the only thing to handle
	return db.activeFile.Sync()
}

// NewWriteBatch initializes a write batch in the DB engine
func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	wb := &WriteBatch{
		mu:            new(sync.RWMutex),
		db:            db,
		options:       options,
		pendingWrites: make(map[string]*data.LogRecord),
	}
	return wb
}
