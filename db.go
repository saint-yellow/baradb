package baradb

import (
	"sync"

	"github.com/saint-yellow/baradb/data"
	"github.com/saint-yellow/baradb/index"
)

// DB baradb engine instance
type DB struct {
	mu            *sync.RWMutex
	options       Options
	activeFile    *data.DataFile
	inactiveFiles map[uint32]*data.DataFile
	index         index.Indexer
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

	if ok := db.index.Put(key, position); !ok {
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

	position := db.index.Get(key)
	if position == nil {
		return nil, ErrKeyNotFound
	}

	var dataFile *data.DataFile
	if position.FileID == db.activeFile.FileID {
		dataFile = db.activeFile
	} else {
		dataFile = db.inactiveFiles[position.FileID]
	}
	if dataFile == nil {
		return nil, ErrFileNotFound
	}

	lr, err := dataFile.GetLogRecord(position.Offset)
	if err != nil {
		return nil, nil
	}

	if lr.Type == data.DeletedLogRecord {
		return nil, ErrKeyNotFound
	}

	return lr.Value, nil
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
		if err := db.activeFile.IOHandler.Close(); err != nil {
			return nil, err
		}

		db.inactiveFiles[db.activeFile.FileID] = db.activeFile

		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	if _, err := db.activeFile.IOHandler.Write(encodedRecord); err != nil {
		return nil, err
	}
	db.activeFile.WriteOffset += encodedLength

	if db.options.WriteSync {
		if err := db.activeFile.IOHandler.Sync(); err != nil {
			return nil, err
		}
	}

	position := &data.LogRecordPosition{FileID: db.activeFile.FileID, Offset: db.activeFile.WriteOffset}
	return position, nil
}

// setActiveFile sets an active data file in DB
// The caller must have a mutex lock before calling this function
func (db *DB) setActiveFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileID + 1
	}

	file, err := data.OpenDataFile(db.options.Directory, initialFileID)
	if err != nil {
		return err
	}
	db.activeFile = file
	return nil
}
