package baradb

import (
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/saint-yellow/baradb/data"
	"github.com/saint-yellow/baradb/index"
)

// DB baradb engine instance
type DB struct {
	mu            *sync.RWMutex
	options       Options
	fileIDs       []int
	activeFile    *data.DataFile
	inactiveFiles map[uint32]*data.DataFile
	index         index.Indexer
}

// LaunchDB launches a DB engine instance
func LaunchDB(options Options) (*DB, error) {
	// make sure that options are validate
	if err := checkOptions(options); err != nil {
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
		inactiveFiles: make(map[uint32]*data.DataFile, 0),
		index:         index.NewIndexer(options.IndexerType),
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

	lr, _, err := dataFile.ReadLogRecord(position.Offset)
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

// loadDataFiles Load data files in the disk
func (db *DB) loadDataFiles() error {
	// Read the directory of the DB to get all the data files
	entries, err := os.ReadDir(db.options.Directory)
	if err != nil {
		return err
	}

	// Collect file ID from every single data file with a name like 00001.data
	var fileIDs []int
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
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
		dataFile, err := data.OpenDataFile(db.options.Directory, uint32(fileID))
		if err != nil {
			return err
		}

		// The last data file shoule be the current active one of the DB engine
		if i == len(fileIDs)-1 {
			db.activeFile = dataFile
		} else {
			db.inactiveFiles[uint32(fileID)] = dataFile
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
			if lr.Type == data.DeletedLogRecord {
				db.index.Delete(lr.Key)
			} else {
				db.index.Put(lr.Key, lrp)
			}

			offset += n
		}

		if i == len(db.fileIDs)-1 {
			db.activeFile.WriteOffset = offset
		}
	}

	return nil
}
