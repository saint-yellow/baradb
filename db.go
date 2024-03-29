package baradb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"

	"github.com/saint-yellow/baradb/data"
	"github.com/saint-yellow/baradb/index"
	"github.com/saint-yellow/baradb/io_handler"
	"github.com/saint-yellow/baradb/utils"
)

const (
	tranNoKey    = "tran-no"
	fileLockName = "flock"
)

// DB represents a baradb engine
type DB struct {
	mu               *sync.RWMutex             // Mutial exclusion lock
	options          DBOptions                 // DB Options
	fileIDs          []int                     // File IDs of all data files
	activeFile       *data.DataFile            // Active data file, readable and writeable
	inactiveFiles    map[uint32]*data.DataFile // Inactive data files, readable but unwritable
	index            index.Index               // In-memory index
	tranNo           uint64                    // Globally increasing serial number of a transaction
	isMerging        bool                      // Whether the DB is merging
	tranNoFileExists bool                      // Whether a file about transaction serial number exists
	isFirstLaunch    bool                      // Whether the DB engine is launched for the first time
	fileLock         *flock.Flock              // File lock
	bytesWritten     uint                      // Bytes written by the DB
	reclaimSize      int64                     // Size of invalid data
}

// Launch launches a DB engine instance
func Launch(options DBOptions) (*DB, error) {
	// make sure that options are valid
	if err := checkDBOptions(options); err != nil {
		return nil, err
	}

	var isFirstLaunch bool
	// make sure the existance of the directory in options
	if _, err := os.Stat(options.Directory); os.IsNotExist(err) {
		isFirstLaunch = true
		if err := os.Mkdir(options.Directory, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// Get the file lock of the directory
	fileLock := flock.New(filepath.Join(options.Directory, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsed
	}

	entries, err := os.ReadDir(options.Directory)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isFirstLaunch = true
	}

	// initialize DB instance
	db := &DB{
		mu:            new(sync.RWMutex),
		options:       options,
		activeFile:    nil,
		inactiveFiles: make(map[uint32]*data.DataFile),
		index: index.New(
			options.IndexType,
			options.Directory,
			options.SyncWrites,
		),
		isFirstLaunch: isFirstLaunch,
		fileLock:      fileLock,
	}

	if err := db.loadMergenceFiles(); err != nil {
		return nil, err
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// If the DB engine uses B+ tree as its index, then it don't need to load index from files
	if options.IndexType != index.BPtree {
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}

	// Reset the type of I/O handler
	if db.options.MMapAtStartup {
		if err := db.resetIOHandler(); err != nil {
			return nil, err
		}
	}

	// Get a transaction serial number from file
	if options.IndexType == index.BPtree {
		if err := db.loadTranNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOffset += size
		}
	}

	return db, nil
}

// Fork creates a new DB engine instance mainly for merging data
func (db *DB) Fork(directory string) (*DB, error) {
	opts := db.options
	opts.Directory = directory

	if err := checkDBOptions(opts); err != nil {
		return nil, err
	}

	return Launch(opts)
}

// Backup copies the DB's data files to a given directory.
//
// NOTE: only data files will be copied, other files like tran-no, flock will be ignored.
func (db *DB) Backup(directory string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	excludedFiles := []string{
		fileLockName,
	}
	return utils.CopyDir(db.options.Directory, directory, excludedFiles)
}

// Put Writes data to the DB engine
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	lr := &data.LogRecord{
		Key:   data.EncodeKey(key, nonTranNo),
		Value: value,
		Type:  data.NormalLogRecord,
	}

	// Append the data to the current active data file
	lrp, err := db.appendLogRecord(lr, true)
	if err != nil {
		return err
	}

	// Update the data in the index
	if oldLRP := db.index.Put(key, lrp); oldLRP != nil {
		db.reclaimSize += int64(oldLRP.Size)
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

	lrp := db.index.Get(key)
	if lrp == nil {
		return nil, ErrKeyNotFound
	}

	// Get value from a data file
	return db.getValueByPosition(lrp)
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
		return nil, err
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
	if p := db.index.Get(key); p == nil {
		return nil
	}

	lr := &data.LogRecord{
		Key:  data.EncodeKey(key, nonTranNo),
		Type: data.DeletedLogRecord,
	}

	//
	lrp, err := db.appendLogRecord(lr, true)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(lrp.Size)

	//
	oldLRP, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldLRP != nil {
		db.reclaimSize += int64(oldLRP.Size)
	}

	return nil
}

// appendLogRecord appends a log record to the current active data file in DB
func (db *DB) appendLogRecord(lr *data.LogRecord, needMutex bool) (*data.LogRecordPosition, error) {
	if needMutex {
		db.mu.Lock()
		defer db.mu.Unlock()
	}

	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	elr, n := data.EncodeLogRecord(lr)
	if db.activeFile.WriteOffset+n > db.options.MaxDataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.inactiveFiles[db.activeFile.FileID] = db.activeFile

		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	writeOffset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(elr); err != nil {
		return nil, err
	}

	// Accumulate the witten bytes
	db.bytesWritten += uint(n)

	// Persist data
	needSync := db.options.SyncWrites
	if !needSync && db.options.SyncThreshold > 0 && db.bytesWritten >= db.options.SyncThreshold {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// Reset the written bytes
		if db.bytesWritten > 0 {
			db.bytesWritten = 0
		}
	}

	lrp := &data.LogRecordPosition{
		FileID: db.activeFile.FileID,
		Offset: writeOffset,
		Size:   uint32(n),
	}
	return lrp, nil
}

// setActiveFile sets an active data file in DB
// The caller must have a mutex lock before calling this function
func (db *DB) setActiveFile() error {
	var fileID uint32 = 0
	if db.activeFile != nil {
		fileID = db.activeFile.FileID + 1
	}

	file, err := data.OpenDataFile(db.options.Directory, fileID, io_handler.FileIOHandler)
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
		ioHandlerType := io_handler.FileIOHandler
		if db.options.MMapAtStartup {
			ioHandlerType = io_handler.MemoryMappedIOHandler
		}
		file, err := data.OpenDataFile(db.options.Directory, uint32(fileID), ioHandlerType)
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

// loadIndexFromDataFiles Build index from data files on the disk and load the built index to the memory
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIDs) == 0 {
		return nil
	}

	hasMerged, nonMergedFileID := false, uint32(0)
	mergedFilePath := filepath.Join(db.options.Directory, data.MergedFileName)
	if _, err := os.Stat(mergedFilePath); err == nil {
		fileID, err := db.getNonMergedFileID(db.options.Directory)
		if err != nil {
			return err
		}
		hasMerged, nonMergedFileID = true, fileID
	}

	updateIndex := func(key []byte, lrt data.LogRecordType, lrp *data.LogRecordPosition) {
		var oldLRP *data.LogRecordPosition
		if lrt == data.DeletedLogRecord {
			oldLRP, _ = db.index.Delete(key)
			db.reclaimSize += int64(lrp.Size)
		} else {
			oldLRP = db.index.Put(key, lrp)
		}

		if oldLRP != nil {
			db.reclaimSize += int64(oldLRP.Size)
		}
	}

	transactionRecords := make(map[uint64][]*data.TransactionRecord)

	currentTransNo := nonTranNo

	for i, fid := range db.fileIDs {
		fileID := uint32(fid)
		if hasMerged && fileID < nonMergedFileID {
			continue
		}
		var file *data.DataFile
		if fileID == db.activeFile.FileID {
			file = db.activeFile
		} else {
			file = db.inactiveFiles[fileID]
		}

		var offset int64 = 0
		for {
			lr, n, err := file.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			lrp := &data.LogRecordPosition{
				FileID: fileID,
				Offset: offset,
				Size:   uint32(n),
			}

			// Decode the key of the log record to get the real key and the transaction serial number
			lrKey, tranNo := data.DecodeKey(lr.Key)
			if tranNo == nonTranNo {
				// If transaction serial number is 0, then update the in-memory index directly
				// Because it is not a transactional operation
				updateIndex(lrKey, lr.Type, lrp)
			} else {
				// Transactional operation
				if lr.Type == data.TransactionFinishedLogRecord {
					for _, tr := range transactionRecords[tranNo] {
						updateIndex(tr.Log.Key, tr.Log.Type, tr.Position)
					}
					delete(transactionRecords, tranNo)
				} else {
					lr.Key = lrKey
					tr := &data.TransactionRecord{
						Log:      lr,
						Position: lrp,
					}
					transactionRecords[tranNo] = append(transactionRecords[tranNo], tr)
				}
			}

			// Update transaction serial number
			if tranNo > currentTransNo {
				currentTransNo = tranNo
			}

			offset += n
		}

		if i == len(db.fileIDs)-1 {
			db.activeFile.WriteOffset = offset
		}
	}

	// Update the DB's transaction serial number
	db.tranNo = currentTransNo

	return nil
}

// loadIndexFromHintFile loads index from a hint file
func (db *DB) loadIndexFromHintFile() error {
	filePath := filepath.Join(db.options.Directory, data.HintFileName)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil
	}

	file, err := data.OpenHintFile(db.options.Directory)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		lr, n, err := file.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		lrp := data.DecodeLogRecordPosition(lr.Value)
		db.index.Put(lr.Key, lrp)
		offset += n
	}
	return nil
}

// loadTranNo Gets a transaction serial number from a file
func (db *DB) loadTranNo() error {
	filePath := filepath.Join(db.options.Directory, data.TranNoFileName)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil
	}

	file, err := data.OpenTranNoFile(db.options.Directory)
	if err != nil {
		return err
	}

	lr, _, err := file.ReadLogRecord(0)
	if err != nil {
		return err
	}

	tranNo, err := strconv.ParseUint(string(lr.Value), 10, 64)
	if err != nil {
		return err
	}

	db.tranNo = tranNo
	db.tranNoFileExists = true

	return os.Remove(filePath)
}

func (db *DB) resetIOHandler() error {
	if db.activeFile == nil {
		return nil
	}

	var err error
	err = db.activeFile.SetIOHandler(io_handler.FileIOHandler)
	if err != nil {
		return err
	}
	for _, file := range db.inactiveFiles {
		err = file.SetIOHandler(io_handler.FileIOHandler)
		if err != nil {
			return err
		}
	}
	return nil
}

// ListKeys gets all keys in the DB engine
func (db *DB) ListKeys() [][]byte {
	iter := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())

	index := 0
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

	iter := db.index.Iterator(false)
	defer iter.Close()
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
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("Failed to unlock the directory, %v", err))
		}
	}()

	// There is nothing to do if the DB engine has no data file
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	var err error

	err = db.index.Close()
	if err != nil {
		return err
	}

	// Save the current transaction serial numbers
	file, err := data.OpenTranNoFile(db.options.Directory)
	if err != nil {
		return err
	}
	lr := &data.LogRecord{
		Key:   []byte(tranNoKey),
		Value: []byte(strconv.FormatUint(db.tranNo, 10)),
	}
	elr, _ := data.EncodeLogRecord(lr)
	err = file.Write(elr)
	if err != nil {
		return err
	}
	err = file.Sync()
	if err != nil {
		return err
	}

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

// Stat returns statistical information of the DB engine
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dataFileNumber := len(db.inactiveFiles)
	if db.activeFile != nil {
		dataFileNumber += 1
	}

	dataFileSize, err := utils.DirSize(db.options.Directory)
	if err != nil {
		panic(fmt.Sprintf("Failed to read the directory of the DB engine: %v", err))
	}

	stat := &Stat{
		KeyNumber:       uint(db.index.Size()),
		DataFileNumber:  uint(dataFileNumber),
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dataFileSize,
	}
	return stat
}
