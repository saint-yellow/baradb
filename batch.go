package baradb

import (
	"sync"
	"sync/atomic"

	"github.com/saint-yellow/baradb/data"
	"github.com/saint-yellow/baradb/index"
)

// nonTranNo This is not a transaction serial number
const nonTranNo uint64 = 0

// tranFinishedKey This is a key that indicates a transaction is finished
var tranFinishedKey = []byte("transaction-finished")

// WriteBatch A transaction that batch writes data and makes sure atomicity
type WriteBatch struct {
	mu            *sync.RWMutex              // Lock
	db            *DB                        // DB engine
	options       WriteBatchOptions          // options for batch writing
	pendingWrites map[string]*data.LogRecord // data (log records) pending to be writen
}

// NewWriteBatch initializes a write batch in the DB engine
func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == index.BPtree && !db.tranNoFileExists && !db.isFirstLaunch {
		panic("Can not use a write batch since the tran-no file does not exist")
	}

	wb := &WriteBatch{
		mu:            new(sync.RWMutex),
		db:            db,
		options:       options,
		pendingWrites: make(map[string]*data.LogRecord),
	}
	return wb
}

// Put writes data
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	lr := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.NormalLogRecord,
	}

	// Temporarily store a log record which is pending to be written
	wb.pendingWrites[string(key)] = lr
	return nil
}

// Delete deletes data
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// Try to find the corresponding position in the index
	// If the position is not found, then delete the corresponding log record in this transaction
	lrp := wb.db.index.Get(key)
	if lrp == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	lr := &data.LogRecord{
		Key:  key,
		Type: data.DeletedLogRecord,
	}

	// Temporarily store a log record which is pending to be written
	wb.pendingWrites[string(key)] = lr
	return nil
}

// Commit commits the transaction, writes the pending data to the disk and updates the in-memory index
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// No pending data
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	// To much pending data
	if len(wb.pendingWrites) > wb.options.MaxBatchNumber {
		return ErrExceedMaxBatchNumber
	}

	// Lock the DB to make sure the serialization of transaction Commit
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// Get the latest serial number of this transaction
	transNo := atomic.AddUint64(&wb.db.tranNo, 1)

	// Write pending data to a data file
	positions := make(map[string]*data.LogRecordPosition)
	for _, lr := range wb.pendingWrites {
		lrp, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   data.EncodeKey(lr.Key, transNo),
			Value: lr.Value,
			Type:  lr.Type,
		}, false)
		if err != nil {
			return err
		}
		positions[string(lr.Key)] = lrp
	}

	// Add a log record that means this transaction is finished
	_, err := wb.db.appendLogRecord(&data.LogRecord{
		Key:  data.EncodeKey(tranFinishedKey, transNo),
		Type: data.TransactionFinishedLogRecord,
	}, false)
	if err != nil {
		return err
	}

	// Sync the written data to the active data file
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		err := wb.db.activeFile.Sync()
		if err != nil {
			return err
		}
	}

	// Update in-memory index
	for _, lr := range wb.pendingWrites {
		lrp := positions[string(lr.Key)]
		var oldLRP *data.LogRecordPosition

		switch lr.Type {
		case data.DeletedLogRecord:
			oldLRP, _ = wb.db.index.Delete(lr.Key)
		case data.NormalLogRecord:
			oldLRP = wb.db.index.Put(lr.Key, lrp)
		}

		if oldLRP != nil {
			wb.db.reclaimSize += int64(oldLRP.Size)
		}
	}

	// Clear the panding data
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}
