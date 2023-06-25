package baradb

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/saint-yellow/baradb/data"
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

	// Try to find the corresponding position in the indexer
	// If the position is not found, then delete the corresponding log record in this transaction
	lrp := wb.db.indexer.Get(key)
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
			Key:   encodeLogRecordKeyWithTranNo(lr.Key, transNo),
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
		Key:  encodeLogRecordKeyWithTranNo(tranFinishedKey, transNo),
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
		if lr.Type == data.NormalLogRecord {
			wb.db.indexer.Put(lr.Key, lrp)
		} else if lr.Type == data.DeletedLogRecord {
			wb.db.indexer.Delete(lr.Key)
		}
	}

	// Clear the panding data
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// encodeLogRecoedKeyWithTranNo encodes a key of a log record and a transaction serial number
func encodeLogRecordKeyWithTranNo(key []byte, tranNo uint64) []byte {
	encodedTranNo := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(encodedTranNo[:], tranNo)

	encodedKey := make([]byte, n+len(key))
	copy(encodedKey[:n], encodedTranNo[:n])
	copy(encodedKey[n:], key)

	return encodedKey
}

// decodeLogRecordKeyWithTranNo decodes a key to get a key of a log record and a transaction serial number
func decodeLogRecordKeyWithTranNo(key []byte) ([]byte, uint64) {
	tranNo, n := binary.Uvarint(key)
	decodedKey := key[n:]
	return decodedKey, tranNo
}