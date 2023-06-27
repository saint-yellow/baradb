package baradb

import "errors"

// User-defined errors
var (
	ErrKeyIsEmpty                = errors.New("the key is empty")
	ErrIndexUpdateFailed         = errors.New("failed to update index")
	ErrFileNotFound              = errors.New("file not found")
	ErrKeyNotFound               = errors.New("key not found")
	ErrDirectoryIsEmpty          = errors.New("data path is empty")
	ErrMaxDataFileSizeIsNegative = errors.New("the maximum size of data file is negative")
	ErrDirectoryCorrupted        = errors.New("maybe the directory of the DB is corrupted")
	ErrExceedMaxBatchNumber      = errors.New("exceed the maximum batch number")
	ErrMergenceIsInProgress      = errors.New("mergence is in progress, try again later")
	ErrDatabaseIsUsed            = errors.New("the database is used by other process")
	ErrInvalidMergenceThreshold  = errors.New("invalid mergence threshold")
	ErrNoMoreDiskSpace           = errors.New("no more disk space to store data")
)
