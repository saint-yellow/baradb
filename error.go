package baradb

import "errors"

// User-defined errors
var (
	ErrKeyIsEmpty                = errors.New("The key is empty")
	ErrIndexUpdateFailed         = errors.New("Failed to update index")
	ErrFileNotFound              = errors.New("File not found")
	ErrKeyNotFound               = errors.New("Key not found")
	ErrDirectoryIsEmpty          = errors.New("Data path is empty")
	ErrMaxDataFileSizeIsNegative = errors.New("The maximum size of data file is negative")
	ErrDirectoryCorrupted        = errors.New("Maybe the directory of the DB is corrupted")
	ErrExceedMaxBatchNumber      = errors.New("Exceed the maximum batch number")
	ErrMergenceIsInProgress      = errors.New("Mergence is in progress, try again later")
	ErrDatabaseIsUsed            = errors.New("The database is used by other process")
)
