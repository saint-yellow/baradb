package baradb

import "errors"

var (
	ErrKeyIsEmpty                = errors.New("The key is empty")
	ErrIndexUpdateFailed         = errors.New("Failed to update index")
	ErrFileNotFound              = errors.New("File not found")
	ErrKeyNotFound               = errors.New("Key not found")
	ErrDirectoryIsEmpty          = errors.New("Data path is empty")
	ErrMaxDataFileSizeIsNegative = errors.New("The maximum size of data file is negative")
	ErrDirectoryCorrupted        = errors.New("Maybe the directory of the DB is corrupted")
)
