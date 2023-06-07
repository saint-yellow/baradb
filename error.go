package baradb

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("The key is empty")
	ErrIndexUpdateFailed = errors.New("Failed to update index")
	ErrFileNotFound      = errors.New("File not found")
	ErrKeyNotFound       = errors.New("Key not found")
)
