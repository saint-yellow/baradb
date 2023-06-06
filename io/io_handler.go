package io

const DataFilePermission = 644

// IOHandler Abstract I/O interface
type IOHandler interface {
	// Read Read corresponding data from the specific position of a file
	Read([]byte, int64) (int, error)
	// Write Write data to a file
	Write([]byte) (int, error)
	// Sync Persistent data
	Sync() error
	// Close Close a file
	Close() error
}
