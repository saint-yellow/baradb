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
	// Size Get the size of a data file (unit: B)
	Size() (int64, error)
}

type IOHandlerType = int8

const (
	FileIOHandler IOHandlerType = iota + 1
)

// NewIOHandler Constructs an IOHandler, such as FileIO
func NewIOHandler(t IOHandlerType, filePath string) (IOHandler, error) {
	switch t {
	case FileIOHandler:
		return NewFileIO(filePath)
	default:
		panic("Unsupproted I/O handler type")
	}
}
