package io_handler

import (
	"os"

	"golang.org/x/exp/mmap"
)

// memoryMappedIO represents a memory-mapped I/O handler
type memoryMappedIO struct {
	readerAt *mmap.ReaderAt
}

// newMemoryMappedIO initializes a memory-mapped I/O handler
func newMemoryMappedIO(filePath string) (*memoryMappedIO, error) {
	_, err := os.OpenFile(filePath, os.O_CREATE, DataFilePermission)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(filePath)
	if err != nil {
		return nil, err
	}

	m := &memoryMappedIO{
		readerAt: readerAt,
	}
	return m, nil
}

// Read Read corresponding data from the specific position of a file
func (m *memoryMappedIO) Read(b []byte, n int64) (int, error) {
	return m.readerAt.ReadAt(b, n)
}

// Write Write data to a file
func (m *memoryMappedIO) Write(b []byte) (int, error) {
	panic("Operation unsupported")
}

// Sync Persistent data
func (m *memoryMappedIO) Sync() error {
	panic("Operation unsupported")
}

// Close Close a file
func (m *memoryMappedIO) Close() error {
	return m.readerAt.Close()
}

// Size Get the size of a data file (unit: B)
func (m *memoryMappedIO) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}
