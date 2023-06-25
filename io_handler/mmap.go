package io_handler

import (
	"os"

	"golang.org/x/exp/mmap"
)

// MMap in-memory mapper
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMap initializes a MMap
func NewMMap(filePath string) (*MMap, error) {
	_, err := os.OpenFile(filePath, os.O_CREATE, DataFilePermission)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(filePath)
	if err != nil {
		return nil, err
	}

	m := &MMap{
		readerAt: readerAt,
	}
	return m, nil
}

// Read Read corresponding data from the specific position of a file
func (m *MMap) Read(b []byte, n int64) (int, error) {
	return m.readerAt.ReadAt(b, n)
}

// Write Write data to a file
func (m *MMap) Write(b []byte) (int, error) {
	panic("Operation unsupported")
}

// Sync Persistent data
func (m *MMap) Sync() error {
	panic("Operation unsupported")
}

// Close Close a file
func (m *MMap) Close() error {
	return m.readerAt.Close()
}

// Size Get the size of a data file (unit: B)
func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}
