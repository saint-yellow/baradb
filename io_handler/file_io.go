package io_handler

import (
	"os"
)

// FileIO Standard file I/O
type FileIO struct {
	fd *os.File // File descriptor
}

func NewFileIO(filePath string) (*FileIO, error) {
	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePermission)
	if err != nil {
		return nil, err
	}
	fio := &FileIO{fd: fd}
	return fio, nil
}

func (fio *FileIO) Read(data []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(data, offset)
}

func (fio *FileIO) Write(data []byte) (int, error) {
	return fio.fd.Write(data)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
