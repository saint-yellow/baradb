package io_handler

import (
	"os"
)

// fileIO Standard file I/O
type fileIO struct {
	fd *os.File // File descriptor
}

func newFileIO(filePath string) (*fileIO, error) {
	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePermission)
	if err != nil {
		return nil, err
	}
	fio := &fileIO{fd: fd}
	return fio, nil
}

func (fio *fileIO) Read(data []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(data, offset)
}

func (fio *fileIO) Write(data []byte) (int, error) {
	return fio.fd.Write(data)
}

func (fio *fileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *fileIO) Close() error {
	return fio.fd.Close()
}

func (fio *fileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
