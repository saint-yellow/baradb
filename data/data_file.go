package data

import "github.com/saint-yellow/baradb/io"

// DataFile represents a data file in a baradb engine instance
type DataFile struct {
	FileID      uint32       // FileID identifies of a data file
	WriteOffset int64        // WriteOffset indicates the offset of the written data in a data file
	IOHandler   io.IOHandler // IOHandler is used to handle I/O operations in a data file
}

// OpenDataFile open a data file
func OpenDataFile(directory string, fileID uint32) (*DataFile, error) {
	return nil, nil
}

// GetLogRecord Get single log record by given offset in a data file
func (df *DataFile) GetLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return df.IOHandler.Sync()
}

func (df *DataFile) Write(data []byte) (int, error) {
	return df.IOHandler.Write(data)
}
