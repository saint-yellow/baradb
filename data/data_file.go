package data

import (
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/saint-yellow/baradb/io_handler"
)

const (
	DataFileNameSuffix = ".data"
	HintFileName       = "hint-index"
	MergedFileName     = "merged"
	TranNoFileName     = "tran-no"
)

// DataFile represents a data file in a baradb engine instance
type DataFile struct {
	FileID      uint32               // FileID identifies of a data file
	WriteOffset int64                // WriteOffset indicates the offset of the written data in a data file
	IOHandler   io_handler.IOHandler // IOHandler is used to handle I/O operations in a data file
}

// newDataFile constructs a data file
func newDataFile(fileaPath string, fileID uint32) (*DataFile, error) {
	ioHandler, err := io_handler.NewIOHandler(io_handler.FileIOHandler, fileaPath)
	if err != nil {
		return nil, err
	}
	file := &DataFile{
		FileID:      fileID,
		WriteOffset: 0,
		IOHandler:   ioHandler,
	}
	return file, nil
}

func GetDataFilePath(directory string, fileID uint32) string {
	fileName := fmt.Sprintf("%09d%s", fileID, DataFileNameSuffix)
	filePath := filepath.Join(directory, fileName)
	return filePath
}

// OpenDataFile opens a data file
func OpenDataFile(directory string, fileID uint32) (*DataFile, error) {
	filePath := GetDataFilePath(directory, fileID)
	return newDataFile(filePath, fileID)
}

// OpenHintFile opens a hint file
func OpenHintFile(directory string) (*DataFile, error) {
	filePath := filepath.Join(directory, HintFileName)
	return newDataFile(filePath, 0)
}

// OpenMergedFile opens a merged file
func OpenMergedFile(directory string) (*DataFile, error) {
	filePath := filepath.Join(directory, MergedFileName)
	return newDataFile(filePath, 0)
}

// OpenTranNoFile opens a file about a transaction serial number
func OpenTranNoFile(directory string) (*DataFile, error) {
	filePath := filepath.Join(directory, TranNoFileName)
	return newDataFile(filePath, 0)
}

// ReadLogRecord reads single log record by given offset in a data file
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// Get the size of the data file
	fileSize, err := df.IOHandler.Size()
	if err != nil {
		return nil, 0, err
	}

	// Make sure the bytes of the header are always included in the data file
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}

	// Get the header
	headerBuffer, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// Decode the header
	header, headerSize := decodeLogRecordHeader(headerBuffer)
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	logRecordSize := headerSize + keySize + valueSize

	// Construct a log record
	logRecord := &LogRecord{
		Type: header.logRecordType,
	}

	if keySize > 0 || valueSize > 0 {
		kvBuffer, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvBuffer[:keySize]
		logRecord.Value = kvBuffer[keySize:]
	}

	// Validate the CRC value of the log record
	logRecordCRC := logRecord.crc(headerBuffer[crc32.Size:headerSize])
	if logRecordCRC != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, logRecordSize, nil
}

func (df *DataFile) Sync() error {
	return df.IOHandler.Sync()
}

func (df *DataFile) Write(data []byte) error {
	n, err := df.IOHandler.Write(data)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(n)
	return nil
}

func (df *DataFile) Close() error {
	return df.IOHandler.Close()
}

// readNBytes Read n bytes from given offset in a data file.
func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IOHandler.Read(b, offset)
	return b, err
}

// WriteHintRecord writes a LogRecordPosition to a hint file
func WriteHintRecord(hintFile *DataFile, key []byte, lrp *LogRecordPosition) error {
	lr := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPosition(lrp),
	}
	elr, _ := EncodeLogRecord(lr)
	return hintFile.Write(elr)
}
