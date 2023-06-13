package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	tempDir string = os.TempDir()
)

func TestOpenDataFile(t *testing.T) {
	file1, err := OpenDataFile(tempDir, 114)
	assert.Nil(t, err)
	assert.NotNil(t, file1)

	file2, err := OpenDataFile(tempDir, 514)
	assert.Nil(t, err)
	assert.NotNil(t, file2)

	file3, err := OpenDataFile(tempDir, 114)
	assert.Nil(t, err)
	assert.NotNil(t, file3)
}

func TestDataFile_Write(t *testing.T) {
	file, _ := OpenDataFile(tempDir, 114)
	err := file.Write([]byte("114"))
	assert.Nil(t, err)
	err = file.Write([]byte("winter flower"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	file1, _ := OpenDataFile(tempDir, 114)
	file1.Write([]byte("14530529"))
	err := file1.Close()
	assert.Nil(t, err)

	file2, _ := OpenDataFile(tempDir, 514)
	err = file2.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	file1, _ := OpenDataFile(tempDir, 114)
	err := file1.Sync()
	assert.Nil(t, err)

	file2, _ := OpenDataFile(tempDir, 514)
	file2.Write([]byte("1145141919810"))
	file2.Write([]byte("1145141919810"))
	err = file2.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	// Suppose to open a brand new data file with a log record
	file, _ := OpenDataFile(tempDir, 1919)

	// Test the first log record
	lr1 := &LogRecord{
		Key:   []byte("1919"),
		Value: []byte("810"),
		Type:  NormalLogRecord,
	}

	b1, ws1 := EncodeLogRecord(lr1)
	file.Write(b1)

	res1, size1, err1 := file.ReadLogRecord(0)
	assert.Nil(t, err1)
	assert.Equal(t, ws1, size1)
	assert.Equal(t, lr1, res1)

	// Append a new log record and test it
	lr2 := &LogRecord{
		Key:   []byte("114"),
		Value: []byte("514"),
		Type:  NormalLogRecord,
	}

	b2, ws2 := EncodeLogRecord(lr2)
	file.Write(b2)

	// Read the log record from the end of the first one
	res2, size2, err2 := file.ReadLogRecord(size1)
	assert.Nil(t, err2)
	assert.Equal(t, ws2, size2)
	assert.Equal(t, lr2, res2)

	// Append a new log record again and test it
	lr3 := &LogRecord{
		Key:   []byte("114"),
		Value: []byte("514"),
		Type:  DeletedLogRecord,
	}

	b3, ws3 := EncodeLogRecord(lr3)
	file.Write(b3)

	// Read the log record from the end of the second one
	res3, size3, err3 := file.ReadLogRecord(size1 + size2)
	assert.Nil(t, err3)
	assert.Equal(t, ws3, size3)
	assert.Equal(t, lr3, res3)
}
