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
	n, err := file.Write([]byte("114"))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)
	n, err = file.Write([]byte("winter flower"))
	assert.Nil(t, err)
	assert.Equal(t, 13, n)
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
