package io_handler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	filePath = filepath.Join("/tmp", "baradb-114514.data")
)

func TestNewFileIO(t *testing.T) {
	fio, err := NewFileIO(filePath)
	defer os.Remove(filePath)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	fio, _ := NewFileIO(filePath)
	defer os.Remove(filePath)

	res, err := fio.Write(nil)
	assert.Nil(t, err)
	assert.True(t, res == 0)

	res, err = fio.Write([]byte(""))
	assert.Nil(t, err)
	assert.True(t, res == 0)

	res, err = fio.Write(make([]byte, 0))
	assert.Nil(t, err)
	assert.True(t, res == 0)

	res, err = fio.Write([]byte("114514"))
	assert.Nil(t, err)
	assert.Equal(t, 6, res)
}

func TestFileIO_Read(t *testing.T) {
	fio, _ := NewFileIO(filePath)
	defer os.Remove(filePath)

	fio.Write([]byte("114514"))
	fio.Write([]byte("1919810"))

	b1 := make([]byte, 6)
	res, err := fio.Read(b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, 6, res)
	assert.Equal(t, []byte("114514"), b1)

	b2 := make([]byte, 7)
	res, err = fio.Read(b2, 6)
	assert.Nil(t, err)
	assert.Equal(t, 7, res)
	assert.Equal(t, []byte("1919810"), b2)
}

func TestFileIO_Sync(t *testing.T) {
	fio, _ := NewFileIO(filePath)
	defer os.Remove(filePath)

	fio.Write([]byte("114514"))
	fio.Write([]byte("1919810"))

	err := fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	fio, _ := NewFileIO(filePath)
	defer os.Remove(filePath)

	fio.Write([]byte("114514"))
	fio.Write([]byte("1919810"))

	err := fio.Close()
	assert.Nil(t, err)
}
