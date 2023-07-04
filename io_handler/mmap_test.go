package io_handler

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryMappedIO_New(t *testing.T) {
	mmap, err := newMemoryMappedIO(filePath)
	defer destroyFile()

	assert.Nil(t, err)
	assert.NotNil(t, mmap)
}

func TestMemoryMappedIO_Read(t *testing.T) {
	mmap, _ := newMemoryMappedIO(filePath)
	defer destroyFile()

	b1 := make([]byte, 6)
	n, err := mmap.Read(b1, 0)
	assert.Zero(t, n)
	assert.Equal(t, io.EOF, err)

	fileIO, _ := newFileIO(filePath)
	fileIO.Write([]byte("114514"))
	fileIO.Write([]byte("1919810"))

	mmap, _ = newMemoryMappedIO(filePath)
	n, err = mmap.Read(b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, len(b1), n)
	assert.Equal(t, "114514", string(b1))

	b2 := make([]byte, 7)
	n, err = mmap.Read(b2, 6)
	assert.Nil(t, err)
	assert.Equal(t, len(b2), n)
	assert.Equal(t, "1919810", string(b2))
}

func TestMemoryMappedIO_Close(t *testing.T) {
	mmap, _ := newMemoryMappedIO(filePath)
	defer destroyFile()

	err := mmap.Close()
	assert.Nil(t, err)

	fileIO, _ := newFileIO(filePath)
	mmap, _ = newMemoryMappedIO(filePath)
	err = mmap.Close()
	assert.Nil(t, err)

	fileIO.Write([]byte("114514"))
	mmap, _ = newMemoryMappedIO(filePath)
	err = mmap.Close()
	assert.Nil(t, err)

	fileIO.Sync()
	mmap, _ = newMemoryMappedIO(filePath)
	err = mmap.Close()
	assert.Nil(t, err)

	fileIO.Close()
	mmap, _ = newMemoryMappedIO(filePath)
	err = mmap.Close()
	assert.Nil(t, err)
}

func TestMemoryMappedIO_Size(t *testing.T) {
	mmap, _ := newMemoryMappedIO(filePath)
	fileIO, _ := newFileIO(filePath)
	defer destroyFile()

	size, err := mmap.Size()
	assert.Nil(t, err)
	assert.Zero(t, size)

	b1 := []byte("114514")
	b2 := []byte("1919810")

	fileIO.Write(b1)
	mmap, _ = newMemoryMappedIO(filePath)
	size, err = mmap.Size()
	assert.Nil(t, err)
	assert.Equal(t, len(b1), int(size))

	fileIO.Write(b2)
	mmap, _ = newMemoryMappedIO(filePath)
	size, err = mmap.Size()
	assert.Nil(t, err)
	assert.Equal(t, len(b1)+len(b2), int(size))

	fileIO.Sync()
	size, err = mmap.Size()
	assert.Nil(t, err)
	assert.Equal(t, len(b1)+len(b2), int(size))

	fileIO.Close()
	size, err = mmap.Size()
	assert.Nil(t, err)
	assert.Equal(t, len(b1)+len(b2), int(size))
}
