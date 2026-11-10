package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, "test.txt"), []byte("hello"), 0644)
	assert.Nil(t, err)

	size, err := DirSize(dir)
	assert.Nil(t, err)
	assert.Equal(t, int64(5), size)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	assert.True(t, size >= 0)
}
