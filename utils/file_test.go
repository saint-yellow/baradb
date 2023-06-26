package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	dir := "/tmp/go"
	size, err := DirSize(dir)
	assert.Nil(t, err)
	assert.True(t, size >= 0)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	assert.True(t, size >= 0)
}
