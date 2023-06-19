package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewKey(t *testing.T) {
	key := NewKey(1)
	assert.Equal(t, "baradb-key-000000001", string(key))
}

func TestNewRandomValue(t *testing.T) {
	value := NewRandomValue(3)
	assert.Equal(t, len("baradb-value-")+3, len(string(value)))
}
