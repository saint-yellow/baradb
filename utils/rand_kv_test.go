package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomKey(t *testing.T) {
	key := RandomKey(1)
	assert.Equal(t, "baradb-key-000000001", string(key))
}

func TestRandomValue(t *testing.T) {
	value := RandonValue(3)
	assert.Equal(t, len("baradb-value-")+3, len(string(value)))
}
