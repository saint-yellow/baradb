package utils

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodingFloat64ToBytes(t *testing.T) {
	value := float64(math.E)
	buffer := Float64ToBytes(value)
	assert.NotNil(t, buffer)
}

func TestParsingBytesToFloat64(t *testing.T) {
	var buffer []byte
	var value float64

	// success
	buffer = Float64ToBytes(float64(math.Pi))
	value = Float64FromBytes(buffer)
	assert.Equal(t, float64(math.Pi), value)

	// failed
	buffer = []byte("114514homo1919810")
	value = Float64FromBytes(buffer)
	assert.Equal(t, float64(0), value)
}
