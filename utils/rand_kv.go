package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randSeed      = rand.New(rand.NewSource(time.Now().Unix()))
	letters       = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
	keyTemplate   = "baradb-key-%09d"
	valueTemplate = "baradb-value-%s"
)

// NewKey generates a random key for testing
func NewKey(n int) []byte {
	return []byte(fmt.Sprintf(keyTemplate, n))
}

// NewRandomValue generates a random value for testing
func NewRandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randSeed.Intn(len(letters))]
	}
	return []byte(fmt.Sprintf(valueTemplate, string(b)))
}
