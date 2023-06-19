package baradb

import (
	"testing"

	"github.com/saint-yellow/baradb/utils"
	"github.com/stretchr/testify/assert"
)

func TestEncodingKeyWithTranNo(t *testing.T) {
	originalKey := utils.NewKey(8)
	var tranNo uint64 = 114514

	encodedKey := encodeLogRecordKeyWithTranNo(originalKey, tranNo)
	assert.NotEqualValues(t, originalKey, encodedKey)

	decodedKey, no := decodeLogRecordKeyWithTranNo(encodedKey)
	assert.EqualValues(t, originalKey, decodedKey)
	assert.Equal(t, tranNo, no)
}
