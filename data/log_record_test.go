package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/saint-yellow/baradb/utils"
)

// Samples: 1. Normal; 2. Nil value; 3. Deleted
var samples = [3]*LogRecord{
	{
		Key:   []byte("114"),
		Value: []byte("514"),
		Type:  NormalLogRecord,
	},
	{
		Key:   []byte("191810"),
		Value: nil,
		Type:  NormalLogRecord,
	},
	{
		Key:   []byte("114514"),
		Value: []byte("1919810"),
		Type:  DeletedLogRecord,
	},
}

func TestEncodingLogRecord(t *testing.T) {
	for _, lr := range samples {
		b, n := EncodeLogRecord(lr)
		assert.NotNil(t, b)
		assert.Greater(t, n, int64(5))
	}
}

func TestEncodingLogRecordHeader(t *testing.T) {
	for _, lr := range samples {
		b1, _ := encodeLogRecordHeader(lr)
		assert.Equal(t, uint8(lr.Type), b1[len(b1)-3])
		b2, _ := EncodeLogRecord(lr)
		assert.EqualValues(t, b1, b2[:len(b1)])
	}
}

func TestDecodingLogRecordHeader(t *testing.T) {
	for _, lr := range samples {
		b, _ := encodeLogRecordHeader(lr)
		lrh, _ := decodeLogRecordHeader(b)
		assert.Equal(t, lrh.logRecordType, lr.Type)
		assert.Equal(t, int(lrh.keySize), len(lr.Key))
		assert.Equal(t, int(lrh.valueSize), len(lr.Value))
	}
}

func TestLogRecordCRC(t *testing.T) {
	for _, lr := range samples {
		b, _ := encodeLogRecordHeader(lr)
		crcValue := lr.crc(b[crc32.Size:])
		h, _ := decodeLogRecordHeader(b)
		assert.Equal(t, crcValue, h.crc)
	}
}

func TestEncodingKeyWithTranNo(t *testing.T) {
	originalKey := utils.NewKey(8)
	var tranNo uint64 = 114514

	encodedKey := EncodeKey(originalKey, tranNo)
	assert.NotEqualValues(t, originalKey, encodedKey)

	decodedKey, no := DecodeKey(encodedKey)
	assert.EqualValues(t, originalKey, decodedKey)
	assert.Equal(t, tranNo, no)
}
