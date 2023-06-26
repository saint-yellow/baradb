package data

import (
	"encoding/binary"
	"hash/crc32"
)

// LogRecordPosition represents a position of a LogRecord
// A position of a LogRecord stores in an in-memory indexer
type LogRecordPosition struct {
	// ID of the data file where the log record is stored in
	FileID uint32

	// Offset of the log record in the data file
	Offset int64

	// Size of the log record (uint: Byte)
	Size uint32
}

// LogRecordType types of log records
type LogRecordType = byte

const (
	NormalLogRecord              LogRecordType = iota + 1 // NormalLogRecord indicates that a log record is normal to read, update or delete
	DeletedLogRecord                                      // DeletedLogRecord indicates that a log record is deleted and unusable
	TransactionFinishedLogRecord                          // TransactionFinishedLogRecord indicates that a Transaction is finished
)

// maxLogRecordHeaderSize Maximum size of a header of a log record
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord represents a log record in a data file
type LogRecord struct {
	Key   []byte        // Key
	Value []byte        // Value
	Type  LogRecordType // Type indicates whether a log record is unusable (deleted) or not
}

// logRecordHeader A header information of a log record
type logRecordHeader struct {
	crc           uint32        // CRC (Cyclic Redundancy Check) value
	keySize       uint32        // Size of the key of the corresponding log record
	valueSize     uint32        // Size of the value of the corresponding log record
	logRecordType LogRecordType // Type of the corresponding log record (normal/deleted/...)
}

// EncodeLogRecord encodes a log record
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	// Initialize a byte array of the header
	header := make([]byte, maxLogRecordHeaderSize)

	// Store the type of the log record to the header
	header[4] = lr.Type

	var index int = 5
	// Store the size of the key and the value of the log record to the header
	index += binary.PutVarint(header[index:], int64(len(lr.Key)))
	index += binary.PutVarint(header[index:], int64(len(lr.Value)))

	// Total size of the header and the log reocrd
	var size int = index + len(lr.Key) + len(lr.Value)

	// Copy the known data to the new byte array
	encodedBytes := make([]byte, size)
	copy(encodedBytes[:index], header[:index])
	copy(encodedBytes[index:], lr.Key)
	copy(encodedBytes[index+len(lr.Key):], lr.Value)

	// Do CRC for the header and the log record
	crc := crc32.ChecksumIEEE(encodedBytes[4:])
	binary.LittleEndian.PutUint32(encodedBytes[:4], crc)

	return encodedBytes, int64(size)
}

// encodeLogRecordHeader returns the encodeed log record header and its length
func encodeLogRecordHeader(lr *LogRecord) ([]byte, int64) {
	// Initialize a byte array of the header
	header := make([]byte, maxLogRecordHeaderSize)

	// Store the type of the log record to the header
	header[4] = lr.Type

	var index int = 5
	// Store the size of the key and the value of the log record to the header
	index += binary.PutVarint(header[index:], int64(len(lr.Key)))
	index += binary.PutVarint(header[index:], int64(len(lr.Value)))

	// Total size of the header and the log reocrd
	var size int = index + len(lr.Key) + len(lr.Value)

	// Copy the known data to the new byte array
	encodedBytes := make([]byte, size)
	copy(encodedBytes[:index], header[:index])
	copy(encodedBytes[index:], lr.Key)
	copy(encodedBytes[index+len(lr.Key):], lr.Value)

	// Do CRC for the header and the log record
	crc := crc32.ChecksumIEEE(encodedBytes[4:])
	binary.LittleEndian.PutUint32(encodedBytes[:4], crc)

	return encodedBytes[:index], int64(index)
}

// decodeLogRecordHeader Decode a header of a log record
func decodeLogRecordHeader(buffer []byte) (*logRecordHeader, int64) {
	if len(buffer) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:           binary.LittleEndian.Uint32(buffer[:4]),
		logRecordType: buffer[4],
	}

	var index int = 5

	// Get the actual size of the key of the log reocrd
	keySize, n := binary.Varint(buffer[index:])
	header.keySize = uint32(keySize)
	index += n

	// Get the actual size of the value of the log record
	valueSize, n := binary.Varint(buffer[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// crc Get the CRC value of a log record
func (lr *LogRecord) crc(header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}

// EncodeLogRecordPosition encodes a LogRecordPosition into a byte array
func EncodeLogRecordPosition(lrp *LogRecordPosition) []byte {
	buffer := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index int = 0
	index += binary.PutVarint(buffer[index:], int64(lrp.FileID))
	index += binary.PutVarint(buffer[index:], lrp.Offset)
	index += binary.PutVarint(buffer[index:], int64(lrp.Size))
	return buffer[:index]
}

// DecodeLogRecordPosition decodes a byte array into a DecodeLogRecordPosition
func DecodeLogRecordPosition(buffer []byte) *LogRecordPosition {
	var index int = 0
	fileID, n := binary.Varint(buffer[index:])
	index += n
	offset, n := binary.Varint(buffer[index:])
	index += n
	size, n := binary.Varint(buffer[index:])
	lrp := &LogRecordPosition{
		FileID: uint32(fileID),
		Offset: offset,
		Size:   uint32(size),
	}
	return lrp
}
