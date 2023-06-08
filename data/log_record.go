package data

import "encoding/binary"

// LogRecordPosition: index of in-memory data
type LogRecordPosition struct {
	FileID uint32 // ID of the file where data is stored in
	Offset int64  // Offset of data in the file
}

type LogRecordType = byte

const (
	NormalLogRecord  LogRecordType = iota // NormalLogRecord indicates that a log record is normal to read, update or delete
	DeletedLogRecord                      // DeletedLogRecord indicates that a log record is deleted and unusable
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
	return nil, 0
}

// decodeLogRecordHeader Decode a header of a log record
func decodeLogRecordHeader(buffer []byte) (*logRecordHeader, int64) {
	return nil, 0
}

// crc Get the CRC value of a log record
func (lr *LogRecord) crc(header []byte) uint32 {
	return 0
}
