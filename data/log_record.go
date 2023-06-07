package data

// LogRecordPosition: index of in-memory data
type LogRecordPosition struct {
	FileID uint32 // ID of the file where data is stored in
	Offset int64  // Offset of data in the file
}

type LogRecordType = byte

const (
	NormalLogRecord  LogRecordType = iota
	DeletedLogRecord               // DeletedLogRecord indicates that a log record is deleted and unusable
)

// LogRecord represents a log record in a data file
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType // Type indicates whether a log record is unusable (deleted) or not
}

// EncodeLogRecord encodes a log record
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	return nil, 0
}
