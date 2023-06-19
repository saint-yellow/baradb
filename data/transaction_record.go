package data

// TransactionRecord
type TransactionRecord struct {
	Log      *LogRecord         // a log record
	Position *LogRecordPosition // position of the log record
}
