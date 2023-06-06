package data

// LogRecordPosition: index of in-memory data
type LogRecordPosition struct {
	FileID uint32 // ID of the file where data is stored in
	Offset int64  // offset of data in the file
}
