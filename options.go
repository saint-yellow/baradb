package baradb

// Options represents options of a DB engine instance
type Options struct {
	Directory       string // Directory of a DB engine instance where data files are stored in
	MaxDataFileSize int64  // Maximum size of a every single data file
	WriteSync       bool   // Indicates whether persist data after writing
}
