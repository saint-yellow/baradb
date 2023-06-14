package baradb

import "github.com/saint-yellow/baradb/indexer"

// Options represents options of a DB engine instance
type Options struct {
	Directory       string              // Directory of a DB engine instance where data files are stored in
	MaxDataFileSize int64               // Maximum size of a every single data file (uint: Byte)
	WriteSync       bool                // Indicates whether persist data after writing
	IndexerType     indexer.IndexerType // Chooses an indexer (BTree/ART/...)
}

// checkOptions
func checkOptions(options Options) error {
	if options.Directory == "" {
		return ErrDirectoryIsEmpty
	}

	if options.MaxDataFileSize <= 0 {
		return ErrMaxDataFileSizeIsNegative
	}

	return nil
}

// DefaultOptions Default options for launching DB engine
var DefaultOptions = Options{
	Directory:       "/tmp/baradb",
	MaxDataFileSize: 1 * 1024 * 1024,
	WriteSync:       false,
	IndexerType:     indexer.Btree,
}
