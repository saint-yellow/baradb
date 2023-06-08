package baradb

import "github.com/saint-yellow/baradb/index"

// Options represents options of a DB engine instance
type Options struct {
	Directory       string            // Directory of a DB engine instance where data files are stored in
	MaxDataFileSize int64             // Maximum size of a every single data file
	WriteSync       bool              // Indicates whether persist data after writing
	IndexerType     index.IndexerType // Chooses an indexer (BTree/ART/...)
}

func checkOptions(options Options) error {
	if options.Directory == "" {
		return ErrDirectoryIsEmpty
	}

	if options.MaxDataFileSize <= 0 {
		return ErrMaxDataFileSizeIsNegative
	}

	return nil
}
