package baradb

import "github.com/saint-yellow/baradb/indexer"

// Options represents options of a DB engine instance
type DBOptions struct {
	Directory       string              // Directory of a DB engine instance where data files are stored in
	MaxDataFileSize int64               // Maximum size of a every single data file (uint: Byte)
	SyncWrites      bool                // Indicates whether persist data after writing
	IndexerType     indexer.IndexerType // Chooses an indexer (BTree/ART/...)
}

// checkOptions
func checkDBOptions(options DBOptions) error {
	if options.Directory == "" {
		return ErrDirectoryIsEmpty
	}

	if options.MaxDataFileSize <= 0 {
		return ErrMaxDataFileSizeIsNegative
	}

	return nil
}

// WriteBatchOptions options for batch writing
type WriteBatchOptions struct {
	MaxBatchNumber int  // Maximum amount of data in one batch
	SyncWrites     bool // Sync data after writing if true
}

var (
	// DefaultDBOptions Default options for launching DB engine
	DefaultDBOptions = DBOptions{
		Directory:       "/tmp/baradb",
		MaxDataFileSize: 1 * 1024 * 1024,
		SyncWrites:      false,
		IndexerType:     indexer.BPtree,
	}
	// DefaultWriteBatchOptions Default options for batch writing
	DefaultWriteBatchOptions = WriteBatchOptions{
		MaxBatchNumber: 100,
		SyncWrites:     true,
	}
)
