package baradb

import "github.com/saint-yellow/baradb/indexer"

// Options represents options of a DB engine instance
type DBOptions struct {
	// Directory of a DB engine instance where data files are stored in
	Directory string

	// Maximum size of every single data file (uint: Byte).
	//
	// It should be greater than 0.
	MaxDataFileSize int64

	// SyncWrites indicates whether persist data after writing
	SyncWrites bool

	// IndexerType indicates the type of the indexer of the DB engine
	IndexerType indexer.IndexerType

	// SyncThreshold indicates a threshold for persisting data.
	//
	// It should equals 0 or be greater than 0 (uint: Byte).
	//
	// When the DB engine wrote bytes more than this threshold, the DB engine will sync the wirtten bytes to the acitve data file.
	SyncThreshold uint

	// MMapAtStartup indicates whether load memory mapping while starting up the DB engine
	MMapAtStartup bool

	// MergenceThreshold indicates a threshold for merging data.
	//
	// When proportion of the invalid data in the DB engine is greater than this threshold,
	// the DB engine will merge all its data.
	//
	// The value of this threshold should be between 0 and 1.
	//
	// If the value is 0, then this threshold is disabled and the DB engine will not merge its data.
	MergenceThreshold float64
}

// checkDBOptions return nil if all DB options are valid and a certain error otherwise.
func checkDBOptions(options DBOptions) error {
	if options.Directory == "" {
		return ErrDirectoryIsEmpty
	}

	if options.MaxDataFileSize <= 0 {
		return ErrMaxDataFileSizeIsNegative
	}

	if options.MergenceThreshold < 0 || options.MergenceThreshold > 1 {
		return ErrInvalidMergenceThreshold
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
		MaxDataFileSize: 512 * 1024 * 1024,
		SyncWrites:      false,
		IndexerType:     indexer.ARtree,
		MMapAtStartup:   false,
	}
	// DefaultWriteBatchOptions Default options for batch writing
	DefaultWriteBatchOptions = WriteBatchOptions{
		MaxBatchNumber: 100,
		SyncWrites:     true,
	}
)
