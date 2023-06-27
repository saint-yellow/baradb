package baradb

import "fmt"

// Stat represents statistical information of a DB engine
type Stat struct {
	KeyNumber       uint  `json:"keyNumber"`       // Number of key(s) in the DB engine
	DataFileNumber  uint  `json:"dataFileNumber"`  // Number of data file(s) in the DB engine
	ReclaimableSize int64 `json:"reclaimableSize"` // Amount of mergable data (unit: byte)
	DiskSize        int64 `json:"diskSize"`        // Size of the DB engine occuppied in disk (unit: byte)
}

func (s Stat) String() string {
	tmpl := "Key(s): %d; Data file(s): %d; Reclaimable size: %d B; Disk size: %d B"
	return fmt.Sprintf(
		tmpl,
		s.KeyNumber,
		s.DataFileNumber,
		s.ReclaimableSize,
		s.DiskSize,
	)
}
