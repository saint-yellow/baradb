package utils

import (
	"io/fs"
	"path/filepath"
	"syscall"
)

// DirSize returns the total size (unit: B) of all files in a given directory.
func DirSize(d string) (int64, error) {
	var size int64
	err := filepath.Walk(d, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvailableDiskSize returns available size of the disk of the machine where the DB hosts in
func AvailableDiskSize() (int64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}

	var stat syscall.Statfs_t
	err = syscall.Statfs(wd, &stat)
	if err != nil {
		return 0, nil
	}

	size := int64(stat.Bavail) * stat.Bsize
	return size, nil
}
