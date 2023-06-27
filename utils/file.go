package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// DirSize returns the total size (unit: B) of all files in a given directory.
func DirSize(d string) (int64, error) {
	var size int64
	err := filepath.Walk(d, func(_ string, info fs.FileInfo, err error) error {
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

// CopyDir copy the data files in a given source directory to a given destination directory
//
// Files excluded will be ignored.
func CopyDir(src, dst string, exclude []string) error {
	// Make sure the destination directory is exist
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		if err := os.MkdirAll(dst, os.ModePerm); err != nil {
			return err
		}
	}

	fn := func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}

		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}

		return os.WriteFile(filepath.Join(dst, fileName), data, info.Mode())
	}
	return filepath.Walk(src, fn)
}
