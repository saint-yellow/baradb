package baradb

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/saint-yellow/baradb/data"
)

const (
	mergenceFolderSuffix = "-merge"
	mergedLogRecordKey   = "merged"
)

// Merge clears invalid data files and generates hint files
func (db *DB) Merge() error {
	// The DB has no any data file
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()

	// The DB can only do mergence once in the same time
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergenceIsInProgress
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// Sync the current active data file
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	// The current active data file will be inactive
	nonMergedFileID := db.activeFile.FileID
	db.inactiveFiles[db.activeFile.FileID] = db.activeFile

	// Generate a new active data file
	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	// All the inactive data file are files to be merged
	var filesToBeMerged []*data.DataFile
	for _, file := range db.inactiveFiles {
		filesToBeMerged = append(filesToBeMerged, file)
	}

	db.mu.Unlock()

	// Sort the files to be merged by their FileIDs
	sort.Slice(filesToBeMerged, func(i, j int) bool {
		return filesToBeMerged[i].FileID < filesToBeMerged[j].FileID
	})

	md := db.getMergenceDiretory()
	// Try to remove the exist directory of the previous mergence
	if _, err := os.Stat(md); err == nil {
		if err := os.RemoveAll(md); err != nil {
			return err
		}
	}
	// New a directory of the current mergence
	if err := os.MkdirAll(md, os.ModePerm); err != nil {
		return err
	}

	// Launch a new DB engine for mergence
	mergenceOptions := db.options
	mergenceOptions.Directory = md
	mergenceOptions.SyncWrites = false
	tempDB, err := LaunchDB(mergenceOptions)
	if err != nil {
		return err
	}
	defer tempDB.Close()

	// Open a hint file
	hintFile, err := data.OpenHintFile(md)
	if err != nil {
		return err
	}

	// Handle every file to be merged
	for _, file := range filesToBeMerged {
		var offset int64 = 0
		for {
			lr, n, err := file.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			lrKey, _ := decodeLogRecordKeyWithTranNo(lr.Key)
			lrp := db.indexer.Get(lrKey)
			if lrp != nil && lrp.FileID == file.FileID && lrp.Offset == offset {
				lr.Key = encodeLogRecordKeyWithTranNo(lrKey, nonTranNo)
				mlrp, err := tempDB.appendLogRecord(lr, false)
				if err != nil {
					return err
				}

				// Write the current position index to the hint file
				if err := data.WriteHintRecord(hintFile, lrKey, mlrp); err != nil {
					return err
				}

			}

			offset += n
		}
	}

	// Sync the hint file and the mergence DB
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := tempDB.Sync(); err != nil {
		return err
	}

	mergedFile, err := data.OpenMergedFile(md)
	if err != nil {
		return err
	}
	mlr := &data.LogRecord{
		Key:   []byte(mergedLogRecordKey),
		Value: []byte(strconv.Itoa(int(nonMergedFileID))),
	}
	emlr, _ := data.EncodeLogRecord(mlr)
	if err := mergedFile.Write(emlr); err != nil {
		return err
	}
	if err := mergedFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) getMergenceDiretory() string {
	parentDirectory := path.Dir(db.options.Directory)
	basename := path.Base(db.options.Directory)
	return filepath.Join(parentDirectory, basename+mergenceFolderSuffix)
}

func (db *DB) loadMergenceFiles() error {
	md := db.getMergenceDiretory()
	if _, err := os.Stat(md); os.IsNotExist(err) {
		return nil
	}

	entries, err := os.ReadDir(md)
	if err != nil {
		return err
	}

	defer func() {
		os.RemoveAll(md)
	}()

	var mergenceFinished bool
	mergedFileNames := make([]string, len(entries))
	for i, entry := range entries {
		if entry.Name() == data.MergedFileName {
			mergenceFinished = true
		}
		if entry.Name() == data.TranNoFileName {
			continue
		}
		mergedFileNames[i] = entry.Name()
	}

	if !mergenceFinished {
		return nil
	}

	nonMergedFileID, err := db.getNonMergedFileID(md)
	if err != nil {
		return err
	}

	var fileID uint32 = 0
	for ; fileID < nonMergedFileID; fileID++ {
		filePath := data.GetDataFilePath(db.options.Directory, fileID)
		if _, err := os.Stat(filePath); err == nil {
			if err := os.Remove(filePath); err != nil {
				return err
			}
		}
	}

	for _, fileName := range mergedFileNames {
		srcPath := filepath.Join(md, fileName)
		dstPath := filepath.Join(db.options.Directory, fileName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}

	return nil
}

// getNonMergedFileID gets the ID of the data file that is not merged
func (db *DB) getNonMergedFileID(directory string) (uint32, error) {
	mergedFile, err := data.OpenMergedFile(directory)
	if err != nil {
		return 0, err
	}

	lr, _, err := mergedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	fileID, err := strconv.Atoi(string(lr.Value))
	if err != nil {
		return 0, err
	}

	return uint32(fileID), nil
}
