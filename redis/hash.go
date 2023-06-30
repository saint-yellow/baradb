package redis

import (
	"encoding/binary"

	"github.com/saint-yellow/baradb"
)

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buffer := make([]byte, len(hk.key)+8+len(hk.field))

	// key
	index := 0
	copy(buffer[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(hk.version))
	index += 8

	// field
	copy(buffer[index:], hk.field)

	return buffer
}

// HSet redis HSET
func (ds *DS) HSet(key, field, value []byte) (bool, error) {
	md, err := ds.getMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	hk := &hashInternalKey{
		key:     key,
		version: md.version,
		field:   field,
	}
	encKey := hk.encode()

	exist := true
	if _, err := ds.db.Get(encKey); err == baradb.ErrKeyNotFound {
		exist = false
	}

	wb := ds.db.NewWriteBatch(baradb.DefaultWriteBatchOptions)
	if !exist {
		md.size++
		wb.Put(key, encodeMetadata(md))
	}
	wb.Put(encKey, value)
	if err := wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

// HGet redis HGET
func (ds *DS) HGet(key, field []byte) ([]byte, error) {
	md, err := ds.getMetadata(key, Hash)
	if err != nil {
		return nil, nil
	}

	if md.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: md.version,
		field:   field,
	}
	encKey := hk.encode()

	return ds.db.Get(encKey)
}

// HDel redis HDEL
//
// When the returned error is nil,
// if the given key exists, then it returns true and false otherwise.
func (ds *DS) HDel(key, field []byte) (bool, error) {
	md, err := ds.getMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	if md.size == 0 {
		return false, err
	}

	hk := &hashInternalKey{
		key:     key,
		version: md.version,
		field:   field,
	}
	encKey := hk.encode()

	exist := true
	_, err = ds.db.Get(encKey)
	if err == baradb.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := ds.db.NewWriteBatch(baradb.DefaultWriteBatchOptions)
		md.size--
		encMd := encodeMetadata(md)
		wb.Put(key, encMd)
		wb.Delete(encKey)

		err = wb.Commit()
		if err != nil {
			return false, err
		}
	}

	return exist, nil
}
