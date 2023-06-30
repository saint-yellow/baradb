package redis

import (
	"encoding/binary"

	"github.com/saint-yellow/baradb"
)

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buffer := make([]byte, len(lk.key)+8+8)

	// key
	index := 0
	copy(buffer[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(lk.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buffer[index:], lk.index)

	return buffer
}

func (ds *DS) listPush(key, element []byte, isLeft bool) (uint32, error) {
	md, err := ds.getMetadata(key, List)
	if err != nil {
		return 0, err
	}

	lk := &listInternalKey{
		key:     key,
		version: md.version,
	}
	if isLeft {
		lk.index = md.head - 1
	} else {
		lk.index = md.tail
	}
	encKey := lk.encode()

	wb := ds.db.NewWriteBatch(baradb.DefaultWriteBatchOptions)
	md.size++
	if isLeft {
		md.head--
	} else {
		md.tail++
	}
	wb.Put(key, encodeMetadata(md))
	wb.Put(encKey, element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return md.size, nil
}

// LPush Redis LPUSH
func (ds *DS) LPush(key, element []byte) (uint32, error) {
	return ds.listPush(key, element, true)
}

// RPush Redis RPUSH
func (ds *DS) RPush(key, element []byte) (uint32, error) {
	return ds.listPush(key, element, false)
}

func (ds *DS) listPop(key []byte, isLeft bool) ([]byte, error) {
	md, err := ds.getMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if md.size == 0 {
		return nil, nil
	}

	lk := &listInternalKey{
		key:     key,
		version: md.version,
	}
	if isLeft {
		lk.index = md.head
	} else {
		lk.index = md.tail - 1
	}
	encKey := lk.encode()

	element, err := ds.db.Get(encKey)
	if err != nil {
		return nil, err
	}

	md.size--
	if isLeft {
		md.head++
	} else {
		md.tail--
	}
	if err = ds.db.Put(key, encodeMetadata(md)); err != nil {
		return nil, err
	}

	return element, nil
}

// LPop Redis LPOP
func (ds *DS) LPop(key []byte) ([]byte, error) {
	return ds.listPop(key, true)
}

// RPop Redis RPOP
func (ds *DS) RPop(key []byte) ([]byte, error) {
	return ds.listPop(key, false)
}

// LLen Redis LLEN
func (ds *DS) LLen(key []byte) (uint32, error) {
	md, err := ds.getMetadata(key, List)
	if err != nil {
		return 0, err
	}
	return md.size, nil
}
