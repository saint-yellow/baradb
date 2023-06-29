package redis

import (
	"encoding/binary"

	"github.com/saint-yellow/baradb"
)

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

// encodeSetInternalKey encodes a set internal key to a byte array
func encodeSetInternalKey(sk *setInternalKey) []byte {
	buffer := make([]byte, len(sk.key)+8+len(sk.member)+4)

	// key
	index := 0
	copy(buffer[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(sk.version))
	index += 8

	// member
	copy(buffer[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size
	binary.LittleEndian.PutUint32(buffer[index:], uint32(len(sk.member)))

	return buffer
}

// SAdd redis SADD
func (ds *DS) SAdd(key []byte, member []byte) (bool, error) {
	md, err := ds.getMetadata(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}
	encKey := encodeSetInternalKey(sk)

	var ok bool
	_, err = ds.db.Get(encKey)
	if err == baradb.ErrKeyNotFound {
		wb := ds.db.NewWriteBatch(baradb.DefaultWriteBatchOptions)
		md.size++
		wb.Put(key, encodeMetadata(md))
		wb.Put(encKey, nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

// SIsMember redis SISMEMBER
func (ds *DS) SIsMember(key, member []byte) (bool, error) {
	md, err := ds.getMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if md.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}
	encKey := encodeSetInternalKey(sk)

	_, err = ds.db.Get(encKey)
	if err != nil {
		if err != baradb.ErrKeyNotFound {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

// TODO: SMembers redis SMEMBERS
func (ds *DS) SMembers(key []byte) ([]byte, error) {
	panic(ErrUnsupportedOperation)
}

// SRem redis SREM
func (ds *DS) SRem(key, member []byte) (bool, error) {
	md, err := ds.getMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if md.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}
	encKey := encodeSetInternalKey(sk)

	_, err = ds.db.Get(encKey)
	if err == baradb.ErrKeyNotFound {
		return false, nil
	}

	wb := ds.db.NewWriteBatch(baradb.DefaultWriteBatchOptions)
	md.size--
	wb.Put(key, encodeMetadata(md))
	wb.Delete(encKey)
	err = wb.Commit()
	if err != nil {
		return false, err
	}
	return true, nil
}
