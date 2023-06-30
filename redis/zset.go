package redis

import (
	"encoding/binary"

	"github.com/saint-yellow/baradb"
	"github.com/saint-yellow/baradb/utils"
)

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buffer := make([]byte, len(zk.key)+len(zk.member)+8)

	index := 0

	// key
	copy(buffer[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(zk.version))
	index += 8

	// member
	copy(buffer[index:], zk.member)

	return buffer
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuffer := utils.Float64ToBytes(zk.score)
	buffer := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuffer)+8+4)

	index := 0

	// key
	copy(buffer[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(zk.version))
	index += 8

	// score
	copy(buffer[index:index+len(scoreBuffer)], scoreBuffer)
	index += len(scoreBuffer)

	// member
	copy(buffer[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// size of member
	binary.LittleEndian.PutUint32(buffer[index:], uint32(len(zk.member)))

	return buffer
}

func (ds *DS) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	md, err := ds.getMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	zk := &zsetInternalKey{
		key:     key,
		version: md.version,
		member:  member,
		score:   score,
	}
	encKey := zk.encodeWithMember()

	exist := true
	buffer, err := ds.db.Get(encKey)
	if err != nil {
		if err == baradb.ErrKeyNotFound {
			exist = false
		} else {
			return false, err
		}
	}
	if exist {
		if score == utils.Float64FromBytes(buffer) {
			return false, nil
		}
	}

	wb := ds.db.NewWriteBatch(baradb.DefaultWriteBatchOptions)
	if !exist {
		md.size++
		wb.Put(key, encodeMetadata(md))
	} else {
		oldKey := &zsetInternalKey{
			key:     key,
			version: md.version,
			member:  member,
			score:   utils.Float64FromBytes(buffer),
		}
		wb.Delete(oldKey.encodeWithScore())
	}
	wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (ds *DS) ZScore(key, member []byte) (float64, error) {
	md, err := ds.getMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if md.size == 0 {
		return -1, nil
	}

	zk := &zsetInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}
	encKey := zk.encodeWithMember()

	buffer, err := ds.db.Get(encKey)
	if err != nil {
		return -1, err
	}

	value := utils.Float64FromBytes(buffer)
	return value, nil
}
