package redis

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/saint-yellow/baradb"
)

const (
	maxMetadataSize       = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetadataSize = binary.MaxVarintLen64 * 2
	initialListMark       = math.MaxUint64 / 2
)

// metadata
type metadata struct {
	dataType dataType
	expire   int64
	version  int64
	size     uint32
	head     uint64
	tail     uint64
}

// encodeMetadata encodes a metadata to a byte array
func encodeMetadata(md *metadata) []byte {
	size := maxMetadataSize
	if md.dataType == List {
		size += extraListMetadataSize
	}

	buffer := make([]byte, size)
	buffer[0] = md.dataType
	index := 1
	index += binary.PutVarint(buffer[index:], md.expire)
	index += binary.PutVarint(buffer[index:], md.version)
	index += binary.PutVarint(buffer[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buffer[index:], md.head)
		index += binary.PutUvarint(buffer[index:], md.tail)
	}

	return buffer[:index]
}

// decodeMetadata decodes a byte array to a metadata
func decodeMetadata(buffer []byte) *metadata {
	dataType := buffer[0]
	index := 1
	expire, n := binary.Varint(buffer[index:])
	index += n
	version, n := binary.Varint(buffer[index:])
	index += n
	size, n := binary.Varint(buffer[index:])
	index += n

	var head uint64
	var tail uint64
	if dataType == List {
		head, n = binary.Uvarint(buffer[index:])
		index += n
		tail, _ = binary.Uvarint(buffer[index:])
	}

	md := &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
	return md
}

// getMetadata
func (ds *DS) getMetadata(key []byte, dt dataType) (*metadata, error) {
	metaBuf, err := ds.db.Get(key)
	if err != nil && err != baradb.ErrKeyNotFound {
		return nil, err
	}

	var md *metadata
	exist := true
	if err == baradb.ErrKeyNotFound {
		exist = false
	} else {
		md = decodeMetadata(metaBuf)
		if md.dataType != dt {
			return nil, ErrWrongTypeOperation
		}
		if md.expire != 0 && md.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		md = &metadata{
			dataType: dt,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dt == List {
			md.head = initialListMark
			md.tail = initialListMark
		}
	}

	return md, nil
}
