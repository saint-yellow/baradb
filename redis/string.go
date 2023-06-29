package redis

import (
	"encoding/binary"
	"time"
)

// Set redis SET
func (ds *DS) Set(key []byte, value []byte, ttl time.Duration) error {
	if len(value) == 0 {
		return nil
	}

	// Encode value: type + expire + payload
	buffer := make([]byte, binary.MaxVarintLen64+1)
	buffer[0] = String
	index := 1

	// If the ttl is 0, then the key will not expire.
	var expire int64
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}

	index += binary.PutVarint(buffer[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buffer[:index])
	copy(encValue[index:], value)

	// Put the key and the encoded value to the DB engine
	return ds.db.Put(key, encValue)
}

// Get redis GET
func (ds *DS) Get(key []byte) ([]byte, error) {
	encValue, err := ds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// Decode the encoded value
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	index := 1
	expire, n := binary.Varint(encValue[index:])
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, ErrExpiredValue
	}
	index += n
	payload := encValue[index:]
	return payload, nil
}
