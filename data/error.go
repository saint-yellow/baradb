package data

import "errors"

var (
	ErrInvalidCRC = errors.New("Invalid CRC value. Maybe the log record was corrupted")
)
