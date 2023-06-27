package data

import "errors"

var ErrInvalidCRC = errors.New("invalid CRC value. Maybe the log record was corrupted")
