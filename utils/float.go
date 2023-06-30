package utils

import "strconv"

// Float64FromBytes parses a floating number from the given byte array
func Float64FromBytes(buffer []byte) float64 {
	value, _ := strconv.ParseFloat(string(buffer), 64)
	return value
}

// Float64ToBytes encodes the given floating number to a byte array
func Float64ToBytes(value float64) []byte {
	buffer := []byte(strconv.FormatFloat(value, 'f', -1, 64))
	return buffer
}
