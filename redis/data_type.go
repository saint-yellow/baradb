package redis

// dataType enum of data types in Redis
type dataType = byte

const (
	String dataType = iota + 1
	Hash
	Set
	List
	Zset
)
