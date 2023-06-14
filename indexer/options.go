package indexer

// Options options of an iterator of an indexer
type IteratorOptions struct {
	Prefix  []byte // Traverses an iterator's keys with a specified non-nil prefix
	Reverse bool   // Traverses an iterator reversely if true
}

// DefaultOptions default options of an iterator of an indexer
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
