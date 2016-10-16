package store

// Store defines a set of methods to manipulate a KV storage for binlog.
// key is the commitTs of binlog, while the binlog payload as value.
// It also records the time of putting KV to store as timestamp for calculating the age of tuple.
type Store interface {
	// // Get returns the payload and age of binlog by given commitTs.
	// Get(commitTS int64) ([]byte, error)
	// Scan scans from the commitTS the specified commitTs.
	Scan(commitTS int64, f func(key []byte, val []byte) (bool, error)) error
	// WriteBatch writes data in Batch.
	WriteBatch(b Batch) error
	// LoadMark loads deposit window from store.
	LoadMark() (int64, error)
	// SaveMark saves deposit window to store.
	SaveMark(int64) error
	// Close closes the store DB.
	Close() error
}

// Batch provides a way to batch txn
type Batch interface {
	// Put appends 'put operation' of the key/value to the batch.
	Put(key []byte, value []byte)
	// Delete appends 'delete operation' of the key/value to the batch.
	Delete(key []byte)
	// Len return length of the batch
	Len() int
}
