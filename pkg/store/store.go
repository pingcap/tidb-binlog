package store

// Store defines a set of methods to manipulate a KV storage for binlog.
// key is the commitTs of binlog, while the binlog payload as value.
// It also records the time of putting KV to store as timestamp for calculating the age of tuple.
type Store interface {
	// Put adds or updates a binlog into store.
	Put(namespace []byte, key []byte, payload []byte) error
	// Get returns the payload and age of binlog by given commitTs.
	Get(namespace []byte, key []byte) ([]byte, error)
	// Scan returns an Iterator of binlog which from the position of the specified commitTs.
	Scan(namespace []byte, startKey []byte, f func(key []byte, val []byte) (bool, error)) error
	// NewBatch creates a Batch for writing.
	NewBatch() Batch
	// Commit writes data in Batch.
	Commit(namespace []byte, b Batch) error
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
