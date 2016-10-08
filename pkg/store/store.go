package store

import (
	"time"

	"github.com/juju/errors"
)

// Store defines a set of methods to manipulate a KV storage for binlog.
// key is the commitTs of binlog, while the binlog payload as value.
// It also records the time of putting KV to store as timestamp for calculating the age of tuple.
type Store interface {
	// Put adds or updates a binlog into store.
	Put(commitTs int64, payload []byte) error
	// Get returns the payload and age of binlog by given commitTs.
	Get(commitTs int64) ([]byte, time.Duration, error)
	// Scan returns an Iterator of binlog which from the position of the specified commitTs.
	Scan(commitTs int64) (Iterator, error)
	// SaveMarker saves the position of commitTs into store.
	SaveMarker(commitTs int64) error
	// LoadMarker loads the position of commitTs from store.
	LoadMarker() (int64, error)
	// Close closed the store DB.
	Close()
}

// Iterator provides a way to iterate through the keyspace from a specified point,
// as well as access the values of those keys.
type Iterator interface {
	// Next moves the iterator to the next sequential key in store.
	Next() error
	// Valid returns false only when an Iterator has iterated past the last key in store.
	Valid() bool
	// Close closes this iterator.
	Close()
	// CommitTs returns the commitTs of binlog which the iterator currently holds.
	CommitTs() (int64, error)
	// Payload returns the payload and age of binlog which the iterator currently holds.
	Payload() ([]byte, time.Duration, error)
}

func encodePayload(payload []byte) ([]byte, error) {
	nowBinary, err := time.Now().MarshalBinary()
	if err != nil {
		return nil, errors.Trace(err)
	}
	n1 := len(nowBinary)
	n2 := len(payload)
	data := make([]byte, n1+n2)
	copy(data[:n1], nowBinary)
	copy(data[n1:], payload)
	return data, nil
}

func decodePayload(data []byte) ([]byte, time.Time, error) {
	ts := time.Now()
	timeBinary, err := ts.MarshalBinary()
	if err != nil {
		return nil, ts, errors.Trace(err)
	}
	n1 := len(timeBinary)
	if err := ts.UnmarshalBinary(data[:n1]); err != nil {
		return nil, ts, errors.Trace(err)
	}
	return data[n1:], ts, nil
}
