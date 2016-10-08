package store

import (
	"math"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/codec"
	"github.com/tecbot/gorocksdb"
)

// RocksStore wraps RocksDB as Store
type RocksStore struct {
	db *gorocksdb.DB
}

// RocksIterator wraps RocksDB iterator
type RocksIterator struct {
	iter *gorocksdb.Iterator
}

// NewRocksStore returns an instance of RocksDB as storage.
func NewRocksStore(dir string) (Store, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &RocksStore{
		db: db,
	}, nil
}

// Put implements the Put() interface of Store
func (r *RocksStore) Put(commitTs int64, payload []byte) error {
	key := codec.EncodeInt([]byte{}, commitTs)
	data, err := encodePayload(payload)
	if err != nil {
		return errors.Trace(err)
	}
	err = r.db.Put(gorocksdb.NewDefaultWriteOptions(), key, data)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Get implements the Get() interface of Store
func (r *RocksStore) Get(commitTs int64) ([]byte, time.Duration, error) {
	key := codec.EncodeInt([]byte{}, commitTs)
	value, err := r.db.Get(gorocksdb.NewDefaultReadOptions(), key)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	defer value.Free()
	if value.Size() == 0 {
		return nil, 0, errors.NotFoundf("commitTs(%d)", commitTs)
	}
	data := make([]byte, value.Size())
	copy(data, value.Data())
	payload, ts, err := decodePayload(data)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return payload, time.Now().Sub(ts), nil
}

// Scan implements the Scan() interface of Store
func (r *RocksStore) Scan(commitTs int64) (Iterator, error) {
	iter := r.db.NewIterator(gorocksdb.NewDefaultReadOptions())
	key := codec.EncodeInt([]byte{}, commitTs)
	iter.Seek(key)
	if iter.Err() != nil {
		return nil, errors.Trace(iter.Err())
	}
	return &RocksIterator{
		iter: iter,
	}, nil
}

// SaveMarker implements the SaveMarker() interface of Store
func (r *RocksStore) SaveMarker(commitTs int64) error {
	// use the MaxInt64 as key to store marker of commitTs
	key := codec.EncodeInt([]byte{}, math.MaxInt64)
	value := codec.EncodeInt([]byte{}, commitTs)
	err := r.db.Put(gorocksdb.NewDefaultWriteOptions(), key, value)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// LoadMarker implements the LoadMarker() interface of Store
func (r *RocksStore) LoadMarker() (int64, error) {
	key := codec.EncodeInt([]byte{}, math.MaxInt64)
	value, err := r.db.Get(gorocksdb.NewDefaultReadOptions(), key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer value.Free()
	if value.Size() == 0 {
		err = r.SaveMarker(0)
		if err != nil {
			return 0, errors.Trace(err)
		}
		return 0, nil
	} else {
		_, ret, err := codec.DecodeInt(value.Data())
		if err != nil {
			return 0, errors.Trace(err)
		}
		return ret, nil
	}
}

// Close implements the Close() interface of Store
func (r *RocksStore) Close() {
	r.db.Close()
}

// Next implements the Next() interface of Iterator
func (ri *RocksIterator) Next() error {
	ri.iter.Next()
	if err := ri.iter.Err(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Valid implements the Valid() interface of Iterator
func (ri *RocksIterator) Valid() bool {
	if !ri.iter.Valid() {
		return false
	}
	key := ri.iter.Key()
	defer key.Free()
	_, ret, err := codec.DecodeInt(key.Data())
	if err != nil {
		return false
	}
	if ret == math.MaxInt64 {
		return false
	}
	return true
}

// Close implements the Close() interface of Iterator
func (ri *RocksIterator) Close() {
	ri.iter.Close()
}

// CommitTs implements the CommitTs() interface of Iterator
func (ri *RocksIterator) CommitTs() (int64, error) {
	key := ri.iter.Key()
	defer key.Free()
	_, ret, err := codec.DecodeInt(key.Data())
	if err != nil {
		return 0, errors.Trace(err)
	}
	return ret, nil
}

// Payload implements the Payload() interface of Iterator
func (ri *RocksIterator) Payload() ([]byte, time.Duration, error) {
	value := ri.iter.Value()
	defer value.Free()
	data := make([]byte, value.Size())
	copy(data, value.Data())
	payload, ts, err := decodePayload(data)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return payload, time.Now().Sub(ts), nil
}
