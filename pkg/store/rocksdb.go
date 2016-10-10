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
	dbReadOpts  *gorocksdb.ReadOptions
	dbWriteOpts *gorocksdb.WriteOptions
	db          *gorocksdb.DB
	marker      []byte
}

// RocksIterator wraps RocksDB iterator
type RocksIterator struct {
	iter *gorocksdb.Iterator
}

// NewRocksStore returns an instance of RocksDB as storage.
func NewRocksStore(dir string) (Store, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	defer opts.Destroy()
	db, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &RocksStore{
		dbReadOpts:  gorocksdb.NewDefaultReadOptions(),
		dbWriteOpts: gorocksdb.NewDefaultWriteOptions(),
		db:          db,
		marker:      codec.EncodeInt([]byte{}, math.MaxInt64),
	}, nil
}

// Put implements the Put() interface of Store
func (r *RocksStore) Put(commitTs int64, payload []byte) error {
	key := codec.EncodeInt([]byte{}, commitTs)
	data, err := encodePayload(payload)
	if err != nil {
		return errors.Trace(err)
	}
	err = r.db.Put(r.dbWriteOpts, key, data)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Get implements the Get() interface of Store
func (r *RocksStore) Get(commitTs int64) ([]byte, time.Duration, error) {
	key := codec.EncodeInt([]byte{}, commitTs)
	value, err := r.db.Get(r.dbReadOpts, key)
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
	iter := r.db.NewIterator(r.dbReadOpts)
	key := codec.EncodeInt([]byte{}, commitTs)
	iter.Seek(key)
	if iter.Err() != nil {
		return nil, errors.Trace(iter.Err())
	}
	return &RocksIterator{
		iter: iter,
	}, nil
}

// SaveMarker implements the SaveMarker() interface of Store,
// using MaxInt64 as key to store the marker of commitTs
func (r *RocksStore) SaveMarker(commitTs int64) error {
	value := codec.EncodeInt([]byte{}, commitTs)
	err := r.db.Put(r.dbWriteOpts, r.marker, value)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// LoadMarker implements the LoadMarker() interface of Store
func (r *RocksStore) LoadMarker() (int64, error) {
	value, err := r.db.Get(r.dbReadOpts, r.marker)
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
	}
	_, ret, err := codec.DecodeInt(value.Data())
	if err != nil {
		return 0, errors.Trace(err)
	}
	return ret, nil
}

// Close implements the Close() interface of Store
func (r *RocksStore) Close() {
	r.db.Close()
	r.dbReadOpts.Destroy()
	r.dbWriteOpts.Destroy()
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
	_, ret, err := codec.DecodeInt(key.Data())
	if err != nil {
		key.Free()
		return false
	}
	if ret == math.MaxInt64 {
		key.Free()
		return false
	}
	key.Free()
	return true
}

// Close implements the Close() interface of Iterator
func (ri *RocksIterator) Close() {
	ri.iter.Close()
}

// CommitTs implements the CommitTs() interface of Iterator
func (ri *RocksIterator) CommitTs() (int64, error) {
	key := ri.iter.Key()
	_, ret, err := codec.DecodeInt(key.Data())
	if err != nil {
		key.Free()
		return 0, errors.Trace(err)
	}
	key.Free()
	return ret, nil
}

// Payload implements the Payload() interface of Iterator
func (ri *RocksIterator) Payload() ([]byte, time.Duration, error) {
	value := ri.iter.Value()
	data := make([]byte, value.Size())
	copy(data, value.Data())
	payload, ts, err := decodePayload(data)
	if err != nil {
		value.Free()
		return nil, 0, errors.Trace(err)
	}
	value.Free()
	return payload, time.Now().Sub(ts), nil
}
