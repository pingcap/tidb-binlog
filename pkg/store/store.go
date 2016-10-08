package store

import (
	"bytes"
	"time"

	"github.com/juju/errors"
	pb "github.com/pingcap/tidb-binlog/proto"
	"github.com/pingcap/tidb/util/codec"
	"github.com/tecbot/gorocksdb"
)

// DB is an interface for (commitTS, Binlog) key-value store.
type DB interface {
	Put(commitTS uint64, value *pb.Binlog) error
	Scan(commitTs uint64) (Iterator, error)
	Close()
}

// Iterator is an interface for visiting key-value in DB.
type Iterator interface {
	Next()
	Valid() bool
	Close()

	Key() (uint64, error)
	Value() (*pb.Binlog, error)
}

var (
	defaultWriteOption = gorocksdb.NewDefaultWriteOptions()
	defaultReadOption  = gorocksdb.NewDefaultReadOptions()
	defaultOption      = gorocksdb.NewDefaultOptions()
)

type rocksDB struct {
	db    *gorocksdb.DB
	cf    []*gorocksdb.ColumnFamilyHandle
	delay time.Duration
}

// New returns a rocksDB which implements DB interface.
// 'path' specifies the RocksDB data directory path.
// data written into DB is not visible immediately, it takes at least
// 'delay' time duration for the data to become visible after their
// first written into DB.
func New(path string, delay time.Duration) (DB, error) {
	opt := gorocksdb.NewDefaultOptions()
	opt.SetCreateIfMissing(true)
	opt.SetCreateIfMissingColumnFamilies(true)
	cfOpts := []*gorocksdb.Options{defaultOption, defaultOption}
	db, cf, err := gorocksdb.OpenDbColumnFamilies(opt, path, []string{"timestamp", "default"}, cfOpts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &rocksDB{
		db: db,
		cf: cf,
	}, nil
}

// Put implements DB interface.
func (r *rocksDB) Put(commitTS uint64, binlog *pb.Binlog) error {
	key := codec.EncodeUint([]byte{}, commitTS)
	now, err := time.Now().MarshalBinary()
	if err != nil {
		return errors.Trace(err)
	}
	err = r.db.PutCF(defaultWriteOption, r.cf[0], key, now)
	if err != nil {
		return errors.Trace(err)
	}
	value, err := encodeBinlog(binlog)
	if err != nil {
		return errors.Trace(err)
	}
	err = r.db.PutCF(defaultWriteOption, r.cf[1], key, value)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Scan implements DB interface.
func (r *rocksDB) Scan(commitTS uint64) (Iterator, error) {
	key := codec.EncodeUint([]byte{}, commitTS)
	iter := r.db.NewIteratorCF(defaultReadOption, r.cf[0])
	iter.Seek(key)

	seekEnd := key
	now := time.Now()
	for iter.Valid() {
		t, err := iterTimeValue(iter)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if t.Add(r.delay).After(now) {
			break
		}

		seekEnd = iterKey(iter)
		iter.Next()
	}

	return &rocksIterator{
		Iterator: r.db.NewIteratorCF(defaultReadOption, r.cf[1]),
		seekEnd:  seekEnd,
	}, nil
}

func (r *rocksDB) Close() {
	if r.db != nil {
		r.db.Close()
		r.db = nil
	}
}

// iterKey wrappers iter.Key(), excepts it doesn't need to call Free.
func iterKey(iter *gorocksdb.Iterator) []byte {
	v := iter.Key()
	ret := make([]byte, v.Size())
	copy(ret, v.Data())
	v.Free()
	return ret
}

func iterBinlogValue(iter *gorocksdb.Iterator) (*pb.Binlog, error) {
	v := iter.Value()
	ret, err := decodeBinlog(v.Data())
	v.Free()
	return ret, err
}

func iterTimeValue(iter *gorocksdb.Iterator) (time.Time, error) {
	v := iter.Value()
	var t time.Time
	err := t.UnmarshalBinary(v.Data())
	v.Free()
	return t, err
}

type rocksIterator struct {
	*gorocksdb.Iterator
	// seek range (start, seekEnd]
	seekEnd []byte
}

func (iter *rocksIterator) Valid() bool {
	if !iter.Iterator.Valid() {
		return false
	}
	key := iter.Iterator.Key()
	if bytes.Compare(key.Data(), iter.seekEnd) > 0 {
		key.Free()
		return false
	}
	key.Free()
	return true
}

func (iter *rocksIterator) Key() (uint64, error) {
	key := iterKey(iter.Iterator)
	_, ret, err := codec.DecodeUint(key)
	return ret, err
}

func (iter *rocksIterator) Value() (*pb.Binlog, error) {
	return iterBinlogValue(iter.Iterator)
}

func encodeBinlog(v *pb.Binlog) ([]byte, error) {
	return v.Marshal()
}

func decodeBinlog(buf []byte) (*pb.Binlog, error) {
	var ret pb.Binlog
	err := ret.Unmarshal(buf)
	return &ret, err
}
