package store

import (
	"github.com/boltdb/bolt"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/codec"
)

var (
	windowNamespace = []byte("window")
	binlogNamespace = []byte("binlog")
	windowKeyName   = []byte("window")
)

// BoltStore wraps BoltDB as Store
type BoltStore struct {
	db *bolt.DB
}

// NewBoltStore return a bolt store
func NewBoltStore(path string, namespaces [][]byte) (*BoltStore, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, namespace := range namespaces {
		if _, err = tx.CreateBucketIfNotExists(namespace); err != nil {
			tx.Rollback()
			return nil, errors.Trace(err)
		}
	}

	if err = tx.Commit(); err != nil {
		return nil, errors.Trace(err)
	}

	return &BoltStore{
		db: db,
	}, nil
}

// Get implements the Get() interface of Store
func (s *BoltStore) Get(namespace []byte, key []byte) ([]byte, error) {
	var value []byte

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(namespace)
		if b == nil {
			return errors.NotFoundf("bolt: bucket %s", namespace)
		}

		v := b.Get(key)
		if v == nil {
			return errors.NotFoundf("namespace %s, key %s", namespace, key)
		}

		value = append(value, v...)
		return nil
	})

	return value, errors.Trace(err)
}

// Put implements the Put() interface of Store
func (s *BoltStore) Put(namespace []byte, key []byte, payload []byte) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(namespace)
		if b == nil {
			return errors.NotFoundf("bolt: bucket %s", namespace)
		}

		err := b.Put(key, payload)
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	})
	return errors.Trace(err)
}

// Scan implements the Scan() interface of Store
func (s *BoltStore) Scan(namespace []byte, startKey []byte, f func([]byte, []byte) (bool, error)) error {
	return s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(namespace)
		if bucket == nil {
			return errors.NotFoundf("bolt: bucket %s", namespace)
		}

		c := bucket.Cursor()
		for ck, cv := c.Seek(startKey); ck != nil; ck, cv = c.Next() {
			valid, err := f(ck, cv)
			if err != nil {
				return errors.Trace(err)
			}

			if !valid {
				break
			}
		}

		return nil
	})
}

// Commit implements the Commit() interface of Store
func (s *BoltStore) Commit(namespace []byte, b Batch) error {
	bt, ok := b.(*batch)
	if !ok {
		return errors.Errorf("invalid batch type %T", b)
	}

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(namespace)
		if b == nil {
			return errors.NotFoundf("bolt: bucket %s", namespace)
		}

		var err error
		for _, w := range bt.writes {
			if !w.isDelete {
				err = b.Put(w.key, w.value)
			} else {
				err = b.Delete(w.key)
			}

			if err != nil {
				return errors.Trace(err)
			}
		}

		return nil
	})
	return errors.Trace(err)
}

// NewBatch implements the NewBatch() interface of Store
func (s *BoltStore) NewBatch() Batch {
	return &batch{}
}

// Close implements the Close() interface of Store
func (s *BoltStore) Close() error {
	return s.db.Close()
}

type write struct {
	key      []byte
	value    []byte
	isDelete bool
}

type batch struct {
	writes []write
}

// Put implements the Put() interface of Batch
func (b *batch) Put(key []byte, value []byte) {
	w := write{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	}
	b.writes = append(b.writes, w)
}

// Delete implements the Delete() interface of Batch
func (b *batch) Delete(key []byte) {
	w := write{
		key:      append([]byte(nil), key...),
		value:    nil,
		isDelete: true,
	}
	b.writes = append(b.writes, w)
}

// Len implements the Len() interface of Batch
func (b *batch) Len() int {
	return len(b.writes)
}

var _ Store = &storeImpl{}

type storeImpl struct {
	*BoltStore
}

func New(path string) (s Store, err error) {
	var ret storeImpl
	ret.BoltStore, err = NewBoltStore(path, [][]byte{windowNamespace, binlogNamespace})
	return &ret, nil
}

// Scan scans from the commitTS the specified commitTs.
func (s *storeImpl) Scan(commitTS int64, f func(key []byte, val []byte) (bool, error)) error {
	startKey := codec.EncodeInt([]byte{}, commitTS)
	return s.BoltStore.Scan(binlogNamespace, startKey, f)
}

// WriteBatch writes data in Batch.
func (s *storeImpl) WriteBatch(b Batch) error {
	return s.BoltStore.Commit(binlogNamespace, b)
}

// LoadMark loads deposit window from store.
func (s *storeImpl) LoadMark() (int64, error) {
	data, err := s.BoltStore.Get(windowNamespace, windowKeyName)
	if err != nil {
		if errors.IsNotFound(err) {
			return 0, nil
		}
		return 0, errors.Trace(err)
	}

	_, l, err := codec.DecodeInt(data)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return l, nil
}

// SaveMark saves deposit window to store.
func (s *storeImpl) SaveMark(val int64) error {
	data := codec.EncodeInt([]byte{}, val)
	err := s.BoltStore.Put(windowNamespace, windowKeyName, data)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
