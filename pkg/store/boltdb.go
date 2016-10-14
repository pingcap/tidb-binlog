package store

import (
	"sync"

	"github.com/boltdb/bolt"
	"github.com/juju/errors"
)

// BoltStore wraps BoltDB as Store
type BoltStore struct {
	sync.RWMutex

	db *bolt.DB
}

// NewBoltStore return a bolt store
func NewBoltStore(path string, namespaces [][]byte) (Store, error) {
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
	s.RLock()
	defer s.RUnlock()
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
	s.Lock()
	defer s.Unlock()

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
	s.RLock()
	defer s.RUnlock()

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
	s.Lock()
	defer s.Unlock()

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
