package cistern

import (
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/util/codec"
)

var windowKeyName = []byte("window")

// DepositWindow holds the upper and lower boundary of the window
// The value of lower boundary should be persisted to store.
type DepositWindow struct {
	upper int64
	lower int64
	bolt  *store.BoltStore
}

// NewDepositWindow return an instance of DepositWindow
func NewDepositWindow(s *store.BoltStore) (*DepositWindow, error) {
	l, err := loadMark(s)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &DepositWindow{
		upper: 0,
		lower: l,
		bolt:  s,
	}, nil
}

// LoadLower returns the lower boundary of window
func (d *DepositWindow) LoadLower() int64 {
	return atomic.LoadInt64(&d.lower)
}

// SaveLower updates the lower boundary of window
func (d *DepositWindow) SaveLower(val int64) {
	atomic.StoreInt64(&d.lower, val)
}

// LoadUpper returns the upper boundary of window
func (d *DepositWindow) LoadUpper() int64 {
	return atomic.LoadInt64(&d.upper)
}

// SaveUpper updates the upper boundary of window
func (d *DepositWindow) SaveUpper(val int64) {
	atomic.StoreInt64(&d.upper, val)
}

// PersistLower updates the lower boundary of window, and write it into storage.
func (d *DepositWindow) PersistLower(val int64) error {
	data := codec.EncodeInt([]byte{}, val)
	err := d.bolt.Put(WindowNamespace, windowKeyName, data)
	if err != nil {
		return errors.Trace(err)
	}
	atomic.StoreInt64(&d.lower, val)
	return nil
}

func loadMark(s *store.BoltStore) (int64, error) {
	var l int64
	data, err := s.Get(WindowNamespace, windowKeyName)
	if err != nil {
		if errors.IsNotFound(err) {
			return 0, nil
		}

		return 0, errors.Trace(err)
	}

	_, l, err = codec.DecodeInt(data)
	if err != nil {
		return 0, errors.Trace(err)
	}

	return l, nil
}
