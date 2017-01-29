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
	meta  store.Store
	ds    *BinlogStorage
}

// NewDepositWindow return an instance of DepositWindow
func NewDepositWindow(s store.Store, ds *BinlogStorage) (*DepositWindow, error) {
	l, u, err := loadMark(s, ds)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &DepositWindow{
		upper: u,
		lower: l,
		meta:  s,
		ds:    ds,
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
	err := d.meta.Put(windowNamespace, windowKeyName, data)
	if err != nil {
		return errors.Trace(err)
	}
	atomic.StoreInt64(&d.lower, val)
	return nil
}

// loadMark loads the lower upper boundary of the window from store.
func loadMark(s store.Store, ds *BinlogStorage) (int64, int64, error) {
	var l, u int64
	data, err := s.Get(windowNamespace, windowKeyName)
	if err != nil {
		if errors.IsNotFound(err) {
			return 0, 0, nil
		}

		return 0, 0, errors.Trace(err)
	}

	_, l, err = codec.DecodeInt(data)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	ts, err := ds.EndKey()
	if err != nil {
		return l, 0, errors.Trace(err)
	}

	if ts == nil {
		return l, 0, nil
	}
	_, u, err = codec.DecodeInt(ts)
	if err != nil {
		return l, 0, errors.Trace(err)
	}

	return l, u, nil
}
