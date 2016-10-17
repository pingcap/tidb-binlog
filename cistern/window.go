package cistern

import (
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/store"
)

var windowKeyName = []byte("window")

// DepositWindow holds the upper and lower boundary of the window
// The value of lower boundary should be persisted to store.
type DepositWindow struct {
	upper int64
	lower int64
	bolt  store.Store
}

// NewDepositWindow return an instance of DepositWindow
func NewDepositWindow(s store.Store) (*DepositWindow, error) {
	l, err := s.LoadMark()
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
	err := d.bolt.SaveMark(val)
	if err != nil {
		return errors.Trace(err)
	}
	atomic.StoreInt64(&d.lower, val)
	return nil
}
