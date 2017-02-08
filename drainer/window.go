package drainer

import (
	"sync/atomic"
)

// DepositWindow holds the upper and lower boundary of the window
// The value of lower boundary should be persisted to store.
type DepositWindow struct {
	upper int64
	lower int64
}

// NewDepositWindow return an instance of DepositWindow
func NewDepositWindow() *DepositWindow {
	return &DepositWindow{}
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
	atomic.StoreInt64(&d.lower, val)
	return nil
}
