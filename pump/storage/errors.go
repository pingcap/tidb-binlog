package storage

import "github.com/pingcap/errors"

var (
	// ErrWrongMagic means the magic number mismatch
	ErrWrongMagic = errors.New("wrong magic")

	// ErrNoAvailableSpace means no available space
	ErrNoAvailableSpace = errors.New("no available space")
)
