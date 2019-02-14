package storage

import "github.com/pingcap/errors"

var (
	// ErrWrongMagic means the magic number mismatch
	ErrWrongMagic = errors.New("wrong magic")
)
