package storage

import "github.com/juju/errors"

var (
	// ErrWrongMagic means the magic number mismatch
	ErrWrongMagic = errors.New("wrong magic")
)
