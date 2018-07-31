package storage

import "github.com/juju/errors"

var (
	// ErrWrongMagic means the magic number dismatch
	ErrWrongMagic = errors.New("wrong magic")
)
