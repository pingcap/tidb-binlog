package etcd

import (
	"fmt"

	"github.com/juju/errors"
)

var (
	errKeyNotFound = errors.New("key not found")
	errKeyExists   = errors.New("key exists")
)

// NewKeyNotFoundError return a not found error that wrap by juju errors
func NewKeyNotFoundError(key string) error {
	msg := fmt.Sprintf("key %s is not found in etcd", key)
	return errors.NewNotFound(errKeyNotFound, msg)
}

// NewKeyExistsError return a exits error that wrap by juju errors
func NewKeyExistsError(key string) error {
	msg := fmt.Sprintf("key %s is already in etcd", key)
	return errors.NewAlreadyExists(errKeyExists, msg)
}
