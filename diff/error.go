package diff

import (
	"errors"
)

var (
	errTableNotExist = notFound{errors.New("table not exist")}
)

type notFound struct {
	error
}

func (n notFound) NotFound() {}

// NotFoundError is the interface for any not found relate error.
// table not found, database not found, file not exist, and so on
type NotFoundError interface {
	error
	NotFound()
}
