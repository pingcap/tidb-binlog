package savepoint

import (
	"fmt"
)

// Position represents a savepoint position .
type Position struct {
	Filename string `toml:"filename" json:"filename"`
	Offset   int64  `toml:"offset" json:"offset"`
	Ts       int64  `toml:"ts" json:"ts"`
}

// Savepoint holds the last position.
type Savepoint interface {
	// Load loads savepoint position
	Load() (Position, error)
	// Save
	Save(pos Position) error
	// Flush flushes the position to persistent storage.
	Flush() error
	// Pos tells current position
	TS() Position
	// Close the resources the savepoint implementations needs.
	Close() error
}

// Open opens a new Savepoint based on the type and path.
func Open(tp string, path string) (Savepoint, error) {
	switch tp {
	case "file":
		return newFileSavepoint(path)
	default:
		panic(fmt.Sprintf("savepoint %s not implemented yet", tp))
	}
}
