package checkpoint

import (
	"fmt"
)

// Position represents a checkpoint position .
type Position struct {
	Filename string `toml:"filename" json:"filename"`
	Offset   int64  `toml:"offset" json:"offset"`
	Ts       int64  `toml:"ts" json:"ts"`
}

// Checkpoint holds the last position.
type Checkpoint interface {
	// Load loads checkpoint position
	Load() (*Position, error)
	// Save
	Save(pos *Position) error
	// Check checks whether the position needed to be flushed.
	Check() bool
	// Flush flushes the position to persistent storage.
	Flush() error
	// Close the resources the checkpoint implementations needs.
	Close() error
}

func Open(tp string, path string) (Checkpoint, error) {
	switch tp {
	case "file":
		return newFileCheckpoint(path)
	default:
		panic(fmt.Sprintf("checkpoint %s not implemented yet"))
	}
}
