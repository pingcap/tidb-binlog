package checkpoint

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
)

var (
	maxSaveTime = 30 * time.Second
)

// implements a file checkpoint.

type fileCheckpoint struct {
	mu           sync.RWMutex
	fd           *file.LockedFile
	pos          *Position
	lastSaveTime time.Time
}

func newFileCheckpoint(filename string) (Checkpoint, error) {
	fd, err := file.TryLockFile(filename, os.O_RDWR|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &fileCheckpoint{fd: fd}, nil
}

func (f *fileCheckpoint) Load() (pos *Position, err error) {
	f.fd.Seek(0, io.SeekStart)
	pos = &Position{}
	_, err = toml.DecodeReader(f.fd, pos)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return pos, nil
}

func (f *fileCheckpoint) Save(pos *Position) (err error) {
	f.mu.Lock()
	f.pos = pos
	f.mu.Unlock()

	if f.Check() {
		err = f.Flush()
	}
	return errors.Trace(err)
}

func (f *fileCheckpoint) Check() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return time.Since(f.lastSaveTime) >= maxSaveTime
}

func (f *fileCheckpoint) Flush() error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	f.fd.Seek(0, io.SeekStart)
	encoder := toml.NewEncoder(f.fd)
	err := encoder.Encode(f.pos)
	if err != nil {
		return errors.Trace(err)
	}
	f.lastSaveTime = time.Now()
	log.Infof("saved checkpoint position %+v to file", f.pos)
	return nil
}

func (f *fileCheckpoint) Close() error {
	return errors.Trace(f.fd.Close())
}
