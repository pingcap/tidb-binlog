package savepoint

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
	flushInterval = 30 * time.Second
)

// implements a file savepoint.

type fileSavepoint struct {
	mu           sync.RWMutex
	fd           *file.LockedFile
	pos          Position
	lastSaveTime time.Time
}

func newFileSavepoint(filename string) (Savepoint, error) {
	fd, err := file.TryLockFile(filename, os.O_RDWR|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &fileSavepoint{fd: fd, lastSaveTime: time.Now(), pos: Position{}}, nil
}

func (f *fileSavepoint) Load() (pos Position, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err = f.fd.Seek(0, io.SeekStart)
	if err != nil {
		return pos, errors.Trace(err)
	}
	pos = Position{}
	_, err = toml.DecodeReader(f.fd, &pos)
	if err != nil {
		return pos, errors.Trace(err)
	}
	f.pos = pos

	return pos, nil
}

func (f *fileSavepoint) Save(pos Position) (err error) {
	f.mu.Lock()
	f.pos = pos
	if time.Since(f.lastSaveTime) >= flushInterval {
		err = f.flush()
	}
	f.mu.Unlock()
	return errors.Trace(err)
}

func (f *fileSavepoint) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flush()
}

func (f *fileSavepoint) flush() error {
	_, err := f.fd.Seek(0, io.SeekStart)
	if err != nil {
		return errors.Trace(err)
	}
	encoder := toml.NewEncoder(f.fd)
	err = encoder.Encode(f.pos)
	if err != nil {
		return errors.Trace(err)
	}
	f.lastSaveTime = time.Now()
	log.Infof("saved savepoint position %+v to file", f.pos)
	return nil
}

func (f *fileSavepoint) Pos() Position {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.pos
}

func (f *fileSavepoint) Close() error {
	return errors.Trace(f.fd.Close())
}
