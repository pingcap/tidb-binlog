package file

import (
	"errors"
	"os"
	"syscall"

	"github.com/ngaut/log"
)

var (
	// ErrLocked means that fail to get file lock
	ErrLocked = errors.New("pkg/file: file already locked")
)

const (
	// PrivateFileMode is the permission for service file
	PrivateFileMode = 0600
	// PrivateDirMode is the permission for service dir
	PrivateDirMode = 0700
)

// LockedFile wraps the file into a LockedFile concept simply
type LockedFile struct{ *os.File }

// TryLockFile tries to open the file with the file lock, it's unblock
func TryLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	log.Debugf("TryLockFile %s", path)
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			err = ErrLocked
		}
		return nil, err
	}
	return &LockedFile{f}, nil
}

// LockFile opens file with the file lock, it's blocked
func LockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return &LockedFile{f}, err
}

// UnLockFile unlock a file
func UnLockFile(lockedFile *LockedFile) error {
	err := syscall.Flock(int(lockedFile.Fd()), syscall.LOCK_UN)
	if err != nil {
		return err
	}

	return lockedFile.Close()
}
