package file

import (
	"errors"
	"os"
	"syscall"
)

var (
	// ErrLocked means that fail to get file lock
	ErrLocked = errors.New("pkg/file: file already locked")
)

// LockedFile wraps the file into a LockedFile concept simply
type LockedFile struct{ *os.File }

// TryLockFile try to open the file with the file lock, it's unblock
func TryLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
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
