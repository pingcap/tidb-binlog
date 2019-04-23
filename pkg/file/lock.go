// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
