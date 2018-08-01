// +build linux

package storage

import "syscall"

func (lf *logFile) sync() error {
	return syscall.Fdatasync(int(lf.fd.Fd()))
}
