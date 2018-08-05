// +build linux

package storage

import "syscall"

// fdatasync() is similar to fsync(), but does not flush modified metadata unless that metadata is needed in order to allow a subsequent data retrieval to be correctly handled
// https://linux.die.net/man/2/fdatasync
// in some os don't support fdatasync, we can just use sync
func (lf *logFile) fdatasync() error {
	return syscall.Fdatasync(int(lf.fd.Fd()))
}
