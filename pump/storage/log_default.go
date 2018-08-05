// +build !linux

package storage

func (lf *logFile) fdatasync() error {
	return lf.fd.Sync()
}
