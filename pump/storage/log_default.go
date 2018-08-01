// +build !linux

package storage

func (lf *logFile) sync() error {
	return lf.fd.Sync()
}
