package file

import (
	"os"
	"sort"

	"github.com/juju/errors"
)

const (
	PrivateFileMode = 0600
	PrivateDirMode  = 0700
)

// reads and returns all file and dir names from directory f
func ReadDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sort.Strings(names)

	return names, nil
}

func TouchDirAll(dir string) error {
	if err := os.MkdirAll(dir, PrivateDirMode); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func CreateDirAll(dir string) error {
	if err := TouchDirAll(dir); err != nil {
		return errors.Trace(err)
	}

	ns, err := ReadDir(dir)
	if err != nil {
		return errors.Trace(err)
	}

	if len(ns) != 0 {
		return errors.Errorf("expected %q to be empty, got %q", dir, ns)
	}

	return nil
}

func Fsync(f *os.File) error {
	return f.Sync()
}

func Exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}
