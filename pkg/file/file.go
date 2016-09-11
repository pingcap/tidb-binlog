package file

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
)

const (
	PrivateFileMode = 0600
	PrivateDirMode  = 0700
)

func isDirWriteable(dir string) error {
	f := path.Join(dir, ".touch")
	if err := ioutil.WriteFile(f, []byte(""), PrivateFileMode); err != nil {
		return err
	}

	return os.Remove(f)
}

// reads and returns all file and dir names from directory f
func ReadDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	sort.Strings(names)

	return names, nil
}

func TouchDirAll(dir string) error {
	if err := os.MkdirAll(dir, PrivateDirMode); err  != nil {
		return err
	}

	return isDirWriteable(dir)
}

func CreateDirAll(dir string) error {
	if err := TouchDirAll(dir); err != nil {
		return err
	}

	ns, err := ReadDir(dir)
	if err != nil {
		return err
	}

	if len(ns) != 0 {
		return fmt.Errorf("expected %q to be empty, got %q", dir, ns)
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
