package file

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

const (
	// PrivateFileMode is the permission for service file
	PrivateFileMode = 0600

	// PrivateDirMode is the permission for service dir
	PrivateDirMode = 0700

	datetimeFormat = "20060102150405"
)

var (
	// ErrBadBinlogName is an error represents invalid file name.
	ErrBadBinlogName = errors.New("bad file name")

	// ErrFileNotFound is an error represents binlog file not found
	ErrFileNotFound = errors.New("binlogger: file not found")
)

// ReadDir reads and returns all file and dir names from directory f
func ReadDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer dir.Close()

	stat, err := dir.Stat()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !stat.IsDir() {
		return nil, errors.Errorf("%s is not a dir", dirpath)
	}

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sort.Strings(names)

	return names, nil
}

// CreateDirAll guarantees to create a new and empty dir
func CreateDirAll(dir string) error {
	if err := os.MkdirAll(dir, PrivateDirMode); err != nil {
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

// Exist detects the file/dir whether exist
func Exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

// SearchIndex returns the last array index of file
// equal to or smaller than the given index.
func SearchIndex(names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		curIndex, err := ParseBinlogName(name)
		if err != nil {
			log.Errorf("parse correct name should never fail: %v", err)
		}

		if index == curIndex {
			return i, true
		}
	}

	return -1, false
}

// IsValidBinlog detects the binlog names is valid
func IsValidBinlog(names []string) bool {
	var lastSuffix uint64
	for _, name := range names {
		curSuffix, err := ParseBinlogName(name)
		if err != nil {
			log.Fatalf("binlogger: parse corrent name should never fail: %v", err)
		}

		if lastSuffix != 0 && lastSuffix != curSuffix-1 {
			return false
		}
		lastSuffix = curSuffix
	}

	return true
}

// ReadBinlogNames returns sorted filenames in the dirpath
func ReadBinlogNames(dirpath string) ([]string, error) {
	names, err := ReadDir(dirpath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	fnames := CheckBinlogNames(names)
	if len(fnames) == 0 {
		return nil, errors.NotFoundf("dir %s", dirpath)
	}

	return fnames, nil
}

func CheckBinlogNames(names []string) []string {
	var fnames []string
	for _, name := range names {
		if strings.HasSuffix(name, "checkpoint") {
			continue
		}

		if _, err := ParseBinlogName(name); err != nil {
			if !strings.HasSuffix(name, ".tmp") {
				log.Warningf("ignored file %v in wal", name)
			}
			continue
		}
		fnames = append(fnames, name)
	}

	return fnames
}

// ParseBinlogName parse binlog file name and return binlog index.
func ParseBinlogName(str string) (index uint64, err error) {
	if !strings.HasPrefix(str, "binlog-") {
		return 0, ErrBadBinlogName
	}

	_, err = fmt.Sscanf(str, "binlog-%016d", &index)
	return index, errors.Trace(err)
}

// BinlogNameWithDateTime creates a binlog file name.
func BinlogNameWithDateTime(index uint64) string {
	currentTime := time.Now().Format(datetimeFormat)
	return fmt.Sprintf("%s-%s", BinlogName(index), currentTime)
}

// BinlogName creates a binlog file name. The file name format is like binlog-0000000000000001
func BinlogName(index uint64) string {
	return fmt.Sprintf("binlog-%016d", index)
}
