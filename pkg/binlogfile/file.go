package binlogfile

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

const (
	datetimeFormat = "20060102150405"

	// Version is the binlog file's version
	Version = "v2"
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

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, errors.Annotatef(err, "dir %s", dirpath)
	}

	sort.Strings(names)

	return names, nil
}

// CreateDirAll guarantees to create a new and empty dir
func CreateDirAll(dir string) error {
	if err := os.MkdirAll(dir, file.PrivateDirMode); err != nil {
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

	fnames := FilterBinlogNames(names)
	if len(fnames) == 0 {
		return nil, errors.Annotatef(ErrFileNotFound, "dir %s", dirpath)
	}

	return fnames, nil
}

// FilterBinlogNames filter binlog names from names.
func FilterBinlogNames(names []string) []string {
	var fnames []string
	for _, name := range names {
		if strings.HasSuffix(name, "checkpoint") || strings.HasSuffix(name, ".lock") {
			continue
		}

		if _, err := ParseBinlogName(name); err != nil {
			if !strings.HasSuffix(name, ".tmp") {
				log.Infof("ignored file %v in binlog dir", name)
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

	items := strings.Split(str, "-")
	switch len(items) {
	case 4:
		// binlog file format like: binlog-v2.1-0000000000000001-20181010101010
		_, err = fmt.Sscanf(items[2], "%016d", &index)
	case 2, 3:
		// backward compatibility
		// binlog file format like: binlog-0000000000000001-20181010101010 or binlog-0000000000000001
		_, err = fmt.Sscanf(items[1], "%016d", &index)
	default:
		return 0, errors.Annotatef(ErrBadBinlogName, "binlog file name %s", str)
	}

	return index, errors.Trace(err)
}

// BinlogName creates a binlog file name. The file name format is like binlog-v2.1.0-0000000000000001-20180101010101
// if ts is 0, will return file name like binlog-0000000000000001
func BinlogName(index uint64, ts int64) string {
	if ts == 0 {
		return binlogName(index)
	}

	// transform ts to rough time
	t := time.Unix(oracle.ExtractPhysical(uint64(ts))/1000, 0)
	return binlogNameWithDateTime(index, t)
}

// binlogName creates a binlog file name with index
func binlogName(index uint64) string {
	return fmt.Sprintf("binlog-%016d", index)
}

// binlogNameWithDateTime creates a binlog file name with version, index and datetime
func binlogNameWithDateTime(index uint64, datetime time.Time) string {
	return fmt.Sprintf("binlog-%s-%016d-%s", Version, index, datetime.Format(datetimeFormat))
}
