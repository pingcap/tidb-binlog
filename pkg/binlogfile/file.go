package binlogfile

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
)

const (
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

	var datetime string
	_, err = fmt.Sscanf(str, "binlog-%016d-%s", &index, &datetime)
	// backward compatibility
	if err == io.ErrUnexpectedEOF {
		_, err = fmt.Sscanf(str, "binlog-%016d", &index)
	}
	return index, errors.Trace(err)
}

// BinlogName creates a binlog file name. The file name format is like binlog-0000000000000001-20180101010101-404615461397069825
func BinlogName(index uint64, ts int64) string {
	return binlogNameWithDateTime(index, ts)
}

// binlogNameWithDateTime creates a binlog file name.
func binlogNameWithDateTime(index uint64, ts int64) string {
	// transfor ts to rough time
	t := time.Unix(ts>>18/1000, 0)
	return fmt.Sprintf("binlog-%016d-%s-%d", index, t.Format(datetimeFormat), ts)
}

// FormatDateTimeStr formate datatime string to standard format like "2018-10-01T01:01:01"
func FormatDateTimeStr(s string) (string, error) {
	if len(s) != len(datetimeFormat) {
		return "", errors.Errorf("%s is not a valid time string in binlog file", s)
	}

	return fmt.Sprintf("%s-%s-%sT%s:%s:%s", s[0:4], s[4:6], s[6:8], s[8:10], s[10:12], s[12:14]), nil
}
