package binlogfile

import (
	"fmt"
	"os"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
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
		curIndex, _, err := ParseBinlogName(name)
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
		curSuffix, _, err := ParseBinlogName(name)
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
func FilterBinlogNames(names []string) (fnames []string) {
	for _, name := range names {
		if strings.HasSuffix(name, "checkpoint") || strings.HasSuffix(name, ".lock") {
			continue
		}

		if _, _, err := ParseBinlogName(name); err != nil {
			if !strings.HasSuffix(name, ".tmp") {
				log.Infof("ignored file %v in binlog dir", name)
			}
			continue
		}
		fnames = append(fnames, name)
	}

	return
}

// ParseBinlogName parse binlog file name and return binlog index.
func ParseBinlogName(str string) (index uint64, ts int64, err error) {
	if !strings.HasPrefix(str, "binlog-") {
		return 0, 0, ErrBadBinlogName
	}

	items := strings.Split(str, "-")
	switch len(items) {
	case 2, 3:
		// backward compatibility
		// binlog file format like: binlog-0000000000000001-20181010101010 or binlog-0000000000000001
		_, err = fmt.Sscanf(items[1], "%016d", &index)
	case 4:
		// binlog file format like: binlog-0000000000000001-20181010101010-407623959013752832.tar.gz
		_, err = fmt.Sscanf(items[1], "%016d", &index)
		if err != nil {
			return 0, 0, errors.Trace(err)
		}

		ts, err = strconv.ParseInt(strings.Split(items[3], ".")[0], 10, 64)
	default:
		return 0, 0, errors.Annotatef(ErrBadBinlogName, "binlog file name %s", str)
	}

	return index, ts, errors.Trace(err)
}

// BinlogName creates a binlog file name. The file name format is like binlog-0000000000000001-20181010101010
func BinlogName(index uint64) string {
	currentTime := time.Now()
	return binlogNameWithDateTime(index, currentTime)
}

// binlogNameWithDateTime creates a binlog file name.
func binlogNameWithDateTime(index uint64, datetime time.Time) string {
	return fmt.Sprintf("binlog-%016d-%s", index, datetime.Format(datetimeFormat))
}

// GetFirstBinlogCommitTS return the first binlog's commit ts in a pb file.
func GetFirstBinlogCommitTS(filename string) (int64, error) {
	_, binlogFileName := path.Split(filename)
	_, ts, err := ParseBinlogName(binlogFileName)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if ts > 0 {
		return ts, nil
	}

	fd, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return 0, errors.Annotatef(err, "open file %s error", filename)
	}
	defer fd.Close()

	// get the first binlog in file
	r, err := NewReader(fd)
	if err != nil {
		return 0, errors.Trace(err)
	}

	binlog, _, err := DecodeBinlog(r)
	if errors.Cause(err) == io.EOF {
		log.Warnf("no binlog find in %s", filename)
		return 0, nil
	}
	if err != nil {
		return 0, errors.Annotatef(err, "decode binlog error")
	}

	return binlog.CommitTs, nil
}