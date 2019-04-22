// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package binlogfile

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"go.uber.org/zap"
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
		// TODO handle the err
		if err != nil {
			log.Error("parse correct name should never fail", zap.Error(err))
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
			log.Fatal("binlogger: parse corrent name should never fail", zap.Error(err))
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

		if _, _, err := ParseBinlogName(name); err != nil {
			if !strings.HasSuffix(name, ".tmp") {
				log.Info("ignored file in binlog dir", zap.String("name", name))
			}
			continue
		}
		fnames = append(fnames, name)
	}

	return fnames
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
