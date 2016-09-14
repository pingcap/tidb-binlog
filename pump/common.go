package pump

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
)

var (
	errBadBinlogName = errors.New("bad file name")
)

// Exist checks the dir exist, that it should have some file
func Exist(dirpath string) bool {
	names, err := file.ReadDir(dirpath)
	if err != nil {
		return false
	}

	return len(names) != 0
}

// searchIndex returns the last array index of file
// equal to or smaller than the given index.
func searchIndex(names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		curIndex, err := parseBinlogName(name)
		if err != nil {
			log.Errorf("parse correct name should never fail: %v", err)
		}

		if index == curIndex {
			return i, true
		}
	}

	return -1, false
}

// isValidBinlog detects the binlog names is valid
func isValidBinlog(names []string) bool {
	var lastSuffix uint64
	for _, name := range names {
		curSuffix, err := parseBinlogName(name)
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

// readBinlogNames returns sorted filenames in the dirpath
func readBinlogNames(dirpath string) ([]string, error) {
	names, err := file.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}

	fnames := checkBinlogNames(names)
	if len(fnames) == 0 {
		return nil, ErrFileNotFound
	}

	return fnames, nil
}

func checkBinlogNames(names []string) []string {
	var fnames []string
	for _, name := range names {
		if _, err := parseBinlogName(name); err != nil {
			if !strings.HasSuffix(name, ".tmp") {
				log.Warningf("ignored file %v in wal", name)
			}
			continue
		}
		fnames = append(fnames, name)
	}

	return fnames
}

func parseBinlogName(str string) (index uint64, err error) {
	if !strings.HasPrefix(str, "binlog-") {
		return 0, errBadBinlogName
	}

	_, err = fmt.Sscanf(str, "binlog-%016d", &index)
	return
}

// the file name format is like binlog-0000000000000001
func fileName(index uint64) string {
	return fmt.Sprintf("binlog-%016d", index)
}
