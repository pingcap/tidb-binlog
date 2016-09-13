package pump

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
)

var (
	badBinlogName = errors.New("bad file name")
)

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

		if index >= curIndex {
			return i, true
		}
	}

	return -1, false
}

// readBinlogNames return sorted filenames in the dirpath
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
	fnames := make([]string, 0)
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
		return 0, badBinlogName
	}

	_, err = fmt.Sscanf(str, "binlog-%016d", &index)
	return
}

// the file name format is like binlog-0000000000000001
func fileName(index uint64) string {
	return fmt.Sprintf("binlog-%016d", index)
}
