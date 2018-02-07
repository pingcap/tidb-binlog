package restore

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
)

func searchFileIndex(names []string, name string) int {
	for i := len(names) - 1; i >= 0; i-- {
		if name == names[i] {
			return i
		}
	}

	log.Fatalf("file %s not found", name)
	return 0
}

// readBinlogNames returns sorted filenames in the dirpath
func readBinlogNames(dirpath string) ([]string, error) {
	names, err := file.ReadDir(dirpath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	fnames := checkBinlogNames(names)
	if len(fnames) == 0 {
		return nil, errors.NotFoundf("dir %s", dirpath)
	}

	return fnames, nil
}

func checkBinlogNames(names []string) []string {
	var fnames []string
	for _, name := range names {
		if strings.HasSuffix(name, "savepoint") {
			continue
		}
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
		log.Warnf("bad file name %s", str)
		return 0, errors.Errorf("bad file name %s", str)
	}

	_, err = fmt.Sscanf(str, "binlog-%016d", &index)
	return index, errors.Trace(err)
}

// the file name format is like binlog-0000000000000001
func fileName(index uint64) string {
	return fmt.Sprintf("binlog-%016d", index)
}
