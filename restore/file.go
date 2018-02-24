package restore

import (
	"fmt"
	"path"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/tidb-binlog/pkg/index"
)

type binlogFile struct {
	fullpath string
	offset   int64
}

// searchFiles return matched file and it's offset
func (r *Restore) searchFiles(dir string) ([]binlogFile, error) {
	// read all file names
	sortedNames, err := readBinlogNames(dir)
	if err != nil {
		return nil, errors.Annotatef(err, "read binlog file name error")
	}

	var (
		firstFile       string
		firstFileOffset int64
	)
	pos := r.savepoint.Pos()
	if pos.Filename != "" {
		firstFile = pos.Filename
		firstFileOffset = pos.Offset
	}

	// try to find matched beginning file.
	if r.cfg.StartTS != 0 {
		idx, err := index.NewPbIndex(r.cfg.Dir, r.cfg.IndexName)
		if err != nil {
			return nil, errors.Trace(err)
		}

		firstFile, firstFileOffset, err = idx.Search(r.cfg.StartTS)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	binlogFiles := make([]binlogFile, 0, len(sortedNames))
	for _, name := range sortedNames {
		fullpath := path.Join(r.cfg.Dir, name)
		cmp := strings.Compare(fullpath, firstFile)
		if cmp < 0 {
			continue
		} else if cmp == 0 {
			binlogFiles = append(binlogFiles, binlogFile{fullpath: fullpath, offset: firstFileOffset})
		} else {
			binlogFiles = append(binlogFiles, binlogFile{fullpath: fullpath, offset: 0})
		}
	}

	return binlogFiles, nil
}

func searchFileIndex(names []string, name string) int {

	// TODO
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
				log.Warningf("ignored file %v", name)
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
