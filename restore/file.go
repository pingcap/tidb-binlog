package restore

import (
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
	sortedNames, err := file.ReadBinlogNames(dir)
	if err != nil {
		return nil, errors.Annotatef(err, "read binlog file name error")
	}

	var (
		firstFile       string
		firstFileOffset int64
	)
	// try to find matched beginning file.
	if r.cfg.StartTSO != 0 {
		idx, err := index.NewPbIndex(r.cfg.Dir, r.cfg.IndexName)
		if err != nil {
			return nil, errors.Trace(err)
		}

		firstFile, firstFileOffset, err = idx.Search(r.cfg.StartTSO)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	log.Infof("firstfile %s %d", firstFile, firstFileOffset)

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

	log.Infof("binlog files %+v", binlogFiles)

	return binlogFiles, nil
}
