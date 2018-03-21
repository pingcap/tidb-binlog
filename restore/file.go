package restore

import (
	"path"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
)

type binlogFile struct {
	fullpath string
	offset   int64
}

// searchFiles return matched file and it's offset
func (r *Restore) searchFiles(dir string) ([]binlogFile, error) {
	// read all file names
	sortedNames, err := bf.ReadBinlogNames(dir)
	if err != nil {
		return nil, errors.Annotatef(err, "read binlog file name error")
	}

	// TODO: use pb index to filter the first target binlog file.
	binlogFiles := make([]binlogFile, 0, len(sortedNames))
	for _, name := range sortedNames {
		fullpath := path.Join(r.cfg.Dir, name)
		binlogFiles = append(binlogFiles, binlogFile{fullpath: fullpath, offset: 0})
	}

	log.Infof("binlog files %+v", binlogFiles)

	return binlogFiles, nil
}
