package repora

import (
	"path"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
)

// searchFiles return matched file with full path
func searchFiles(dir string) ([]string, error) {
	// read all file names
	sortedNames, err := bf.ReadBinlogNames(dir)
	if err != nil {
		return nil, errors.Annotatef(err, "read binlog file name error")
	}

	binlogFiles := make([]string, 0, len(sortedNames))
	for _, name := range sortedNames {
		fullpath := path.Join(dir, name)
		binlogFiles = append(binlogFiles, fullpath)
	}

	log.Infof("binlog files %+v", binlogFiles)

	return binlogFiles, nil
}
