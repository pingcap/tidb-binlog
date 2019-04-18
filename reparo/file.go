package reparo

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

	return binlogFiles, nil
}

// filterFiles assume fileNames is sorted by commit time stamp,
// and may filter files not not overlap with [startTS, endTS]
func filterFiles(fileNames []string, startTS int64, endTS int64) ([]string, error) {
	binlogFiles := make([]string, 0, len(fileNames))
	var latestBinlogFile string

	appendFile := func() {
		if latestBinlogFile != "" {
			binlogFiles = append(binlogFiles, latestBinlogFile)
			latestBinlogFile = ""
		}
	}

	for _, file := range fileNames {
		ts, err := bf.GetFirstBinlogCommitTS(file)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ts <= startTS {
			latestBinlogFile = file
			continue
		}

		if ts > endTS && endTS != 0 {
			break
		}

		appendFile()
		latestBinlogFile = file
	}
	appendFile()

	log.Infof("binlog files %+v, start tso: %d, stop tso: %d", binlogFiles, startTS, endTS)
	return binlogFiles, nil
}
