package reparo

import (
	"bufio"
	"io"
	"os"
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
		ts, err := getFirstBinlogCommitTS(file)
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

func getFirstBinlogCommitTS(filename string) (int64, error) {
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return 0, errors.Annotatef(err, "open file %s error", filename)
	}
	defer fd.Close()

	// get the first binlog in file
	br := bufio.NewReader(fd)
	binlog, _, err := Decode(br)
	if errors.Cause(err) == io.EOF {
		log.Warnf("no binlog find in %s", filename)
		return 0, nil
	}
	if err != nil {
		return 0, errors.Annotatef(err, "decode binlog error")
	}

	return binlog.CommitTs, nil
}
