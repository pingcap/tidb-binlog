package repora

import (
	"bufio"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
)

type binlogFile struct {
	fullpath string
	offset   int64
}

// searchFiles return matched file and it's offset
func (r *Reparo) searchFiles(dir string) ([]binlogFile, error) {
	// read all file names
	sortedNames, err := bf.ReadBinlogNames(dir)
	if err != nil {
		return nil, errors.Annotatef(err, "read binlog file name error")
	}

	if len(sortedNames) == 0 {
		return nil, errors.Errorf("no binlog file found under %s", dir)
	}

	return r.filterFiles(sortedNames)
}

func (r *Reparo) filterFiles(fileNames []string) ([]binlogFile, error) {
	binlogFiles := make([]binlogFile, 0, len(fileNames))
	var latestBinlogFile string

	appendFile := func() {
		if latestBinlogFile != "" {
			fullpath := path.Join(r.cfg.Dir, latestBinlogFile)
			binlogFiles = append(binlogFiles, binlogFile{fullpath: fullpath, offset: 0})
			latestBinlogFile = ""
		}
	}

	for _, file := range fileNames {
		ts, err := r.getFirstBinlogCommitTS(file)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if ts < r.cfg.StartTSO {
			latestBinlogFile = file
			continue
		}

		if ts == r.cfg.StartTSO {
			latestBinlogFile = file
			appendFile()
			continue
		}

		if ts > r.cfg.StopTSO && r.cfg.StopTSO != 0 {
			break
		}

		appendFile()
		latestBinlogFile = file
	}
	appendFile()

	log.Infof("binlog files %+v, start tso: %d, stop tso: %d", binlogFiles, r.cfg.StartTSO, r.cfg.StopTSO)
	return binlogFiles, nil
}

func (r *Reparo) getFirstBinlogCommitTS(filename string) (int64, error) {
	fileNameItems := strings.Split(filename, "-")

	// old version's binlog file looks like binlog-00000000000000001-20180101010101
	// new version's binlog file looks like binlog-00000000000000001-20180101010101-404615461397069825
	if len(fileNameItems) == 4 {
		ts, err := strconv.ParseInt(fileNameItems[2], 10, 64)
		if err != nil {
			return 0, errors.Annotatef(err, "analyse binlog file name error")
		}

		return ts, nil
	}

	fd, err := os.OpenFile(path.Join(r.cfg.Dir, filename), os.O_RDONLY, 0600)
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
