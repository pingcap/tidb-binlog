package repora

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb/store/tikv/oracle"
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

	// new version's binlog file looks like binlog.00000000000000001-t20180101010101
	if strings.HasPrefix(fileNameItems[len(fileNameItems)-1], "t") {
		timeStr, err := formatTimeStr(fileNameItems[len(fileNameItems)-1][1:])
		if err != nil {
			return 0, errors.Annotatef(err, "analyse binlog file name error")
		}
		t, err := time.ParseInLocation("2006-01-02T15:04:05", timeStr, time.Local)
		if err != nil {
			return 0, errors.Annotatef(err, "analyse binlog file name error")
		}
		return int64(oracle.ComposeTS(t.Unix()*1000, 0)), nil
	} else {
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
}

func formatTimeStr(s string) (string, error) {
	if len(s) != len("20180102010101") {
		return "", errors.Errorf("%s is not a valid time string in binlog file", s)
	}

	return fmt.Sprintf("%s-%s-%sT%s:%s:%s", s[0:4], s[4:6], s[6:8], s[8:10], s[10:12], s[12:14]), nil
}
