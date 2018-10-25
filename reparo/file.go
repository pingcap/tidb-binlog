package repora

import (
	"fmt"
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

	return r.filterFiles(sortedNames)
}

func (r *Reparo) filterFiles(fileNames []string) ([]binlogFile, error) {
	binlogFiles := make([]binlogFile, 0, len(fileNames))
	preBinlogFile := ""

	for _, name := range fileNames {
		fileNameItems := strings.Split(name, "-")
		createTimeStr, err := formatTimeStr(fileNameItems[len(fileNameItems)-1])
		if err != nil {
			return nil, errors.Annotatef(err, "analyse binlog file name error")
		}

		fileCreateTime, err := time.ParseInLocation("2006-01-02T15:04:05", createTimeStr, time.Local)
		if err != nil {
			return nil, errors.Annotatef(err, "analyse binlog file name error")
		}

		if int64(oracle.ComposeTS(fileCreateTime.Unix()*1000, 0)) < r.cfg.StartTSO {
			preBinlogFile = name
			continue
		}

		if int64(oracle.ComposeTS(fileCreateTime.Unix()*1000, 0)) > r.cfg.StopTSO && r.cfg.StopTSO != 0{
			preBinlogFile = name
			break
		}

		if preBinlogFile != "" {
			fullpath := path.Join(r.cfg.Dir, preBinlogFile)
			binlogFiles = append(binlogFiles, binlogFile{fullpath: fullpath, offset: 0})
		} 
		
		preBinlogFile = name
	}

	fullpath := path.Join(r.cfg.Dir, preBinlogFile)
	binlogFiles = append(binlogFiles, binlogFile{fullpath: fullpath, offset: 0})

	log.Infof("binlog files %+v, start tso: %d, stop tso: %d", binlogFiles, r.cfg.StartTSO, r.cfg.StopTSO)

	return binlogFiles, nil
}

func formatTimeStr(s string) (string, error) {
	if len(s) != len("20180102010101") {
		return "", errors.Errorf("%s is not a valid time string in binlog file", s)
	}

	return fmt.Sprintf("%s-%s-%sT%s:%s:%s", s[0:4], s[4:6], s[6:8], s[8:10], s[10:12], s[12:14]), nil
}
