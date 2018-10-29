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

	if len(sortedNames) == 0 {
		return nil, errors.Errorf("no binlog file found under %s", dir)
	}

	return r.filterFiles(sortedNames)
}

func (r *Reparo) filterFiles(fileNames []string) ([]binlogFile, error) {
	binlogFiles := make([]binlogFile, 0, len(fileNames))
	latestBinlogFile := ""

	for _, name := range fileNames {
		fileNameItems := strings.Split(name, "-")
		createTimeStr, err := bf.FormatTimeStr(fileNameItems[len(fileNameItems)-1])
		if err != nil {
			return nil, errors.Annotatef(err, "analyse binlog file name error")
		}
		fileCreateTime, err := time.ParseInLocation("2006-01-02T15:04:05", createTimeStr, time.Local)
		if err != nil {
			return nil, errors.Annotatef(err, "analyse binlog file name error")
		}

		if int64(oracle.ComposeTS(fileCreateTime.Unix()*1000, 0)) < r.cfg.StartTSO {
			latestBinlogFile = name
			continue
		}

		if int64(oracle.ComposeTS(fileCreateTime.Unix()*1000, 0)) > r.cfg.StopTSO && r.cfg.StopTSO != 0 {
			latestBinlogFile = name
			break
		}

		if preBinlogFile != "" {
			fullpath := path.Join(r.cfg.Dir, preBinlogFile)
			latestBinlogFile = append(binlogFiles, binlogFile{fullpath: fullpath, offset: 0})
		}

		latestBinlogFile = name
	}

	fullpath := path.Join(r.cfg.Dir, latestBinlogFile)
	binlogFiles = append(binlogFiles, binlogFile{fullpath: fullpath, offset: 0})

	log.Infof("binlog files %+v, start tso: %d, stop tso: %d", binlogFiles, r.cfg.StartTSO, r.cfg.StopTSO)
	return binlogFiles, nil
}
