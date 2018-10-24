package repora

import (
	"fmt"
	//"strconv"
	"time"
	"strings"
	"path"

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
		//fileCreateTime, err := time.ParseInLocation("20180102010101", fileNameItems[len(fileNameItems)-1], time.Local)
		fileCreateTime, err := time.ParseInLocation("2006-01-02T15:04:05", createTimeStr, time.Local)
		//fileCreateTime, err := time.Parse("2018-01-02T01:01:01Z", "2018-10-01T10:11:11Z")
		//fileCreateTime, err := strToTime(fileNameItems[len(fileNameItems)-1])
		//fileCreateTime, err := time.Parse(time.RFC1123, fileNameItems[len(fileNameItems)-1])
		if err != nil {
			return nil, errors.Annotatef(err, "analyse binlog file name error")
		}

		log.Infof("%s to tso %d, StartTSO: %d, less: %s", fileCreateTime, oracle.ComposeTS(fileCreateTime.Unix()*1000, 0), r.cfg.StartTSO, int64(oracle.ComposeTS(fileCreateTime.Unix()*1000, 0)) < r.cfg.StartTSO)
		if int64(oracle.ComposeTS(fileCreateTime.Unix()*1000, 0)) < r.cfg.StartTSO {
			preBinlogFile = name
			continue
		}

		if preBinlogFile != "" {
			fullpath := path.Join(r.cfg.Dir, preBinlogFile)
			binlogFiles = append(binlogFiles, binlogFile{fullpath: fullpath, offset: 0})
			preBinlogFile = ""
		} else {
			preBinlogFile = name
		}
	}

	fullpath := path.Join(r.cfg.Dir, fileNames[len(fileNames)-1])
	binlogFiles = append(binlogFiles, binlogFile{fullpath: fullpath, offset: 0})

	log.Infof("binlog files %+v, start tso: %d", binlogFiles, r.cfg.StartTSO)

	return binlogFiles, nil
}

func formatTimeStr(s string) (string, error) {
	if len(s) != len("20180102010101") {
		return "", errors.Errorf("%s is not a valid time string in binlog file", s)
	}

	return fmt.Sprintf("%s-%s-%sT%s:%s:%s", s[0:4], s[4:6], s[6:8], s[8:10], s[10:12], s[12:14]), nil
}
/*
func strToTime(s string) (time.Time, error) {
	t := time.Time{}

	if len(s) != len("20180102010101") {
		return t, errors.Errorf("%s is not a valid time string in binlog file", s)
	}
	_, err := strconv.Atoi(s)
	if err != nil {
		return t, errors.Errorf("%s is not a valid time string in binlog file", s)
	}
	year, _ := strconv.Atoi(s[0:4])
	mounth, _ := strconv.Atoi(s[4:6])
	day, _ := strconv.Atoi(s[6:8])
	hour, _ := strconv.Atoi(s[8:10])
	miniute, _ := strconv.Atoi(s[10:12])
	second, _ := strconv.Atoi(s[12:14])

	t.Year = year
	t.Month = mounth
	t.Day = day
	t.Hour = hour
	t.Minute = miniute
	t.Second = second

	return t, nil
}
*/