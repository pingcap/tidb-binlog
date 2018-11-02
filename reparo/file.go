package repora

import (
	"bufio"
	"io"
	"os"
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
func (r *Reparo) searchFiles(dir string) ([]binlogFile, error) {
	// read all file names
	sortedNames, err := bf.ReadBinlogNames(dir)
	if err != nil {
		return nil, errors.Annotatef(err, "read binlog file name error")
	}

	binlogFiles := make([]binlogFile, 0, len(sortedNames))
	for _, name := range sortedNames {
		fullpath := path.Join(r.cfg.Dir, name)
		binlogFiles = append(binlogFiles, binlogFile{fullpath: fullpath, offset: 0})
	}

	return r.filterFiles(binlogFiles)
}

func (r *Reparo) filterFiles(files []binlogFile) ([]binlogFile, error) {
	binlogFiles := make([]binlogFile, 0, len(files))
	var latestBinlogFile binlogFile

	appendFile := func() {
		if latestBinlogFile.fullpath != "" {
			binlogFiles = append(binlogFiles, latestBinlogFile)
			latestBinlogFile.fullpath = ""
		}
	}

	for _, file := range files {
		fd, err := os.OpenFile(file.fullpath, os.O_RDONLY, 0600)
		if err != nil {
			return nil, errors.Annotatef(err, "open file %s error", file.fullpath)
		}
		defer fd.Close()

		// get the first binlog in file
		br := bufio.NewReader(fd)
		binlog, _, err := Decode(br)
		if errors.Cause(err) == io.EOF {
			fd.Close()
			log.Infof("read file %s end", file.fullpath)
			break
		}
		if err != nil {
			return nil, errors.Annotatef(err, "decode binlog error")
		}

		if binlog.CommitTs < r.cfg.StartTSO {
			latestBinlogFile = file
			continue
		}

		if binlog.CommitTs == r.cfg.StartTSO {
			latestBinlogFile = file
			appendFile()
			continue
		}

		if binlog.CommitTs >= r.cfg.StopTSO && r.cfg.StopTSO != 0 {
			appendFile()
			latestBinlogFile = file
			break
		}

		appendFile()
		latestBinlogFile = file
	}
	appendFile()

	log.Infof("binlog files %+v, start tso: %d, stop tso: %d", binlogFiles, r.cfg.StartTSO, r.cfg.StopTSO)
	return binlogFiles, nil
}
