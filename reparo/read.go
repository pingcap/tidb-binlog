package reparo

import (
	"io"
	"os"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// PbReader is a reader to read pb Binlog
type PbReader interface {
	// read return io.EOF if meet end of data normally
	read() (binlog *pb.Binlog, err error)
}

type dirPbReader struct {
	dir   string
	files []string

	startTS int64
	endTS   int64

	file   *os.File
	reader io.Reader
	idx    int // index of next file to read in files
}

var _ PbReader = &dirPbReader{}

// newDirPbReader return a Reader to read binlogs with commit ts in [startTS, endTS]
func newDirPbReader(dir string, startTS int64, endTS int64) (r *dirPbReader, err error) {
	files, err := searchFiles(dir)
	if err != nil {
		return nil, errors.Annotate(err, "searchFiles failed")
	}

	files, err = filterFiles(files, startTS, endTS)
	if err != nil {
		return nil, errors.Annotate(err, "filterFiles failed")
	}

	r = &dirPbReader{
		startTS: startTS,
		endTS:   endTS,
		dir:     dir,
		files:   files,
		idx:     0,
	}

	// if empty files in dir, return success and later `Read` will return `io.EOF`
	if len(files) > 0 {
		err = r.nextFile()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return
}

func (r *dirPbReader) close() {
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}
}

func (r *dirPbReader) nextFile() (err error) {
	if r.idx >= len(r.files) {
		return io.EOF
	}
	bfile := r.files[r.idx]
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}

	r.file, err = os.OpenFile(bfile, os.O_RDONLY, 0600)
	if err != nil {
		return errors.Annotatef(err, "open file %s error", bfile)
	}

	r.reader, err = bf.NewReader(r.file)
	if err != nil {
		return errors.Trace(err)
	}

	r.idx++

	return nil
}

func (r *dirPbReader) read() (binlog *pb.Binlog, err error) {
	if len(r.files) == 0 {
		return nil, io.EOF
	}

	for {
		binlog, _, err = bf.DecodeBinlog(r.reader)
		if err == nil {
			if !isAcceptableBinlog(binlog, r.startTS, r.endTS) {
				continue
			}

			return
		}

		if errors.Cause(err) == io.EOF {
			log.Infof("read file %s end", r.files[r.idx-1])
			err = r.nextFile()
			if err != nil {
				return nil, err
			}
			continue
		}

		return nil, errors.Annotate(err, "decode failed")
	}
}
