package repora

import (
	"bufio"
	"io"
	"os"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
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

	file   *os.File
	reader *bufio.Reader
	idx    int // index of next file to read in files
}

var _ PbReader = &dirPbReader{}

func newDirPbReader(dir string) (r *dirPbReader, err error) {
	files, err := searchFiles(dir)
	if err != nil {
		return nil, errors.Annotate(err, "searchFiles failed")
	}

	r = &dirPbReader{
		dir:   dir,
		files: files,
		idx:   0,
	}

	// if empty files id dir, return success and later `Read` will return `io.EOF`
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

	r.reader = bufio.NewReader(r.file)

	r.idx++

	return nil
}

func (r *dirPbReader) read() (binlog *pb.Binlog, err error) {
	if len(r.files) == 0 {
		return nil, io.EOF
	}

	for {
		binlog, _, err = Decode(r.reader)
		if err == nil {
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
