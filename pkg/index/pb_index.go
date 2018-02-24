package index

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/lunny/log"
	"github.com/siddontang/go/sync2"
)

// index file format:  ts:file:offset

const (
	defaultInterval  int64 = 1000
	defaultPosBuffer       = 100

	defaultIndexName = "binlog.index"
)

// Position is a mapping relation among ts, filename and offset.
type Position struct {
	Ts     int64
	File   string
	Offset int64
}

func (p Position) String() string {
	return fmt.Sprintf("%d:%s:%d", p.Ts, p.File, p.Offset)
}

// PbIndex holds information about pb index file.
type PbIndex struct {
	dir      string
	file     string
	fd       *os.File
	bw       *bufio.Writer
	br       *bufio.Reader
	posCh    chan Position
	interval int64
	curpos   sync2.AtomicInt64
}

// NewPbIndex creates a new PbIndex object. filepath should fullpath of index file.
func NewPbIndex(dir, indexName string) (*PbIndex, error) {
	if indexName == "" {
		indexName = defaultIndexName
	}

	fp := path.Join(dir, indexName)

	fd, err := os.OpenFile(fp, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &PbIndex{
		file:     fp,
		dir:      dir,
		fd:       fd,
		bw:       bufio.NewWriter(fd),
		br:       bufio.NewReader(fd),
		posCh:    make(chan Position, defaultPosBuffer),
		interval: defaultInterval,
	}, nil
}

// SetInterval sets interval value.
func (pi *PbIndex) SetInterval(interval int64) {
	pi.interval = interval
}

// Run handles position.
// FIXME: keep data of head and tail?
func (pi *PbIndex) Run() {
	for {
		select {
		case pos := <-pi.posCh:
			pi.curpos.Add(1)
			if pi.curpos.Get() > pi.interval {
				err := pi.write(pos)
				if err != nil {
					log.Errorf("write pb index error %v", err)
				}
				// reset it
				pi.curpos.Set(0)
			}
		}
	}
}

func (pi *PbIndex) write(pos Position) error {
	_, err := pi.bw.WriteString(pos.String())
	if err != nil {
		return errors.Trace(err)
	}
	err = pi.bw.WriteByte('\n')
	return errors.Trace(err)
}

// MarkOffset marks position to file(if meets conditions).
func (pi *PbIndex) MarkOffset(pos Position) {
	pi.posCh <- pos
}

// Close closes pbindex.
func (pi *PbIndex) Close() {
	if err := pi.bw.Flush(); err != nil {
		log.Warnf("flush pb index error %v", err)
	}
	pi.fd.Close()
}

// Search searches target protobuf files.
func (pi *PbIndex) Search(ts int64) (file string, offset int64, err error) {
	stat, err := pi.fd.Stat()
	if err != nil {
		return "", 0, errors.Trace(err)
	}
	// the file content is empty.
	if stat.Size() == 0 {
		return "", 0, nil
	}

	// TODO: improve performance

	tsStr := strconv.FormatInt(ts, 10)

	var targetLine string
	var lastLine string

	for {
		line, err := pi.br.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", 0, errors.Trace(err)
		}
		realLine := strings.TrimSpace(line[:len(line)-1])
		if len(realLine) == 0 {
			continue
		}

		// Note: it costs 1 ~ 2 seconds to find target line in a 10-million-lines file.
		// So I think the performance is acceptable now.

		// FIXME: is it reliable to use len(tsStr)?
		cmp := strings.Compare(realLine[:len(tsStr)], tsStr)
		if cmp == 0 {
			targetLine = realLine
			log.Infof("found target ts line %s", targetLine)
			break
		} else if cmp == -1 {
			continue
		} else if cmp == 1 {
			if lastLine != "" {
				targetLine = lastLine
			} else {
				targetLine = realLine
			}
			log.Infof("found target ts line %s", targetLine)
			break
		}
	}

	contents := strings.Split(targetLine, ":")
	file = contents[1]
	offset, err = strconv.ParseInt(contents[2], 10, 64)
	return file, offset, errors.Trace(err)
}
