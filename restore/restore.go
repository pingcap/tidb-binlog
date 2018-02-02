package restore

import (
	"bufio"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/restore/translator"
)

type Restore struct {
	cfg *Config
}

func New(cfg *Config) *Restore {
	return &Restore{cfg: cfg}
}

func (r *Restore) Start() error {
	binlogFile := r.cfg.Binfile

	dir := filepath.Dir(binlogFile)
	// read all file names
	names, err := readBinlogNames(dir)
	if err != nil {
		log.Fatalf("read binlog file name error %v", err)
	}

	trans := translator.New("print", false)

	log.Debugf("names %+v, name %s", names, filepath.Base(binlogFile))
	// find the target file's index
	index := searchFileIndex(names, filepath.Base(binlogFile))
	log.Debugf("index %d", index)
	for _, name := range names[index:] {
		p := path.Join(dir, name)
		f, err := os.OpenFile(p, os.O_RDONLY, 0600)
		if err != nil {
			log.Fatalf("open file %s error %v", name, err)
		}
		defer f.Close()

		reader := bufio.NewReader(io.Reader(f))
		for {
			payload, err := readBinlog(reader)
			if err != nil && err != io.EOF {
				log.Fatalf("decode error %v", err)
			}
			if err == io.EOF {
				break
			}
			translator.Translate(payload, trans)
		}
	}

	return nil
}

func (r *Restore) Close() error {
	return nil
}
