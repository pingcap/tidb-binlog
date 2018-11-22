package repora

import (
	"bufio"
	"io"
	"os"
	"regexp"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb-binlog/reparo/executor"
	"github.com/pingcap/tidb-binlog/reparo/translator"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// Reparo i the main part of the recovery tool.
type Reparo struct {
	cfg        *Config
	translator translator.Translator
	executor   executor.Executor

	reMap map[string]*regexp.Regexp
}

// New creates a Reparo object.
func New(cfg *Config) (*Reparo, error) {
	reporaExecutor, err := executor.New(cfg.DestType, cfg.DestDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("cfg %+v", cfg)
	return &Reparo{
		cfg:        cfg,
		translator: translator.New(cfg.DestType, false),
		executor:   reporaExecutor,
		reMap:      make(map[string]*regexp.Regexp),
	}, nil
}

func (r *Reparo) prepare() error {
	r.GenRegexMap()
	return nil
}

// Process runs the main procedure.
func (r *Reparo) Process() error {
	if err := r.prepare(); err != nil {
		return errors.Trace(err)
	}

	dir := r.cfg.Dir
	files, err := r.searchFiles(dir)
	if err != nil {
		return errors.Trace(err)
	}

	compression := compress.ToCompressionCodec(r.cfg.Compression)

	var offset int64
	for _, file := range files {
		fd, err := os.OpenFile(file.fullpath, os.O_RDONLY, 0600)
		if err != nil {
			return errors.Annotatef(err, "open file %s error", file.fullpath)
		}
		defer fd.Close()

		offset += file.offset
		ret, err := fd.Seek(file.offset, io.SeekStart)
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("seek to file %s offset %d got %d", file.fullpath, file.offset, ret)

		br := bufio.NewReader(fd)

		for {
			binlog, length, err := Decode(br, compression)
			if errors.Cause(err) == io.EOF {
				fd.Close()
				log.Infof("read file %s end", file.fullpath)
				offset = 0
				break
			}
			if err != nil {
				return errors.Annotatef(err, "decode binlog error")
			}
			offset += length

			sqls, args, isDDL, err := r.Translate(binlog)
			if err != nil {
				return errors.Trace(err)
			}
			if len(sqls) == 0 {
				continue
			}

			err = r.executor.Execute(sqls, args, isDDL)
			if err != nil {
				if !pkgsql.IgnoreDDLError(err) {
					return errors.Trace(err)
				}
				log.Warnf("[ignore ddl error][sql]%s[args]%v[error]%v", sqls, args, err)
			}

			dt := time.Unix(oracle.ExtractPhysical(uint64(binlog.CommitTs))/1000, 0)
			log.Infof("offset %d ts %d, datetime %s", offset, binlog.CommitTs, dt.String())
		}
	}

	return nil
}

// Close closes the Reparo object.
func (r *Reparo) Close() error {
	return errors.Trace(r.executor.Close())
}
