package restore

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"path"
	"regexp"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	"github.com/pingcap/tidb-binlog/restore/executor"
	"github.com/pingcap/tidb-binlog/restore/savepoint"
	"github.com/pingcap/tidb-binlog/restore/translator"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// Restore is the main part of the restore tool.
type Restore struct {
	cfg        *Config
	translator translator.Translator
	executor   executor.Executor
	savepoint  savepoint.Savepoint

	reMap map[string]*regexp.Regexp
}

// New creates a Restore object.
func New(cfg *Config) (*Restore, error) {
	executor, err := executor.New(cfg.DestType, cfg.DestDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("cfg %+v", cfg)
	fullpath := path.Join(cfg.Dir, cfg.Savepoint.Name)
	log.Infof("savepoint type %s, name %s", cfg.Savepoint.Type, fullpath)
	savepoint, err := savepoint.Open(cfg.Savepoint.Type, fullpath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Restore{
		cfg:        cfg,
		translator: translator.New(cfg.DestType, false),
		executor:   executor,
		savepoint:  savepoint,
		reMap:      make(map[string]*regexp.Regexp),
	}, nil
}

func (r *Restore) prepare() error {
	r.GenRegexMap()

	_, err := r.savepoint.Load()
	return errors.Trace(err)
}

// Process runs the main procedure.
func (r *Restore) Process() error {
	if err := r.prepare(); err != nil {
		return errors.Trace(err)
	}

	dir := r.cfg.Dir
	files, err := r.searchFiles(dir)
	if err != nil {
		return errors.Trace(err)
	}
	codec := compress.ToCompressionCodec(r.cfg.Compression)

	for _, file := range files {
		fd, err := os.OpenFile(file.fullpath, os.O_RDONLY, 0600)
		if err != nil {
			return errors.Annotatef(err, "open file %s error", file.fullpath)
		}
		defer fd.Close()

		ret, err := fd.Seek(file.offset, io.SeekStart)
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("seek to file %s offset %d got %d", file.fullpath, file.offset, ret)

		br := bufio.NewReader(fd)
		var rd io.Reader
		switch codec {
		case compress.CompressionNone:
			rd = br
		case compress.CompressionGZIP:
			gzr, err := gzip.NewReader(br)
			if err == io.EOF {
				continue
			}
			if err != nil {
				return errors.Trace(err)
			}
			rd = gzr
			defer gzr.Close()
		}

		for {
			binlog, err := Decode(rd)
			if errors.Cause(err) == io.EOF {
				if gzr, ok := rd.(*gzip.Reader); ok {
					gzr.Close()
				}
				fd.Close()
				log.Infof("read file %s end", file.fullpath)
				break
			}
			if err != nil {
				return errors.Annotatef(err, "decode binlog error")
			}

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

			ret, err := fd.Seek(0, io.SeekCurrent)
			if err != nil {
				return errors.Trace(err)
			}
			dt := time.Unix(oracle.ExtractPhysical(uint64(binlog.CommitTs))/1000, 0)
			log.Infof("offset %d, ts %d, datetime %s", ret, binlog.CommitTs, dt.String())
			err = r.savepoint.Save(&savepoint.Position{Filename: file.fullpath, Offset: ret, Ts: binlog.CommitTs})
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

// Close closes the Restore object.
func (r *Restore) Close() error {
	if err := r.savepoint.Flush(); err != nil {
		return errors.Trace(err)
	}

	if err := r.executor.Close(); err != nil {
		log.Errorf("close executor err %v", err)
	}
	if err := r.savepoint.Close(); err != nil {
		log.Errorf("close savepoint err %v", err)
	}
	return nil
}
