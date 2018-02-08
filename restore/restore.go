package restore

import (
	"bufio"
	"io"
	"os"
	"regexp"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pkgsql "github.com/pingcap/tidb-binlog/pkg/sql"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb-binlog/restore/executor"
	"github.com/pingcap/tidb-binlog/restore/savepoint"
	"github.com/pingcap/tidb-binlog/restore/translator"
)

type Restore struct {
	cfg        *Config
	translator translator.Translator
	executor   executor.Executor
	savepoint  savepoint.Savepoint

	reMap map[string]*regexp.Regexp
}

func New(cfg *Config) (*Restore, error) {
	executor, err := executor.New(cfg.DestType, cfg.DestDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	savepoint, err := savepoint.Open(cfg.Savepoint.Type, cfg.Savepoint.Path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Restore{
		cfg:        cfg,
		translator: translator.New(cfg.DestType, false),
		executor:   executor,
		savepoint:  savepoint,
	}, nil
}

type binlogFile struct {
	fullpath string
	offset   int64
}

// searchFiles return matched file and it's offset
func (r *Restore) searchFiles(dir string) ([]binlogFile, error) {
	// read all file names
	names, err := readBinlogNames(dir)
	if err != nil {
		return nil, errors.Annotatef(err, "read binlog file name error")
	}

	_ = names

	// TODO
	return nil, nil
}

// Start runs the restore procedure.
func (r *Restore) Start() error {
	r.GenRegexMap()
	_, err := r.savepoint.Load()
	if err != nil {
		return errors.Trace(err)
	}

	dir := r.cfg.Dir
	files, err := r.searchFiles(dir)
	if err != nil {
		return errors.Trace(err)
	}

	for _, file := range files {
		f, err := os.OpenFile(file.fullpath, os.O_RDONLY, 0600)
		if err != nil {
			return errors.Annotatef(err, "open file %s error", file.fullpath)
		}
		defer f.Close()

		ret, err := f.Seek(file.offset, io.SeekStart)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debugf("seek to file %s offset %d got %d", file.fullpath, file.offset, ret)

		reader := bufio.NewReader(f)
		for {
			binlog, err := Decode(reader)
			if err != nil && errors.Cause(err) != io.EOF {
				return errors.Annotatef(err, "decode binlog error")
			}
			if errors.Cause(err) == io.EOF {
				break
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
		}
	}

	return nil
}

// Close closes Restore.
func (r *Restore) Close() error {
	if err := r.executor.Close(); err != nil {
		log.Errorf("close executor err %v", err)
	}
	if err := r.savepoint.Close(); err != nil {
		log.Errorf("close savepoint err %v", err)
	}
	return nil
}

// Translate translate payload to SQL.
func (r *Restore) Translate(binlog *pb.Binlog) (sqls []string, args [][]interface{}, isDDL bool, err error) {
	if !isAcceptableBinlog(binlog, r.cfg.StartTS, r.cfg.EndTS) {
		return
	}

	switch binlog.Tp {
	case pb.BinlogType_DML:
		sqls, args, err = r.translateDML(binlog)
		return sqls, args, false, errors.Trace(err)
	case pb.BinlogType_DDL:
		sqls, args, err = r.translateDDL(binlog)
		return sqls, args, true, errors.Trace(err)
	default:
		panic("unreachable")
	}
}

func (r *Restore) translateDML(binlog *pb.Binlog) ([]string, [][]interface{}, error) {
	// skip

	dml := binlog.DmlData
	if dml == nil {
		return nil, nil, errors.New("dml binlog's data can't be empty")
	}

	sqls := make([]string, 0, len(dml.Events))
	args := make([][]interface{}, 0, len(dml.Events))

	var (
		sql string
		arg []interface{}
		err error
	)

	for _, event := range dml.Events {
		if r.SkipBySchemaAndTable(event.GetSchemaName(), event.GetTableName()) {
			continue
		}

		e := &event
		tp := e.GetTp()
		row := e.GetRow()
		switch tp {
		case pb.EventType_Insert:
			sql, arg, err = r.translator.TransInsert(binlog, e, row)
		case pb.EventType_Update:
			sql, arg, err = r.translator.TransUpdate(binlog, e, row)
		case pb.EventType_Delete:
			sql, arg, err = r.translator.TransDelete(binlog, e, row)
		default:
			panic("unreachable")
		}
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		sqls = append(sqls, sql)
		args = append(args, arg)
	}

	return sqls, args, nil
}

func (r *Restore) translateDDL(binlog *pb.Binlog) ([]string, [][]interface{}, error) {
	_, table, err := parseDDL(string(binlog.GetDdlQuery()))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if r.SkipBySchemaAndTable(table.Schema, table.Name) {
		return nil, nil, nil
	}

	ddl, args, err := r.translator.TransDDL(binlog)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return []string{ddl}, [][]interface{}{args}, nil
}
