package executor

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/file"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb-binlog/pump"
)

type pbExecutor struct {
	dir       string
	binlogger pump.Binlogger
}

func newPB(cfg *DBConfig) (Executor, error) {
	var (
		binlogger pump.Binlogger
		err       error
	)
	dirPath := cfg.BinlogFileDir
	names, err := file.ReadDir(dirPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(names) > 0 {
		binlogger, err = pump.OpenBinlogger(dirPath)
	} else {
		binlogger, err = pump.CreateBinlogger(dirPath)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pbExecutor{
		dir:       cfg.BinlogFileDir,
		binlogger: binlogger,
	}, nil
}

func (p *pbExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	binlog := &pb.Binlog{DmlData: new(pb.DMLData)}
	for i := range sqls {
		if isDDL {
			binlog.DdlQuery = []byte(sqls[0])
		} else {
			event := args[i][0].(*pb.Event)
			binlog.DmlData.Events = append(binlog.DmlData.Events, *event)
		}
	}
	return p.saveBinlog(binlog)
}

func (p *pbExecutor) Close() error {
	return p.binlogger.Close()
}

func (p *pbExecutor) saveBinlog(binlog *pb.Binlog) error {
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	return p.binlogger.WriteTail(data)
}
