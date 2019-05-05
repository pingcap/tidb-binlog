package executor

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	tb "github.com/pingcap/tipb/go-binlog"
)

type pbExecutor struct {
	dir       string
	binlogger binlogfile.Binlogger
	*baseError
}

func newPB(cfg *DBConfig) (Executor, error) {
	binlogger, err := binlogfile.OpenBinlogger(cfg.BinlogFileDir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &pbExecutor{
		dir:       cfg.BinlogFileDir,
		binlogger: binlogger,
		baseError: newBaseError(),
	}, nil
}

func (p *pbExecutor) Execute(sqls []string, args [][]interface{}, commitTSs []int64, isDDL bool) error {
	if len(sqls) == 0 {
		return nil
	}
	binlog := &pb.Binlog{CommitTs: commitTSs[0]}
	if isDDL {
		binlog.Tp = pb.BinlogType_DDL
		binlog.DdlQuery = []byte(sqls[0])
		return p.saveBinlog(binlog)
	}

	binlog.Tp = pb.BinlogType_DML
	binlog.DmlData = new(pb.DMLData)
	for i := range sqls {
		// event can be only pb.Event, otherwise need to panic
		event := args[i][0].(*pb.Event)
		binlog.DmlData.Events = append(binlog.DmlData.Events, *event)
	}

	return errors.Trace(p.saveBinlog(binlog))
}

func (p *pbExecutor) Close() error {
	return p.binlogger.Close()
}

func (p *pbExecutor) saveBinlog(binlog *pb.Binlog) error {
	data, err := binlog.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	_, err = p.binlogger.WriteTail(&tb.Entity{Payload: data})
	return errors.Trace(err)
}