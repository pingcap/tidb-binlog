package executor

import (
	"github.com/juju/errors"
	bf "github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb-binlog/pump"
)

type pbExecutor struct {
	dir       string
	binlogger pump.Binlogger
}

func newPB(cfg *DBConfig) (Executor, error) {
	var (
		binlogger  pump.Binlogger
		err        error
		needCreate = true
		dirPath    = cfg.BinlogFileDir
	)

	if pump.Exist(dirPath) {
		// ignore file not found error
		binlogNames, _ := bf.ReadBinlogNames(dirPath)
		if len(binlogNames) > 0 {
			needCreate = false
		}
	}

	codec := compress.ToCompressionCodec(cfg.Compression)
	if needCreate {
		binlogger, err = pump.CreateBinlogger(dirPath, codec)
	} else {
		binlogger, err = pump.OpenBinlogger(dirPath, codec)
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

	_, err = p.binlogger.WriteTail(data)
	return errors.Trace(err)
}
