package repora

import (
	"github.com/juju/errors"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// Translate translates payload to SQL.
func (r *Reparo) Translate(binlog *pb.Binlog) (sqls []string, args [][]interface{}, isDDL bool, err error) {
	if !isAcceptableBinlog(binlog, r.cfg.StartTSO, r.cfg.StopTSO) {
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

func (r *Reparo) translateDML(binlog *pb.Binlog) ([]string, [][]interface{}, error) {
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

func (r *Reparo) translateDDL(binlog *pb.Binlog) ([]string, [][]interface{}, error) {
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
