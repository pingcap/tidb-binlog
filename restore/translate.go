package restore

import (
	"github.com/juju/errors"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb-binlog/restore/translator"
)

// Translate translates payload to SQL.
func (r *Restore) Translate(binlog *pb.Binlog) (results []*translator.TranslateResult, isDDL bool, err error) {
	if !isAcceptableBinlog(binlog, r.cfg.StartTSO, r.cfg.StopTSO) {
		return
	}

	switch binlog.Tp {
	case pb.BinlogType_DML:
		results, err = r.translateDML(binlog)
		return results, false, errors.Trace(err)
	case pb.BinlogType_DDL:
		results, err = r.translateDDL(binlog)
		return results, true, errors.Trace(err)
	default:
		panic("unreachable")
	}
}

func (r *Restore) translateDML(binlog *pb.Binlog) ([]*translator.TranslateResult, error) {
	dml := binlog.DmlData
	if dml == nil {
		return nil, errors.New("dml binlog's data can't be empty")
	}
	results := make([]*translator.TranslateResult, 0, len(dml.Events))

	var (
		result *translator.TranslateResult
		err    error
	)

	for _, event := range dml.Events {
		if r.SkipBySchemaAndTable(event.GetSchemaName(), event.GetTableName()) {
			continue
		}

		table, err := r.getTable(event.GetSchemaName(), event.GetTableName())
		if err != nil {
			return nil, errors.Trace(err)
		}

		e := &event
		tp := e.GetTp()
		row := e.GetRow()
		switch tp {
		case pb.EventType_Insert:
			result, err = r.translator.TransInsert(binlog, e, row, table)
		case pb.EventType_Update:
			result, err = r.translator.TransUpdate(binlog, e, row, table)
		case pb.EventType_Delete:
			result, err = r.translator.TransDelete(binlog, e, row, table)
		default:
			panic("unreachable")
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		results = append(results, result)
	}

	return results, nil
}

func (r *Restore) translateDDL(binlog *pb.Binlog) ([]*translator.TranslateResult, error) {
	_, table, err := parseDDL(string(binlog.GetDdlQuery()))
	if err != nil {
		return nil, errors.Trace(err)
	}

	if r.SkipBySchemaAndTable(table.Schema, table.Name) {
		return nil, nil
	}

	result, err := r.translator.TransDDL(binlog)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r.clearTables()
	return []*translator.TranslateResult{result}, nil
}
