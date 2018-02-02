package translator

import (
	"github.com/juju/errors"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

// Translator is the interface for translating binlog to target sqls
type Translator interface {
	// GenInsert generates the insert sqls
	TransInsert(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error)

	// GenUpdate generates the update
	TransUpdate(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error)

	// GenUpdateSafeMode generate delete and insert  from update sqls
	TransUpdateSafeMode(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error)

	// GenDelete generates the delete sqls by cols values
	TransDelete(binlog *pb.Binlog, event *pb.Event, row [][]byte) (string, []interface{}, error)

	// GenDDL generates the ddl sql by query string
	TransDDL(binlog *pb.Binlog) (string, []interface{}, error)
}

func New(name string, safeMode bool) Translator {
	switch name {
	case "print":
		return newPrintTranslator()

	case "pb":
		return newPBTranslator()
	}
	return newPrintTranslator()
}

func Translate(payload []byte, translator Translator) ([]string, [][]interface{}, error) {
	binlog := &pb.Binlog{}
	err := binlog.Unmarshal(payload)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	switch binlog.Tp {
	case pb.BinlogType_DML:
		return translateDML(binlog, translator)
	case pb.BinlogType_DDL:
		return translateDDL(binlog, translator)
	default:
		panic("unreachable")
	}
}

func translateDML(binlog *pb.Binlog, translator Translator) ([]string, [][]interface{}, error) {
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
		e := &event
		tp := e.GetTp()
		row := e.GetRow()
		switch tp {
		case pb.EventType_Insert:
			sql, arg, err = translator.TransInsert(binlog, e, row)
		case pb.EventType_Update:
			sql, arg, err = translator.TransUpdate(binlog, e, row)
		case pb.EventType_Delete:
			sql, arg, err = translator.TransDelete(binlog, e, row)
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

func translateDDL(binlog *pb.Binlog, translator Translator) ([]string, [][]interface{}, error) {
	ddl, args, err := translator.TransDDL(binlog)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return []string{ddl}, [][]interface{}{args}, nil
}
