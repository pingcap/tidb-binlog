package translator

import (
	"github.com/lunny/log"
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

// New creates a new Translator based on name.
func New(name string, safeMode bool) Translator {
	switch name {
	case "print":
		return newPrintTranslator()
	case "mysql", "tidb":
		return newMysqlTranslator()
	}
	log.Infof("name %s not found, use print translator by default", name)
	return newPrintTranslator()
}
