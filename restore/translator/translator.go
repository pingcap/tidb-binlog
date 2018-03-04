package translator

import (
	"github.com/ngaut/log"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	tbl "github.com/pingcap/tidb-binlog/restore/table"
)

// TranslateResult contains translation result.
type TranslateResult struct {
	SQL  string
	Keys []string
	Args []interface{}
}

// Translator is the interface for translating binlog to target sqls
type Translator interface {
	// GenInsert generates the insert sqls
	TransInsert(binlog *pb.Binlog, event *pb.Event, row [][]byte, table *tbl.Table) (*TranslateResult, error)

	// GenUpdate generates the update
	TransUpdate(binlog *pb.Binlog, event *pb.Event, row [][]byte, table *tbl.Table) (*TranslateResult, error)

	// GenUpdateSafeMode generate delete and insert  from update sqls
	TransUpdateSafeMode(binlog *pb.Binlog, event *pb.Event, row [][]byte, table *tbl.Table) (*TranslateResult, error)

	// GenDelete generates the delete sqls by cols values
	TransDelete(binlog *pb.Binlog, event *pb.Event, row [][]byte, table *tbl.Table) (*TranslateResult, error)

	// GenDDL generates the ddl sql by query string
	TransDDL(binlog *pb.Binlog) (*TranslateResult, error)
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
