package translator

import (
	"database/sql"

	"github.com/ngaut/log"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
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
	TransInsert(event *pb.Event, row [][]byte) (*TranslateResult, error)

	// GenUpdate generates the update
	TransUpdate(event *pb.Event, row [][]byte) (*TranslateResult, error)

	// GenDelete generates the delete sqls by cols values
	TransDelete(event *pb.Event, row [][]byte) (*TranslateResult, error)

	// GenDDL generates the ddl sql by query string
	TransDDL(ddl string) (*TranslateResult, error)
}

// New creates a new Translator based on name.
func New(name string, safeMode bool, db *sql.DB) Translator {
	switch name {
	case "print":
		return newPrintTranslator()
	case "mysql":
		return newMysqlTranslator(db)
	}
	log.Infof("name %s not found, use print translator by default", name)
	return newPrintTranslator()
}
