package translator

import (
	"github.com/ngaut/log"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb-binlog/reparo/common"
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
	TransInsert(event *pb.Event) (*TranslateResult, error)

	// GenUpdate generates the update
	TransUpdate(event *pb.Event) (*TranslateResult, error)

	// GenDelete generates the delete sqls by cols values
	TransDelete(event *pb.Event) (*TranslateResult, error)

	// GenDDL generates the ddl sql by query string
	TransDDL(ddl string, table common.Table) (*TranslateResult, error)

	// Close closes the Translator.
	Close() error
}

// New creates a new Translator based on name.
func New(name string, safeMode bool, cfg *common.DBConfig) (Translator, error) {
	switch name {
	case "print":
		return newPrintTranslator()
	case "mysql":
		return newMysqlTranslator(cfg)
	}
	log.Infof("name %s not found, use print translator by default", name)
	return newPrintTranslator()
}
