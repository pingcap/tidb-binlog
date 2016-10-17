package translator

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
)

// OpType represents type of the operation
type OpType byte

const (
	// Insert defines a OpType constant for insert operation.
	Insert = iota + 1
	// Update defines a OpType constant for update operation.
	Update
	// Del defines a OpType constant for delete operation.
	Del
	// DelByID defines a OpType constant for delete operation.
	DelByID
	// DelByPK defines a OpType constant for delete operation.
	DelByPK
	// DelByCol defines a OpType constant for delete operation.
	DelByCol
	// DDL defines a OpType constant for ddl operation.
	DDL
)

var providers = make(map[string]SQLTranslator)

// SQLTranslator is the interface for translating TiDB binlog to target sqls
type SQLTranslator interface {
	// GenInsertSQLs generates the insert sqls
	GenInsertSQLs(string, *model.TableInfo, [][]byte) ([]string, [][]interface{}, error)

	// GenUpdateSQLs generates the update sqls
	GenUpdateSQLs(string, *model.TableInfo, [][]byte) ([]string, [][]interface{}, error)

	// GenDeleteSQLsByID generates the delete by ID sqls
	GenDeleteSQLsByID(string, *model.TableInfo, []int64) ([]string, [][]interface{}, error)

	// GenDeleteSQLs generates the delete sqls by cols values
	GenDeleteSQLs(string, *model.TableInfo, OpType, [][]byte) ([]string, [][]interface{}, error)

	// GenDDLSQL generates the ddl sql by query string
	GenDDLSQL(string, string) (string, error)
}

// Register registers the SQLTranslator into the providers
func Register(name string, provider SQLTranslator) {
	if provider == nil {
		log.Fatal("SQLTranslator: Register provide is nil")
	}

	if _, dup := providers[name]; dup {
		log.Fatal("SQLTranslator: Register called twice for provider " + name)
	}

	providers[name] = provider
}

// Unregister unregisters the SQLTranslator by name
func Unregister(name string) {
	delete(providers, name)
}

// New returns the SQLTranslator by given providerName
func New(providerName string) (SQLTranslator, error) {
	translator, ok := providers[providerName]
	if !ok {
		return nil, errors.Errorf("SQLTranslator: unknown provider %q", providerName)
	}

	return translator, nil
}
