package translator

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
)

// OpType represents type of the operation
type OpType byte

const (
	// DML is the constant OpType for delete operation
	DML = iota + 1
	// DDL is the constant OpType for ddl operation
	DDL
	// FLUSH is for wait all operation executed
	FLUSH
)

var providers = make(map[string]SQLTranslator)

// SQLTranslator is the interface for translating TiDB binlog to target sqls
type SQLTranslator interface {
	// Config set the configuration
	Config(bool, bool)

	// GenInsertSQLs generates the insert sqls
	GenInsertSQLs(string, *model.TableInfo, [][]byte) ([]string, [][]string, [][]interface{}, error)

	// GenUpdateSQLs generates the update sqls
	GenUpdateSQLs(string, *model.TableInfo, [][]byte) ([]string, [][]string, [][]interface{}, error)

	// GenDeleteSQLs generates the delete sqls by cols values
	GenDeleteSQLs(string, *model.TableInfo, [][]byte) ([]string, [][]string, [][]interface{}, error)

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
