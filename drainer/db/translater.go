package db

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
)

type opType byte

const (
	insert = iota + 1
	update
	del
	delByID
	delByPK
	delByCol
	ddl
)

var providers = make(map[string]SQLTranslator)

// SQLTranslator is the interface for translating TiDB binlog to target sqls
type SQLTranslator interface {
	// GenInsertSQLs generates the insert SQLs
	GenInsertSQLs(string, *model.TableInfo, [][]byte) ([]string, [][]interface{}, error)

	// GenUpdateSQLs generates the update SQLs
	GenUpdateSQLs(string, *model.TableInfo, [][]byte) ([]string, [][]interface{}, error)

	// GenDeleteSQLsByID generates the delete by ID SQLs
	GenDeleteSQLsByID(string, *model.TableInfo, []int64) ([]string, [][]interface{}, error)

	// GenDeleteSQLs generates the delete SQLs by cols values
	GenDeleteSQLs(string, *model.TableInfo, opType, [][]byte) ([]string, [][]interface{}, error)

	// IsDDLSQL checks whether the query string is DDL SQL
	IsDDLSQL(string) (bool, error)

	// GenDDLSQL generates the ddl SQL by  query string
	GenDDLSQL(string, string) (string, error)
}

// Register registers the SQLTranslator into the providers
func Register(name string, provider SQLTranslator) {
	if provider == nil {
		panic("SQLTranslator: Register provide is nil")
	}

	if _, dup := providers[name]; dup {
		panic("SQLTranslator: Register called twice for provider " + name)
	}

	providers[name] = provider
}

// Unregister unregisters the SQLTranslator by name
func Unregister(name string) {
	delete(providers, name)
}

// Manager is the SQLTranslator factory
type Manager struct {
	translator SQLTranslator
}

// NewManager returns the Manager by given providerName
func NewManager(providerName string) (*Manager, error) {
	translator, ok := providers[providerName]
	if !ok {
		return nil, errors.Errorf("translator: unknown provider %q", providerName)
	}

	return &Manager{translator: translator}, nil
}

// GenInsertSQLs wraps the GenInsertSQLs's GenInsertSQLs method
func (m *Manager) GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]interface{}, error) {
	return m.translator.GenInsertSQLs(schema, table, rows)
}

// GenUpdateSQLs wraps the SQLTranslator's GenUpdateSQLs method
func (m *Manager) GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]interface{}, error) {
	return m.translator.GenUpdateSQLs(schema, table, rows)
}

// GenDeleteSQLsByID wraps the GenInsertSQLs's GenDeleteSQLsByID method
func (m *Manager) GenDeleteSQLsByID(schema string, table *model.TableInfo, rows []int64) ([]string, [][]interface{}, error) {
	return m.translator.GenDeleteSQLsByID(schema, table, rows)
}

// GenDeleteSQLs wraps the GenInsertSQLs's GenDeleteSQLs method
func (m *Manager) GenDeleteSQLs(schema string, table *model.TableInfo, op opType, rows [][]byte) ([]string, [][]interface{}, error) {
	return m.translator.GenDeleteSQLs(schema, table, op, rows)
}

// IsDDLSQL wraps the GenInsertSQLs's IsDDLSQL method
func (m *Manager) IsDDLSQL(sql string) (bool, error) {
	return m.translator.IsDDLSQL(sql)
}

// GenDDLSQL wraps the GenInsertSQLs's GenDDLSQL method
func (m *Manager) GenDDLSQL(sql string, schema string) (string, error) {
	return m.translator.GenDDLSQL(sql, schema)
}
