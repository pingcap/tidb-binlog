package loader

import (
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"

	"github.com/ngaut/log"
)

// DMLType represents the dml type
type DMLType int

// DMLType types
const (
	UnknownDMLType DMLType = 0
	InsertDMLType  DMLType = 1
	UpdateDMLType  DMLType = 2
	DeleteDMLType  DMLType = 3
)

// DML holds the dml info
type DML struct {
	Database string
	Table    string

	Tp DMLType
	// only set when Tp = UpdateDMLType
	OldValues map[string]interface{}
	Values    map[string]interface{}

	info *tableInfo
}

// DDL holds the ddl info
type DDL struct {
	Database string
	Table    string
	SQL      string
}

// Txn holds transaction info, an DDL or DML sequences
type Txn struct {
	DMLs []*DML
	DDL  *DDL

	// This field is used to hold arbitrary data you wish to include so it
	// will be available when receiving on the Successes channel
	Metadata interface{}
}

// AppendDML append a dml
func (t *Txn) AppendDML(dml *DML) {
	t.DMLs = append(t.DMLs, dml)
}

// NewDDLTxn return a Txn
func NewDDLTxn(db string, table string, sql string) *Txn {
	txn := new(Txn)
	txn.DDL = &DDL{
		Database: db,
		Table:    table,
		SQL:      sql,
	}

	return txn
}

func (t *Txn) String() string {
	if t.isDDL() {
		return fmt.Sprintf("{ddl: %s}", t.DDL.SQL)
	}

	return fmt.Sprintf("dml: %v", t.DMLs)
}

func (t *Txn) isDDL() bool {
	return t.DDL != nil
}

func (dml *DML) primaryKeys() []string {
	if dml.info.primaryKey == nil {
		return nil
	}

	return dml.info.primaryKey.columns
}

func (dml *DML) primaryKeyValues() []interface{} {
	names := dml.primaryKeys()

	var values []interface{}
	for _, name := range names {
		v := dml.Values[name]
		values = append(values, v)
	}

	return values
}

func (dml *DML) formatKey() string {
	return formatKey(dml.primaryKeyValues())
}

func (dml *DML) formatOldKey() string {
	return formatKey(dml.oldPrimaryKeyValues())
}

func (dml *DML) updateKey() bool {
	if len(dml.OldValues) == 0 {
		return false
	}

	values := dml.primaryKeyValues()
	oldValues := dml.oldPrimaryKeyValues()

	for i := 0; i < len(values); i++ {
		if values[i] != oldValues[i] {
			return true
		}
	}

	return false
}

func (dml *DML) String() string {
	return fmt.Sprintf("{db: %s, table: %s,tp: %v values: %d old_values: %d}",
		dml.Database, dml.Table, dml.Tp, len(dml.Values), len(dml.OldValues))
}

func (dml *DML) oldPrimaryKeyValues() []interface{} {
	if len(dml.OldValues) == 0 {
		return dml.primaryKeyValues()
	}

	names := dml.primaryKeys()

	var values []interface{}
	for _, name := range names {
		v := dml.OldValues[name]
		values = append(values, v)
	}

	return values
}

func (dml *DML) updateSQL() (sql string, args []interface{}) {
	var setNames []string
	for name, arg := range dml.Values {
		setNames = append(setNames, fmt.Sprintf("%s = ?", quoteName(name)))
		args = append(args, arg)
	}

	whereStr, whereArgs := dml.buildWhere()
	args = append(args, whereArgs...)

	sql = fmt.Sprintf("UPDATE %s SET %s WHERE %s LIMIT 1", quoteSchema(dml.Database, dml.Table), strings.Join(setNames, ","), whereStr)

	return
}

func (dml *DML) buildWhere() (str string, args []interface{}) {
	wnames, wargs := dml.whereSlice()
	for i := 0; i < len(wnames); i++ {
		if i > 0 {
			str += " AND "
		}
		if wargs[i] == nil {
			str += quoteName(wnames[i]) + " IS NULL"
		} else {
			str += quoteName(wnames[i]) + " = ? "
			args = append(args, wargs[i])
		}
	}
	return
}

func (dml *DML) whereValues(names []string) (values []interface{}) {
	valueMap := dml.Values
	if dml.Tp == UpdateDMLType {
		valueMap = dml.OldValues
	}

	for _, name := range names {
		v := valueMap[name]
		values = append(values, v)
	}
	return
}

func (dml *DML) whereSlice() (colNames []string, args []interface{}) {
	for _, index := range dml.info.uniqueKeys {
		values := dml.whereValues(index.columns)
		var i int
		for i = 0; i < len(values); i++ {
			if values[i] == nil {
				break
			}
		}
		if i == len(values) {
			return index.columns, values
		}
	}

	return dml.info.columns, dml.whereValues(dml.info.columns)

}

func (dml *DML) deleteSQL() (sql string, args []interface{}) {
	whereStr, whereArgs := dml.buildWhere()
	sql = fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT 1", quoteSchema(dml.Database, dml.Table), whereStr)
	args = whereArgs
	return
}

func (dml *DML) replaceSQL() (sql string, args []interface{}) {
	info := dml.info
	sql = fmt.Sprintf("REPLACE INTO %s(%s) VALUES(%s)", quoteSchema(dml.Database, dml.Table), buildColumnList(info.columns), holderString(len(info.columns)))
	for _, name := range info.columns {
		v := dml.Values[name]
		args = append(args, v)
	}
	return
}

func (dml *DML) insertSQL() (sql string, args []interface{}) {
	sql, args = dml.replaceSQL()
	sql = strings.Replace(sql, "REPLACE", "INSERT", 1)
	return
}

func (dml *DML) sql() (sql string, args []interface{}) {
	switch dml.Tp {
	case InsertDMLType:
		return dml.insertSQL()
	case UpdateDMLType:
		return dml.updateSQL()
	case DeleteDMLType:
		return dml.deleteSQL()
	}

	log.Debugf("dml: %+v sql: %s, args: %v", dml, sql, args)

	return
}

func formatKey(values []interface{}) string {
	builder := new(strings.Builder)
	for i, v := range values {
		if i != 0 {
			builder.WriteString("--")
		}
		builder.WriteString(fmt.Sprintf("%v", v))
	}

	return builder.String()
}

func getKey(names []string, values map[string]interface{}) string {
	builder := new(strings.Builder)
	for _, name := range names {
		v := values[name]
		if v == nil {
			continue
		}

		builder.WriteString(fmt.Sprintf("(%s: %v)", name, v))
	}

	return builder.String()
}

func getKeys(dml *DML) (keys []string) {
	info := dml.info

	tableName := quoteSchema(dml.Database, dml.Table)

	var addOldKey int
	var addNewKey int

	for _, index := range info.uniqueKeys {
		key := getKey(index.columns, dml.Values)
		if len(key) > 0 {
			addNewKey++
			keys = append(keys, key+tableName)
		}
	}

	if dml.Tp == UpdateDMLType {
		for _, index := range info.uniqueKeys {
			key := getKey(index.columns, dml.OldValues)
			if len(key) > 0 {
				addOldKey++
				keys = append(keys, key+tableName)
			}
		}
	}

	if addNewKey == 0 {
		key := getKey(info.columns, dml.Values) + tableName
		key = strconv.Itoa(int(crc32.ChecksumIEEE([]byte(key))))
		keys = append(keys, key)
	}

	if dml.Tp == UpdateDMLType && addOldKey == 0 {
		key := getKey(info.columns, dml.OldValues) + tableName
		key = strconv.Itoa(int(crc32.ChecksumIEEE([]byte(key))))
		keys = append(keys, key)
	}

	return
}
