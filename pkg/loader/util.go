package loader

import (
	gosql "database/sql"
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/juju/errors"
)

type tableInfo struct {
	columns []string
	indexs  []indexType
}

type indexType struct {
	name    string
	columns []string
}

// getTableInfo return the table info
// https://dev.mysql.com/doc/refman/8.0/en/show-columns.html
// https://dev.mysql.com/doc/refman/8.0/en/show-index.html
func getTableInfo(db *gosql.DB, schema string, table string) (info *tableInfo, err error) {
	info = new(tableInfo)

	// get column info
	sql := fmt.Sprintf("show columns from %s", quoteSchema(schema, table))
	rows, err := db.Query(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer rows.Close()

	for rows.Next() {
		cols := make([]interface{}, 6)
		var name string
		cols[0] = &name
		for i := 1; i < len(cols); i++ {
			cols[i] = &gosql.RawBytes{}
		}

		err = rows.Scan(cols...)
		if err != nil {
			return nil, errors.Trace(err)
		}

		info.columns = append(info.columns, name)
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	// get index info
	sql = fmt.Sprintf("show index from %s", quoteSchema(schema, table))
	rows, err = db.Query(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer rows.Close()

	// get pk and uk
	// key for PRIMARY or other index name
	for rows.Next() {
		cols := make([]interface{}, 13)
		for i := 0; i < len(cols); i++ {
			cols[i] = &gosql.RawBytes{}
		}

		var nonUnique int
		var keyName string
		var columnName string
		cols[1] = &nonUnique
		cols[2] = &keyName
		cols[4] = &columnName

		err = rows.Scan(cols...)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// log.Debug(nonUnique, keyName, columnName)
		if nonUnique == 1 {
			continue
		}

		var i int
		for i = 0; i < len(info.indexs); i++ {
			if info.indexs[i].name == keyName {
				info.indexs[i].columns = append(info.indexs[i].columns, columnName)
				break
			}
		}
		if i == len(info.indexs) {
			info.indexs = append(info.indexs, indexType{keyName, []string{columnName}})
		}

	}

	// put primary key at first place
	for i := 0; i < len(info.indexs); i++ {
		if info.indexs[i].name == "PRIMARY" {
			info.indexs[i], info.indexs[0] = info.indexs[0], info.indexs[i]
			break
		}
	}

	if err = rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	return
}

// CreateDB return sql.DB
func CreateDB(user string, password string, host string, port int) (db *gosql.DB, err error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8&interpolateParams=true&readTimeout=1m&multiStatements=true", user, password, host, port)

	db, err = gosql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return
}

func quoteSchema(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", schema, table)
}

func quoteName(name string) string {
	return "`" + name + "`"
}

func holderString(n int) string {
	builder := new(strings.Builder)
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	return builder.String()
}

func genHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func splitDMLs(dmls []*DML, size int) (res [][]*DML) {
	for i := 0; i < len(dmls); i += size {
		end := i + size
		if end > len(dmls) {
			end = len(dmls)
		}

		res = append(res, dmls[i:end])
	}
	return
}

func buildColumnList(names []string) string {
	b := new(strings.Builder)
	for i, name := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("`" + name + "`")
	}

	return b.String()
}
