package loader

import (
	gosql "database/sql"
	"fmt"
	"hash/crc32"
	"strings"

	"github.com/pingcap/errors"
)

type tableInfo struct {
	columns    []string
	primaryKey *indexInfo
	// include primary key if have
	uniqueKeys []indexInfo
}

type indexInfo struct {
	name    string
	columns []string
}

// getTableInfo return the table info
// https://dev.mysql.com/doc/refman/8.0/en/show-columns.html
// https://dev.mysql.com/doc/refman/8.0/en/show-index.html
func getTableInfo(db *gosql.DB, schema string, table string) (info *tableInfo, err error) {
	info = new(tableInfo)

	// get column info
	//
	// mysql> SHOW COLUMNS FROM City;
	// +-------------+----------+------+-----+---------+----------------+
	// | Field       | Type     | Null | Key | Default | Extra          |
	// +-------------+----------+------+-----+---------+----------------+
	// | ID          | int(11)  | NO   | PRI | NULL    | auto_increment |
	// | Name        | char(35) | NO   |     |         |                |
	// | CountryCode | char(3)  | NO   | MUL |         |                |
	// | District    | char(20) | NO   |     |         |                |
	// | Population  | int(11)  | NO   |     | 0       |                |
	// +-------------+----------+------+-----+---------+----------------+
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
	//
	// mysql> show index from a;
	// +-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	// | Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
	// +-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	// | a     |          0 | PRIMARY  |            1 | id          | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
	// | a     |          1 | a1       |            1 | a1          | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
	// +-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
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
		var seqInIndex int // start at 1
		cols[1] = &nonUnique
		cols[2] = &keyName
		cols[3] = &seqInIndex
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
		// set columns in the order by Seq_In_Index
		for i = 0; i < len(info.uniqueKeys); i++ {
			if info.uniqueKeys[i].name == keyName {
				// expand columns size
				for seqInIndex > len(info.uniqueKeys[i].columns) {
					info.uniqueKeys[i].columns = append(info.uniqueKeys[i].columns, "")
				}
				info.uniqueKeys[i].columns[seqInIndex-1] = columnName
				break
			}
		}
		if i == len(info.uniqueKeys) {
			info.uniqueKeys = append(info.uniqueKeys, indexInfo{keyName, []string{columnName}})
		}

	}

	// put primary key at first place
	// and set primaryKey
	for i := 0; i < len(info.uniqueKeys); i++ {
		if info.uniqueKeys[i].name == "PRIMARY" {
			info.uniqueKeys[i], info.uniqueKeys[0] = info.uniqueKeys[0], info.uniqueKeys[i]
			info.primaryKey = &info.uniqueKeys[0]
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
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4,utf8&interpolateParams=true&readTimeout=1m&multiStatements=true", user, password, host, port)

	db, err = gosql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return
}

func quoteSchema(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

func quoteName(name string) string {
	return "`" + escapeName(name) + "`"
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
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
		b.WriteString(quoteName(name))

	}

	return b.String()
}
