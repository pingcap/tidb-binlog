package reparo

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	tbl "github.com/pingcap/tidb-binlog/reparo/table"
	tmysql "github.com/pingcap/tidb/mysql"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

const (
	maxRetryCount = 1000

	retryTimeout = time.Second * 1
)

func (r *Reparo) getTableFromDB(db *sql.DB, schema string, name string) (*tbl.Table, error) {
	table := &tbl.Table{}
	table.Schema = schema
	table.Name = name
	table.IndexColumns = make(map[string][]*tbl.Column)

	err := getTableColumns(r.db, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = getTableIndex(r.db, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(table.Columns) == 0 {
		return nil, errors.Errorf("invalid table %s.%s", schema, name)
	}

	return table, nil
}

func (r *Reparo) getTable(schema string, table string) (*tbl.Table, error) {
	key := fmt.Sprintf("%s.%s", schema, table)

	value, ok := r.tables[key]
	if ok {
		return value, nil
	}

	t, err := r.getTableFromDB(r.db, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	r.tables[key] = t
	return t, nil
}

func (r *Reparo) clearTables() {
	r.tables = make(map[string]*tbl.Table)
}

func getTableIndex(db *sql.DB, table *tbl.Table) error {
	if table.Schema == "" || table.Name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", table.Schema, table.Name)
	rows, err := querySQL(db, query)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Show an example.
	/*
		mysql> show index from test.t;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| t     |          0 | PRIMARY  |            1 | a           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | PRIMARY  |            2 | b           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | ucd      |            1 | c           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		| t     |          0 | ucd      |            2 | d           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	var columns = make(map[string][]string)
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return errors.Trace(err)
		}

		nonUnique := string(data[1])
		if nonUnique == "0" {
			keyName := strings.ToLower(string(data[2]))
			columns[keyName] = append(columns[keyName], string(data[4]))
		}
	}
	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	table.IndexColumns = findColumns(table.Columns, columns)
	return nil
}

func findColumns(columns []*tbl.Column, indexColumns map[string][]string) map[string][]*tbl.Column {
	result := make(map[string][]*tbl.Column)

	for keyName, indexCols := range indexColumns {
		cols := make([]*tbl.Column, 0, len(indexCols))
		for _, name := range indexCols {
			column := findColumn(columns, name)
			if column != nil {
				cols = append(cols, column)
			}
		}
		result[keyName] = cols
	}

	return result
}

func findColumn(columns []*tbl.Column, indexColumn string) *tbl.Column {
	for _, column := range columns {
		if column.Name == indexColumn {
			return column
		}
	}

	return nil
}

func getTableColumns(db *sql.DB, table *tbl.Table) error {
	if table.Schema == "" || table.Name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", table.Schema, table.Name)
	rows, err := querySQL(db, query)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Show an example.
	/*
	   mysql> show columns from test.t;
	   +-------+---------+------+-----+---------+-------+
	   | Field | Type    | Null | Key | Default | Extra |
	   +-------+---------+------+-----+---------+-------+
	   | a     | int(11) | NO   | PRI | NULL    |       |
	   | b     | int(11) | NO   | PRI | NULL    |       |
	   | c     | int(11) | YES  | MUL | NULL    |       |
	   | d     | int(11) | YES  |     | NULL    |       |
	   +-------+---------+------+-----+---------+-------+
	*/

	idx := 0
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return errors.Trace(err)
		}

		column := &tbl.Column{}
		column.Idx = idx
		column.Name = string(data[0])

		if strings.ToLower(string(data[2])) == "no" {
			column.NotNull = true
		}

		// Check whether column has unsigned flag.
		if strings.Contains(strings.ToLower(string(data[1])), "unsigned") {
			column.Unsigned = true
		}

		table.Columns = append(table.Columns, column)
		idx++
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	return nil
}

func querySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	for i := 0; i < maxRetryCount; i++ {
		if i > 0 {
			log.Warnf("sql query retry %d: %s", i, query)
			time.Sleep(retryTimeout)
		}

		log.Debugf("[query][sql]%s", query)

		rows, err = db.Query(query)
		if err != nil {
			if !isRetryableError(err) {
				return rows, errors.Trace(err)
			}
			log.Warnf("[query][sql]%s[error]%v", query, err)
			continue
		}

		return rows, nil
	}

	if err != nil {
		log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	return nil, errors.Errorf("query sql[%s] failed", query)
}

func isRetryableError(err error) bool {
	err = errors.Cause(err)
	if err == driver.ErrBadConn || err == gmysql.ErrBadConn {
		return true
	}

	if nerr, ok := err.(net.Error); ok {
		if nerr.Timeout() {
			return true
		}
	}

	mysqlErr, ok := err.(*mysql.MySQLError)
	if ok {
		switch mysqlErr.Number {
		// ER_LOCK_DEADLOCK can retry to commit while meet deadlock
		case tmysql.ErrUnknown, gmysql.ER_LOCK_DEADLOCK, tmysql.ErrPDServerTimeout, tmysql.ErrTiKVServerTimeout, tmysql.ErrTiKVServerBusy, tmysql.ErrResolveLockTimeout, tmysql.ErrRegionUnavailable:
			return true
		default:
			return false
		}
	}

	return true
}
