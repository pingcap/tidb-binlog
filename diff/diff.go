package diff

import (
	"bytes"
	"database/sql"
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// Diff contains two sql DB, used for comparing.
type Diff struct {
	db1 *sql.DB
	db2 *sql.DB
}

func New(db1, db2 *sql.DB) *Diff {
	return &Diff{
		db1: db1,
		db2: db2,
	}
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal() (eq bool, err error) {
	tbls1, err := getTables(df.db1)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	tbls2, err := getTables(df.db2)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	eq = equalStrings(tbls1, tbls2)
	if !eq {
		log.Info("show tables get different table.")
		return false, nil
	}

	for _, tblName := range tbls1 {
		eq, err = df.EqualIndex(tblName)
		if err != nil {
			err = errors.Trace(err)
			return
		}
		if !eq {
			log.Infof("table have different index: %s\n", tblName)
			return
		}

		eq, err = df.EqualTable(tblName)
		if err != nil || !eq {
			err = errors.Trace(err)
			return
		}
	}

	return
}

// EqualTable tests whether two database table have same data and schema.
func (df *Diff) EqualTable(tblName string) (bool, error) {
	eq, err := df.equalCreateTable(tblName)
	if err != nil {
		return eq, errors.Trace(err)
	}
	if !eq {
		log.Infof("table have different schema: %s\n", tblName)
		return eq, err
	}

	eq, err = df.equalTableData(tblName)
	if err != nil {
		return eq, errors.Trace(err)
	}
	if !eq {
		log.Infof("table data different: %s\n", tblName)
	}
	return eq, err
}

// EqualIndex tests whether two database index are same.
func (df *Diff) EqualIndex(tblName string) (bool, error) {
	index1, err := getTableIndex(df.db1, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}
	index2, err := getTableIndex(df.db2, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}
	eq, err := equalRows(index1, index2)
	if err != nil {
		return eq, errors.Trace(err)
	}
	return true, nil
}

func (df *Diff) equalCreateTable(tblName string) (bool, error) {
	table1, err1 := getCreateTable(df.db1, tblName)
	table2, err2 := getCreateTable(df.db2, tblName)

	_, notfound1 := err1.(NotFoundError)
	_, notfound2 := err2.(NotFoundError)
	switch {
	// table not exist should not be handled as error
	case notfound1 && notfound2:
		return true, nil
	case err1 != nil:
		return false, errors.Trace(err1)
	case err2 != nil:
		return false, errors.Trace(err2)
	}

	return table1 == table2, nil
}

func (df *Diff) equalTableData(tblName string) (bool, error) {
	rows1, err := getTableRows(df.db1, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}

	rows2, err := getTableRows(df.db2, tblName)
	if err != nil {
		return false, errors.Trace(err)
	}

	return equalRows(rows1, rows2)
}

func equalRows(rows1, rows2 *sql.Rows) (bool, error) {
	cols1, err := rows1.Columns()
	if err != nil {
		return false, errors.Trace(err)
	}
	cols2, err := rows2.Columns()
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(cols1) != len(cols2) {
		return false, nil
	}

	for rows1.Next() {
		if !rows2.Next() {
			// rows2 count less than rows1
			log.Info("rows count different")
			return false, nil
		}

		eq, err := equalOneRow(rows1, rows2, len(cols1))
		if err != nil || !eq {
			return eq, errors.Trace(err)
		}
	}
	if rows2.Next() {
		// rows1 count less than rows2
		log.Info("rows count different")
		return false, nil
	}
	return true, nil
}

func equalOneRow(rows1, rows2 *sql.Rows, n int) (bool, error) {
	data1 := make([]sql.RawBytes, n)
	data2 := make([]sql.RawBytes, n)

	args := make([]interface{}, n)

	for i := 0; i < len(args); i++ {
		args[i] = &data1[i]
	}
	err := rows1.Scan(args...)
	if err != nil {
		return false, errors.Trace(err)
	}

	for i := 0; i < len(args); i++ {
		args[i] = &data2[i]
	}
	err = rows2.Scan(args...)
	if err != nil {
		return false, errors.Trace(err)
	}

	for i := 0; i < len(data1); i++ {
		if !bytes.Equal(data1[i], data2[i]) {
			log.Infof("row different, one is %#v, the other is %#v", data1, data2)
			return false, nil
		}
	}

	return true, nil
}

func getTableRows(db *sql.DB, tblName string) (*sql.Rows, error) {
	descs, err := getTableSchema(db, tblName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pk1 := orderbyKey(descs)

	// TODO select all data out may OOM if table is huge
	rows, err := querySQL(db, fmt.Sprintf("select * from %s order by %s", tblName, pk1))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func getTableIndex(db *sql.DB, tblName string) (*sql.Rows, error) {
	rows, err := querySQL(db, fmt.Sprintf("show index from %s;", tblName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func getTables(db *sql.DB) ([]string, error) {
	rs, err := querySQL(db, "show tables;")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rs.Close()

	var tbls []string
	for rs.Next() {
		var name string
		err := rs.Scan(&name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tbls = append(tbls, name)
	}
	return tbls, nil
}

func getCreateTable(db *sql.DB, tn string) (string, error) {
	stmt := fmt.Sprintf("show create table %s;", tn)
	rs, err := querySQL(db, stmt)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rs.Close()

	if rs.Next() {
		var (
			name string
			cs   string
		)
		err := rs.Scan(&name, &cs)
		return cs, errors.Trace(err)
	}
	return "", errTableNotExist
}

type describeTable struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default interface{}
	Extra   interface{}
}

func getTableSchema(db *sql.DB, tblName string) ([]describeTable, error) {
	stmt := fmt.Sprintf("describe %s;", tblName)
	rows, err := querySQL(db, stmt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var descs []describeTable
	for rows.Next() {
		var desc describeTable
		err := rows.Scan(&desc.Field, &desc.Type, &desc.Null, &desc.Key, &desc.Default, &desc.Extra)
		if err != nil {
			return nil, errors.Trace(err)
		}
		descs = append(descs, desc)
	}
	return descs, err
}

func orderbyKey(descs []describeTable) string {
	// TODO can't get the real primary key
	var buf bytes.Buffer
	firstTime := true
	for _, desc := range descs {
		if desc.Key == "PRI" {
			if firstTime {
				fmt.Fprintf(&buf, "%s", desc.Field)
				firstTime = false
			} else {
				fmt.Fprintf(&buf, ",%s", desc.Field)
			}
		}
	}
	if buf.Len() == 0 {
		// if no primary key found, use the a field as order by key
		// when descs is empty, panic is the right behavior
		return descs[0].Field
	}
	return buf.String()
}

func querySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	log.Debugf("[query][sql]%s", query)

	rows, err = db.Query(query)

	if err != nil {
		log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}
	return rows, nil

}

func equalStrings(str1, str2 []string) bool {
	if len(str1) != len(str2) {
		return false
	}
	for i := 0; i < len(str1); i++ {
		if str1[i] != str2[i] {
			return false
		}
	}
	return true
}
