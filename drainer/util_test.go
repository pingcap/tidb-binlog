package drainer

import (
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
)

type mockSQLTx struct {
	mc *mockSQLConn
}

func (tx *mockSQLTx) Commit() error {
	if isErr, ok := isErrMocks[commitTxErr]; ok && isErr {
		return errMock
	}

	return nil
}

func (tx *mockSQLTx) Rollback() (err error) {
	if isErr, ok := isErrMocks[rollbackTxErr]; ok && isErr {
		return errMock
	}

	return nil
}

type mockSQLConn struct {
}

func (conn *mockSQLConn) Prepare(query string) (driver.Stmt, error) {
	if isErr, ok := isErrMocks[prepareErr]; ok && isErr {
		return nil, errMock
	}

	return nil, nil
}

func (conn *mockSQLConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if isErr, ok := isErrMocks[queryErr]; ok && isErr {
		return nil, errMock
	}

	return nil, nil
}

func (conn *mockSQLConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if isErr, ok := isErrMocks[execErr]; ok && isErr {
		return nil, errMock
	}

	return nil, nil
}

func (conn *mockSQLConn) Begin() (driver.Tx, error) {
	if isErr, ok := isErrMocks[beginTxErr]; ok && isErr {
		return nil, errMock
	}

	return &mockSQLTx{}, nil
}

func (conn *mockSQLConn) Close() error {
	if isErr, ok := isErrMocks[closeConnErr]; ok && isErr {
		return errMock
	}

	return nil
}

type mockDB struct {
}

func (db *mockDB) Open(dsn string) (driver.Conn, error) {
	return &mockSQLConn{}, nil
}

func (t *testDrainerSuite) TestExecuteSqls(c *C) {
	// open mock db err
	_, err := openDB("root", "", "127.0.0.1", 3306, "mockTestSQL")
	c.Assert(err, NotNil)

	// test open db
	sql.Register("mockTestSQL", &mockDB{})
	db, err := openDB("root", "", "127.0.0.1", 3306, "mockTestSQL")
	c.Assert(err, IsNil)

	// test execute empty sql
	err = executeSQLs(db, nil, nil, false)
	c.Assert(err, IsNil)

	// test execute sql
	err = executeSQLs(db, []string{"test sql"}, [][]interface{}{{}}, false)
	c.Assert(err, IsNil)

	// test retry sql
	maxRetryCount = 2
	retryTimeout = 0
	isErrMocks[beginTxErr] = true
	err = executeSQLs(db, []string{"test sql"}, [][]interface{}{{}}, true)
	c.Assert(err, NotNil)

	// test tx exec error
	isErrMocks[beginTxErr] = false
	isErrMocks[execErr] = true
	isErrMocks[rollbackTxErr] = true
	err = executeSQLs(db, []string{"test sql"}, [][]interface{}{{}}, false)
	c.Assert(err, NotNil)

	// test tx commit err
	isErrMocks[execErr] = false
	isErrMocks[rollbackTxErr] = false
	isErrMocks[commitTxErr] = true
	err = executeSQLs(db, []string{"test sql"}, [][]interface{}{{}}, false)
	c.Assert(err, NotNil)
	isErrMocks[commitTxErr] = false

	// test close db
	err = closeDB(db)
	c.Assert(err, IsNil)
}

func (t *testDrainerSuite) TestIgnoreDDLError(c *C) {
	// test non-mysqltype error
	err := errors.New("test")
	ok := ignoreDDLError(err)
	c.Assert(ok, IsFalse)
	// test ignore error
	err1 := &mysql.MySQLError{
		Number:  1054,
		Message: "test",
	}
	ok = ignoreDDLError(err1)
	c.Assert(ok, IsTrue)
	// test non-ignore error
	err2 := &mysql.MySQLError{
		Number:  1052,
		Message: "test",
	}
	ok = ignoreDDLError(err2)
	c.Assert(ok, IsFalse)

}

func (t *testDrainerSuite) TestFormatIgnoreSchemas(c *C) {
	ignoreDBs := formatIgnoreSchemas("test,test1")
	ignoreList := make(map[string]struct{})
	ignoreList["test"] = struct{}{}
	ignoreList["test1"] = struct{}{}
	c.Assert(ignoreDBs, DeepEquals, ignoreList)
}

func (t *testDrainerSuite) TestFilterIgnoreSchema(c *C) {
	ignoreList := make(map[string]struct{})
	ignoreList["test"] = struct{}{}
	ignoreList["test1"] = struct{}{}

	c.Assert(filterIgnoreSchema(&model.DBInfo{Name: model.NewCIStr("test1")}, ignoreList), IsTrue)
	c.Assert(filterIgnoreSchema(&model.DBInfo{Name: model.NewCIStr("test2")}, ignoreList), IsFalse)
}
