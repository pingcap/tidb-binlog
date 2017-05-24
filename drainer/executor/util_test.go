package executor

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testExecutorSuite{})

type testExecutorSuite struct{}

var (
	isErrMocks = make(map[errMockType]bool)
	errMock    = errors.New("mock error")
)

type errMockType int64

const (
	beginTxErr = iota
	queryErr
	prepareErr
	execErr
	closeConnErr
	commitTxErr
	rollbackTxErr
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

func (t *testExecutorSuite) TestExecuteSqls(c *C) {
	// open mock db err
	_, err := openDB("mockTestSQL", "127.0.0.1", 3306, "root", "")
	c.Assert(err, NotNil)

	// test open db
	sql.Register("mockTestSQL", &mockDB{})
	db, err := openDB("mockTestSQL", "127.0.0.1", 3306, "root", "")
	c.Assert(err, IsNil)

	// test execute empty sql
	err = executeSQLs(db, nil, nil, false)
	c.Assert(err, IsNil)

	// test execute sql
	err = executeSQLs(db, []string{"test sql"}, [][]interface{}{{}}, false)
	c.Assert(err, IsNil)

	// test retry sql
	maxDMLRetryCount = 2
	retryWaitTime = 0
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
	err = db.Close()
	c.Assert(err, IsNil)
}
