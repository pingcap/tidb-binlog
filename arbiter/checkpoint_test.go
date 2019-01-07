package arbiter

import (
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/juju/errors"
	check "github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

type CheckpointSuite struct {
}

var _ = check.Suite(&CheckpointSuite{})

func setNewExpect(mock sqlmock.Sqlmock) {
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 1))
}

func (cs *CheckpointSuite) TestNewCheckpoint(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	setNewExpect(mock)

	_, err = NewCheckpoint(db, "topic_name")
	c.Assert(err, check.IsNil)

	c.Assert(mock.ExpectationsWereMet(), check.IsNil)
}

func (cs *CheckpointSuite) TestSaveAndLoad(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	setNewExpect(mock)
	cp, err := NewCheckpoint(db, "topic_name")
	c.Assert(err, check.IsNil)

	mock.ExpectQuery(fmt.Sprintf("SELECT (.+) FROM %s.%s WHERE topic_name = '%s'",
		cp.database, cp.table, cp.topicName)).
		WillReturnError(errors.NotFoundf("no checkpoint for: %s", cp.topicName))

	_, _, err = cp.Load()
	c.Log(err)
	c.Assert(errors.IsNotFound(err), check.IsTrue)

	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(0, 1))
	var saveTS int64 = 10
	saveStatus := 1
	err = cp.Save(saveTS, saveStatus)
	c.Assert(err, check.IsNil)

	rows := sqlmock.NewRows([]string{"ts", "status"}).
		AddRow(saveTS, saveStatus)
	mock.ExpectQuery("SELECT ts, status FROM").WillReturnRows(rows)
	ts, status, err := cp.Load()
	c.Assert(err, check.IsNil)
	c.Assert(ts, check.Equals, saveTS)
	c.Assert(status, check.Equals, saveStatus)
}
