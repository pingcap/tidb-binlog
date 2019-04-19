package syncer

import (
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
)

type testMysqlSuite struct{}

var _ = check.Suite(&testMysqlSuite{})

func (s *testMysqlSuite) TestMysqlSyncer(c *check.C) {
	db, _, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	syncer, err := newMysqlSyncerFromSQLDB(db)
	c.Assert(err, check.IsNil)

	err = syncer.Close()
	c.Assert(err, check.IsNil)
}
