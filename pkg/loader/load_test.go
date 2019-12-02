package loader

import (
	sqlmock "github.com/DATA-DOG/go-sqlmock"
	check "github.com/pingcap/check"
)

type LoadSuite struct {
}

var _ = check.Suite(&LoadSuite{})

func (cs *LoadSuite) SetUpTest(c *check.C) {
}

func (cs *LoadSuite) TearDownTest(c *check.C) {
}

func (cs *LoadSuite) TestNewClose(c *check.C) {
	db, _, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	loader, err := NewLoader(db)
	c.Assert(err, check.IsNil)

	loader.Close()
}

type needRefreshTableInfoSuite struct{}

var _ = check.Suite(&needRefreshTableInfoSuite{})

func (s *needRefreshTableInfoSuite) TestNeedRefreshTableInfo(c *check.C) {
	cases := map[string]bool{
		"DROP TABLE a":           false,
		"DROP DATABASE a":        false,
		"TRUNCATE TABLE a":       false,
		"CREATE DATABASE a":      false,
		"CREATE TABLE a(id int)": true,
	}

	for sql, res := range cases {
		c.Assert(needRefreshTableInfo(sql), check.Equals, res)
	}
}
