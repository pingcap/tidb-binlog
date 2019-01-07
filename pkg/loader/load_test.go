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

	loader, err := NewLoader(db, 10, 10)
	c.Assert(err, check.IsNil)

	err = loader.Close()
	c.Assert(err, check.IsNil)
}
