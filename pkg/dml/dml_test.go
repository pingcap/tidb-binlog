package dml

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDMLSuite{})

type testDMLSuite struct{}

func (s *testDMLSuite) TestGenColumnPlaceholders(c *C) {
	got := GenColumnPlaceholders(3)
	c.Assert(got, Equals, "?,?,?")
}
