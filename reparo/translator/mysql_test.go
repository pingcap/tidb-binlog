package translator

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTranslatorSuite{})

type testTranslatorSuite struct{}

func (s *testTranslatorSuite) TestGenWhere(c *C) {
	cases := []struct {
		cols               []string
		args               []interface{}
		where              string
		expectedArgsLength int
	}{
		{[]string{"a"}, []interface{}{""}, "`a` = ?", 1},
		{[]string{"a", "b"}, []interface{}{"", ""}, "`a` = ? AND `b` = ?", 2},
		{[]string{"a", "b"}, []interface{}{nil, ""}, "`a` IS NULL AND `b` = ?", 1},
		{[]string{"a", "b", "c"}, []interface{}{"a", nil, "c"}, "`a` = ? AND `b` IS NULL AND `c` = ?", 2},
		{[]string{"a", "b", "c"}, []interface{}{"a", "b", nil}, "`a` = ? AND `b` = ? AND `c` IS NULL", 2},
		{[]string{"a", "b"}, []interface{}{nil, nil}, "`a` IS NULL AND `b` IS NULL", 0},
	}

	for _, t := range cases {
		where, values := genWhere(t.cols, t.args)
		c.Assert(len(values), Equals, t.expectedArgsLength)
		c.Assert(where, Equals, t.where)
	}
}

func (s *testTranslatorSuite) TestGenKVs(c *C) {
	cases := []struct {
		cols []string
		kvs  string
	}{
		{[]string{"a"}, "`a` = ?"},
		{[]string{"a", "b", "c"}, "`a` = ?, `b` = ?, `c` = ?"},
	}
	for _, t := range cases {
		c.Assert(genKVs(t.cols), Equals, t.kvs)
	}
}
