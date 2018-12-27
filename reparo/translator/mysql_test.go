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
		cols  []string
		args  []interface{}
		where string
	}{
		{[]string{"a"}, []interface{}{""}, "`a` = ?"},
		{[]string{"a", "b"}, []interface{}{"", ""}, "`a` = ? AND `b` = ?"},
		{[]string{"a", "b"}, []interface{}{nil, ""}, "`a` IS ? AND `b` = ?"},
	}

	for _, t := range cases {
		where, args := genWhere(t.cols, t.args)
		t.args = args
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
