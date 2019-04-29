package syncer

import (
	"reflect"

	"github.com/pingcap/check"
)

type testSyncerSuite struct{}

var _ = check.Suite(&testSyncerSuite{})

func (s *testSyncerSuite) TestNewSyncer(c *check.C) {
	cfg := new(DBConfig)

	testCases := []struct {
		typeStr string
		tp      reflect.Type
		checker check.Checker
	}{
		{
			"print",
			reflect.TypeOf(new(printSyncer)),
			check.Equals,
		}, {
			"memory",
			reflect.TypeOf(new(MemSyncer)),
			check.Equals,
		}, {
			"print",
			reflect.TypeOf(new(MemSyncer)),
			check.Not(check.Equals),
		},
	}

	for _, testCase := range testCases {
		syncer, err := New(testCase.typeStr, cfg)
		c.Assert(err, check.IsNil)
		c.Assert(reflect.TypeOf(syncer), testCase.checker, testCase.tp)
	}
}
