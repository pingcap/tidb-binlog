package syncer

import (

	"github.com/pingcap/check"
)

type testSyncerSuite struct{}

var _ = check.Suite(&testSyncerSuite{})

func (s *testSyncerSuite) TestNewSyncer(c *check.C) {
	cfg := new(DBConfig)
	
	syncer, err := New("mysql", cfg)
	c.Assert(err, check.IsNil)
	_, ok := syncer.(*mysqlSyncer)
	c.Assert(ok, check.Equals, true)

	syncer, err = New("print", cfg)
	c.Assert(err, check.IsNil)
	_, ok = syncer.(*printSyncer)
	c.Assert(ok, check.Equals, true)

	syncer, err = New("memory", cfg)
	c.Assert(err, check.IsNil)
	_, ok = syncer.(*MemSyncer)
	c.Assert(ok, check.Equals, true)

	syncer, err = New("mysql", cfg)
	c.Assert(err, check.IsNil)
	_, ok = syncer.(*MemSyncer)
	c.Assert(ok, check.Equals, false)
}