package syncer

import (
	"time"

	"github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type testMemorySuite struct{}

var _ = check.Suite(&testMemorySuite{})

func (s *testMemorySuite) TestMemorySyncer(c *check.C) {
	syncer, err := newMemSyncer()
	c.Assert(err, check.IsNil)

	ddlBinlog := &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		DdlQuery: []byte("create database test;"),
	}
	dmlBinlog := &pb.Binlog{
		Tp: pb.BinlogType_DML,
		DmlData: &pb.DMLData{
			Events: generateDMLEvents(c)[0:1],
		},
	}

	binlogs := make([]*pb.Binlog, 0, 2)
	err = syncer.Sync(ddlBinlog, func(binlog *pb.Binlog) {
		c.Log(binlog)
		binlogs = append(binlogs, binlog)
	})
	c.Assert(err, check.IsNil)

	err = syncer.Sync(dmlBinlog, func(binlog *pb.Binlog) {
		c.Log(binlog)
		binlogs = append(binlogs, binlog)
	})
	c.Assert(err, check.IsNil)

	time.Sleep(100 * time.Millisecond)
	c.Assert(binlogs, check.HasLen, 2)

	err = syncer.Close()
	c.Assert(err, check.IsNil)

	binlog := syncer.GetBinlogs()
	c.Assert(binlog, check.DeepEquals, []*pb.Binlog{ddlBinlog, dmlBinlog})
}
