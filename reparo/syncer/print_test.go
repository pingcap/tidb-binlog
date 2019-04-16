package syncer

import (

	"github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type testPrintSuite struct{}

var _ = check.Suite(&testPrintSuite{})

func (s *testPrintSuite) TestPrintSyncerStr(c *check.C) {
	syncer, err := newPrintSyncer()
	c.Assert(err, check.IsNil)

	ddlBinlog := &pb.Binlog {
		Tp:       pb.BinlogType_DDL,
		DdlQuery: []byte("create database test;"),
	}
	dmlBinlog := &pb.Binlog {
		Tp:       pb.BinlogType_DML,
	}

	err = syncer.Sync(ddlBinlog, func(binlog *pb.Binlog){})
	c.Assert(err, check.IsNil)

	err = syncer.Sync(dmlBinlog, func(binlog *pb.Binlog){})
	c.Assert(err, check.IsNil)

	err = syncer.Close()
	c.Assert(err, check.IsNil)

	ddlStr := getDDLStr(ddlBinlog)
	c.Assert(ddlStr, check.Equals, "DDL query: create database test;\n")

	schema := "test"
	table := "t1"
	event := &pb.Event {
		Tp: pb.EventType_Insert,
		SchemaName: &schema,
		TableName: &table,
	}

	eventHeaderStr := getEventHeaderStr(event)
	c.Assert(eventHeaderStr, check.Equals, "schema: test; table: t1; type: Insert\n")
}
