package syncer

import (
	"strings"

	capturer "github.com/kami-zh/go-capturer"
	"github.com/pingcap/check"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

type testPrintSuite struct{}

var _ = check.Suite(&testPrintSuite{})

func (s *testPrintSuite) TestPrintSyncer(c *check.C) {
	syncer, err := newPrintSyncer()
	c.Assert(err, check.IsNil)

	syncTest(c, Syncer(syncer))

	err = syncer.Close()
	c.Assert(err, check.IsNil)
}

func (s *testPrintSuite) TestPrintEventHeader(c *check.C) {
	schema := "test"
	table := "t1"
	event := &pb.Event{
		Tp:         pb.EventType_Insert,
		SchemaName: &schema,
		TableName:  &table,
	}

	out := capturer.CaptureStdout(func() {
		printEventHeader(event)
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	c.Assert(lines, check.HasLen, 1)
	c.Assert(lines[0], check.Matches, ".*schema: test; table: t1; type: Insert.*")
}

func (s *testPrintSuite) TestPrintDDL(c *check.C) {
	ddlBinlog := &pb.Binlog{
		Tp:       pb.BinlogType_DDL,
		DdlQuery: []byte("create database test;"),
	}

	out := capturer.CaptureStdout(func() {
		printDDL(ddlBinlog)
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	c.Assert(lines, check.HasLen, 1)
	c.Assert(lines[0], check.Matches, ".*DDL query: create database test;.*")
}

func (s *testPrintSuite) TestPrintRow(c *check.C) {
	cols := generateColumns(c)

	insertEvent := &pb.Event{
		Tp:  pb.EventType_Insert,
		Row: [][]byte{cols[0], cols[1]},
	}

	out := capturer.CaptureStdout(func() {
		printEvent(insertEvent)
	})
	lines := strings.Split(strings.TrimSpace(out), "\n")
	c.Assert(lines, check.HasLen, 3)
	c.Assert(lines[0], check.Equals, "schema: ; table: ; type: Insert")
	c.Assert(lines[1], check.Equals, "a(int): 1")
	c.Assert(lines[2], check.Equals, "b(varchar): test")

	deleteEvent := &pb.Event{
		Tp:  pb.EventType_Delete,
		Row: [][]byte{cols[0], cols[1]},
	}
	out = capturer.CaptureStdout(func() {
		printEvent(deleteEvent)
	})
	lines = strings.Split(strings.TrimSpace(out), "\n")
	c.Assert(lines, check.HasLen, 3)
	c.Assert(lines[0], check.Equals, "schema: ; table: ; type: Delete")
	c.Assert(lines[1], check.Equals, "a(int): 1")
	c.Assert(lines[2], check.Equals, "b(varchar): test")

	updateEvent := &pb.Event{
		Tp:  pb.EventType_Update,
		Row: [][]byte{cols[2]},
	}
	out = capturer.CaptureStdout(func() {
		printEvent(updateEvent)
	})
	lines = strings.Split(strings.TrimSpace(out), "\n")
	c.Assert(lines, check.HasLen, 2)
	c.Assert(lines[0], check.Equals, "schema: ; table: ; type: Update")
	c.Assert(lines[1], check.Equals, "c(varchar): test => abc")
}
