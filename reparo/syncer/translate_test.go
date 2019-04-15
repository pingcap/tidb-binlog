package syncer

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
)

func Test(t *testing.T) { check.TestingT(t) }

type testTranslateSuite struct{}

var _ = check.Suite(&testTranslateSuite{})

func (s *testTranslateSuite) TestPBBinlogToTxn(c *check.C) {
	tests := map[*pb.Binlog]*loader.Txn{
		&pb.Binlog{
			Tp:       pb.BinlogType_DDL,
			DdlQuery: []byte("use db1; create table table1(id int)"),
		}: &loader.Txn{
			DDL: &loader.DDL{
				SQL: "use db1; create table table1(id int)",
			},
		},
		// TODO add dml test
		&pb.Binlog{
			Tp: pb.BinlogType_DML,
			DmlData: &pb.DMLData{
				Events: []pb.Event{},
			},
		}: &loader.Txn{},
	}

	for binlog, txn := range tests {
		getTxn, err := pbBinlogToTxn(binlog)
		c.Assert(err, check.IsNil)
		c.Assert(getTxn, check.DeepEquals, txn)
	}
}
