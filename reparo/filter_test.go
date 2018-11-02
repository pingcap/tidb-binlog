package repora

import (
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb-binlog/pump"
	"github.com/pingcap/tidb/store/tikv/oracle"
	gb "github.com/pingcap/tipb/go-binlog"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testReparoSuite{})

type testReparoSuite struct{}

func (s *testReparoSuite) TestIsAcceptableBinlog(c *C) {
	cases := []struct {
		startTs  int64
		endTs    int64
		binlog   *pb.Binlog
		expected bool
	}{
		{
			startTs: 0,
			endTs:   0,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: true,
		}, {
			startTs: 1518003281,
			endTs:   0,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: true,
		}, {
			startTs: 1518003283,
			endTs:   0,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: false,
		}, {
			startTs: 0,
			endTs:   1518003283,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: true,
		}, {
			startTs: 0,
			endTs:   1518003281,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: false,
		}, {
			startTs: 1518003281,
			endTs:   1518003283,
			binlog: &pb.Binlog{
				CommitTs: 1518003282,
			},
			expected: true,
		},
	}

	for _, t := range cases {
		res := isAcceptableBinlog(t.binlog, t.startTs, t.endTs)
		c.Assert(res, Equals, t.expected)
	}
}

/*
func (s *testReparoSuite) TestIsAcceptableBinlogFile(c *C) {
	reparos := []*Reparo{
		{
			cfg: &Config{
				StartDatetime: "2018-10-01 11:11:11",
				StopDatetime:  "2018-10-01 12:11:11",
			},
		},
		{
			cfg: &Config{
				StopDatetime: "2018-10-01 12:11:11",
			},
		},
		{
			cfg: &Config{
				StartDatetime: "2018-10-01 11:11:11",
			},
		},
	}

	fileNames := [][]string{
		{
			"binlog-0000000000000000-20181001101111",
			"binlog-0000000000000000-20181001102111",
			"binlog-0000000000000000-20181001103111",
			"binlog-0000000000000000-20181001111110",
		},
		{
			"binlog-0000000000000000-20181001111111",
			"binlog-0000000000000000-20181001111112",
			"binlog-0000000000000000-20181001121111",
		},
		{
			"binlog-0000000000000000-20181001111112",
			"binlog-0000000000000000-20181001111113",
			"binlog-0000000000000000-20181001211113",
		},
	}

	expectFileNums := [][]int{
		{1, 3, 2},
		{4, 3, 2},
		{1, 3, 3},
	}

	var err error
	for i, r := range reparos {
		r.cfg.StartTSO, err = dateTimeToTSO(r.cfg.StartDatetime)
		c.Assert(err, IsNil)
		r.cfg.StopTSO, err = dateTimeToTSO(r.cfg.StopDatetime)
		c.Assert(err, IsNil)

		for j, fs := range fileNames {
			filterBinlogFile, err := r.filterFiles(fs)
			c.Assert(err, IsNil)
			c.Assert(len(filterBinlogFile), Equals, expectFileNums[i][j])
		}

	}
}
*/

func (s *testReparoSuite) TestIsAcceptableBinlogFile(c *C) {
	binlogDir := "./reparo-test"

	err := os.MkdirAll(binlogDir, 0755)
	c.Assert(err, IsNil)
	defer os.RemoveAll(binlogDir)

	fb, err := pump.OpenBinlogger(binlogDir, compress.CompressionNone)
	c.Assert(err, IsNil)
	defer fb.Close()

	baseTS := int64(oracle.ComposeTS(time.Now().Unix()*1000, 0))

	// create binlog file
	for i := 0; i < 10; i++ {
		binlog := &pb.Binlog{
			CommitTs: baseTS + int64(i),
		}
		binlogData, err := binlog.Marshal()
		c.Assert(err, IsNil)
		fb.WriteTail(&gb.Entity{Payload: binlogData})
		err = fb.Rotate()
		c.Assert(err, IsNil)
	}

	reparos := []*Reparo{
		{
			cfg: &Config{
				Dir:      binlogDir,
				StartTSO: baseTS,
				StopTSO:  baseTS + 9,
			},
		},
		{
			cfg: &Config{
				Dir:      binlogDir,
				StartTSO: baseTS + 2,
			},
		},
		{
			cfg: &Config{
				Dir:     binlogDir,
				StopTSO: baseTS + 2,
			},
		},
	}

	expectFileNums := []int{10, 8, 3}

	for i, r := range reparos {
		files, err := r.searchFiles(binlogDir)
		c.Assert(err, IsNil)

		c.Assert(len(files), Equals, expectFileNums[i])
	}

}
