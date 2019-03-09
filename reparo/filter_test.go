package repora

import (
	"fmt"
	"io/ioutil"
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

func (s *testReparoSuite) TestIsAcceptableBinlogFile(c *C) {
	// we can get the first binlog's commit ts by decode data in binlog file.
	binlogDir, err := ioutil.TempDir("", "./reparo-test")
	c.Assert(err, IsNil)
	defer os.RemoveAll(binlogDir)

	baseTS := int64(oracle.ComposeTS(time.Now().Unix()*1000, 0))

	// create binlog file
	for i := 0; i < 10; i++ {
		binlog := &pb.Binlog{
			CommitTs: baseTS + int64(i),
		}
		binlogData, err := binlog.Marshal()
		c.Assert(err, IsNil)

		// generate binlog file.
		binloger, err := pump.CreateBinlogger(binlogDir, fmt.Sprintf("binlog-%016d-20180101010101", i), compress.CompressionNone)
		c.Assert(err, IsNil)
		binloger.WriteTail(&gb.Entity{Payload: binlogData})
		err = binloger.Close()
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
				StartTSO: baseTS + 1,
				StopTSO:  baseTS + 2,
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

	expectFileNums := []int{10, 2, 8, 3}

	for i, r := range reparos {
		files, err := r.searchFiles(binlogDir)
		c.Assert(err, IsNil)
		c.Assert(len(files), Equals, expectFileNums[i])
	}
}
