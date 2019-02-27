package repora

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	. "github.com/pingcap/check"
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

func (s *testReparoSuite) TestIsAcceptableBinlogFileNew(c *C) {
	// we can get the first binlog's commit ts by parse binlog file'name in new version.
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
			// all files' time less than the reparo config1's start time
			// all files' time less than the reparo config2's stop time
			// the max time is less than the reparo config3's start time
			"binlog-v2-0000000000000000-20181001101111",
			"binlog-v2-0000000000000001-20181001102111",
			"binlog-v2-0000000000000002-20181001103111",
			"binlog-v2-0000000000000003-20181001111110",
		},
		{
			// the min time is equal to the reparo config1's start time, the max time is equal to the reparo config1's stop time
			// the max time is equal to the reparo config2's stop time
			// the min time is equal to the reparo config3's start time
			"binlog-v2-0000000000000000-20181001111111",
			"binlog-v2-0000000000000001-20181001111112",
			"binlog-v2-0000000000000002-20181001121111",
		},
		{
			// the min time is greater than the reparo config1's start time, the max time is greater than the reparo config1's stop time
			// the max time is greater than the reparo config2's stop time
			// the min time is greater than the reparo config3's start time
			"binlog-v2-0000000000000000-20181001111112",
			"binlog-v2-0000000000000001-20181001111113",
			"binlog-v2-0000000000000002-20181001211113",
		},
	}

	expectFileNums := [][]int{
		{1, 3, 2},
		{4, 3, 2},
		{1, 3, 3},
	}

	for j, fs := range fileNames {
		binlogDir, err := ioutil.TempDir("", "./reparo-test")
		c.Assert(err, IsNil)
		defer os.RemoveAll(binlogDir)

		for _, f := range fs {
			os.OpenFile(path.Join(binlogDir, f), os.O_RDONLY|os.O_CREATE, 0666)
		}

		for i, r := range reparos {
			if r.cfg.StartDatetime != "" {
				r.cfg.StartTSO, err = dateTimeToTSO(r.cfg.StartDatetime)
				c.Assert(err, IsNil)
			}

			if r.cfg.StopDatetime != "" {
				r.cfg.StopTSO, err = dateTimeToTSO(r.cfg.StopDatetime)
				c.Assert(err, IsNil)
			}

			r.cfg.Dir = binlogDir

			filterBinlogFile, err := r.filterFiles(fs)
			c.Assert(err, IsNil)
			c.Assert(len(filterBinlogFile), Equals, expectFileNums[i][j])
		}

		os.RemoveAll(binlogDir)
	}
}

func (s *testReparoSuite) TestIsAcceptableBinlogFileOld(c *C) {
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

		// generate binlog file by old version's format.
		binloger, err := pump.CreateBinloggerForTest(binlogDir, fmt.Sprintf("binlog-000000000000000%d-20180101010101", i))
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
