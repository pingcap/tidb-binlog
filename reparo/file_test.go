package reparo

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/compress"
	pb "github.com/pingcap/tidb-binlog/proto/binlog"
	"github.com/pingcap/tidb/store/tikv/oracle"
	gb "github.com/pingcap/tipb/go-binlog"
)

var _ = Suite(&testFileSuite{})

type testFileSuite struct{}

func (s *testFileSuite) TestIsAcceptableBinlogFile(c *C) {
	binlogDir := c.MkDir()
	baseTS := int64(oracle.ComposeTS(time.Now().Unix()*1000, 0))

	// set SegmentSizeBytes to 1 can rotate binlog file after every binlog write
	binlogfile.SegmentSizeBytes = 1

	// create binlog file
	for i := 0; i < 10; i++ {
		binlog := &pb.Binlog{
			CommitTs: baseTS + int64(i),
		}
		binlogData, err := binlog.Marshal()
		c.Assert(err, IsNil)

		binloger, err := binlogfile.OpenBinlogger(binlogDir, compress.CompressionNone)
		c.Assert(err, IsNil)
		binloger.WriteTail(&gb.Entity{
			Payload: binlogData,
			Meta: gb.Meta{
				CommitTs: binlog.CommitTs,
			},
		})
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
	expectFileNums := []int{10, 2, 9, 3}

	allFiles, err := searchFiles(binlogDir)
	c.Assert(err, IsNil)

	for i, r := range reparos {
		files, err := filterFiles(allFiles, r.cfg.StartTSO, r.cfg.StopTSO)
		c.Assert(err, IsNil)
		c.Assert(files, HasLen, expectFileNums[i])
	}
}
