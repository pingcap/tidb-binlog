package storage

import (
	"hash/crc32"
	"os"
	"path"
	"strconv"

	fuzz "github.com/google/gofuzz"
	"github.com/ngaut/log"
	"github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func init() {
	log.SetLevel(log.LOG_LEVEL_ERROR)
}

type LogFileSuit struct{}

var _ = check.Suite(&LogFileSuit{})

func (lfs *LogFileSuit) TestLogFile(c *check.C) {
	// max ts binlog save in file
	var maxTS int64
	fuzz := fuzz.New().NilChance(0).NumElements(200, 200).Funcs(
		func(r *Record, c fuzz.Continue) {
			r.magic = recordMagic
			binlog := new(pb.Binlog)
			binlog.CommitTs = int64(c.Intn(10000))
			if binlog.CommitTs > maxTS {
				maxTS = binlog.CommitTs
			}
			binlog.Tp = pb.BinlogType_Commit
			r.payload, _ = binlog.Marshal()
			r.length = uint64(len(r.payload))
			r.checksum = crc32.Checksum(r.payload, crcTable)
		},
	)

	fid := 0
	name := path.Join(os.TempDir(), strconv.Itoa(fid))
	defer os.Remove(name)

	lf, err := newLogFile(uint32(fid), name)
	c.Assert(err, check.IsNil)

	// wirte some random records
	var records []*Record
	fuzz.Fuzz(&records)
	c.Logf("fuzz %d records", len(records))
	c.Check(len(records), check.Greater, 0)

	for _, r := range records {
		_, err := encodeRecord(lf.fd, r.payload)
		c.Assert(err, check.IsNil)
	}

	var readRecords = make([]*Record, len(records))
	// save to check weather the call back valuePointer'offset is right
	var recordOffsets = make([]int64, len(records))

	// read it back by specify offset and check if it's equal
	var offset int64
	for i := range records {
		readRecords[i], err = lf.readRecord(offset)
		c.Assert(err, check.IsNil)

		recordOffsets[i] = offset

		c.Assert(readRecords[i].payload, check.DeepEquals, records[i].payload)
		offset += readRecords[i].recordLength()
	}

	// read by scan and check if if's equal
	idx := 0
	lf.scan(0, func(vp valuePointer, r *Record) error {
		c.Assert(r.payload, check.DeepEquals, records[idx].payload)
		c.Assert(recordOffsets[idx], check.Equals, vp.Offset)
		idx++

		return nil
	})

	c.Assert(idx, check.Equals, len(records))

	// test recover lf.maxTS
	c.Assert(int(lf.maxTS), check.Equals, 0, check.Commentf("lf.maxTS should be 0, we did't update it"))

	lf.close()
	lf, err = newLogFile(lf.fid, lf.path)
	c.Assert(err, check.IsNil)

	c.Assert(lf.maxTS, check.Equals, maxTS)
}
