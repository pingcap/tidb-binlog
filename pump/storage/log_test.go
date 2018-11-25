package storage

import (
	"bufio"
	"bytes"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"

	fuzz "github.com/google/gofuzz"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func init() {
	log.SetLevel(log.LOG_LEVEL_ERROR)
}

type LogFileSuit struct {
	fuzz *fuzz.Fuzzer
	// max ts allocate by fuzz for pb.Binlog
	maxTS int64

	lf *logFile
}

var _ = check.Suite(&LogFileSuit{})

func (lfs *LogFileSuit) SetUpTest(c *check.C) {
	// set up fuzz
	lfs.maxTS = 0
	lfs.fuzz = fuzz.New().NilChance(0).NumElements(200, 200).Funcs(
		func(r *Record, c fuzz.Continue) {
			r.magic = recordMagic
			binlog := new(pb.Binlog)
			binlog.CommitTs = int64(c.Intn(10000))
			if binlog.CommitTs > lfs.maxTS {
				lfs.maxTS = binlog.CommitTs
			}
			binlog.Tp = pb.BinlogType_Commit
			r.payload, _ = binlog.Marshal()
			r.length = uint64(len(r.payload))
			r.checksum = crc32.Checksum(r.payload, crcTable)
		},
	)

	// set up logFile
	fid := 0
	name := path.Join(os.TempDir(), strconv.Itoa(fid))
	c.Log("file path: ", name)

	lf, err := newLogFile(uint32(fid), name)
	c.Assert(err, check.IsNil)
	lfs.lf = lf
}

func (lfs *LogFileSuit) TearDownTest(c *check.C) {
	// delete log file
	os.Remove(lfs.lf.path)
}

func (lfs *LogFileSuit) TestSeekToNextRecord(c *check.C) {
	buffer := new(bytes.Buffer)

	// first write a record
	var payload = make([]byte, 100)
	recordLen, err := encodeRecord(buffer, payload)
	c.Log("record len: ", recordLen)
	c.Assert(err, check.IsNil)
	// write corruption data
	_, err = buffer.Write(payload)
	c.Assert(err, check.IsNil)
	// write a record again
	recordLen, err = encodeRecord(buffer, payload)
	c.Assert(err, check.IsNil)

	// data contains <record><payload len zero data><record>
	data := buffer.Bytes()
	firstStart := 0
	secondStart := recordLen + len(payload)
	c.Logf("data len: %d, firstStart: %d, secondStart: %d", len(data), firstStart, secondStart)

	for idx := 0; idx < len(data); idx++ {
		reader := bytes.NewReader(data[idx:])
		bytes, err := seekToNextRecord(bufio.NewReader(reader))
		if idx <= firstStart {
			c.Assert(err, check.IsNil)
			c.Assert(bytes, check.Equals, firstStart-idx)
		} else if idx <= secondStart {
			c.Assert(err, check.IsNil)
			c.Assert(bytes, check.Equals, secondStart-idx)
		} else {
			c.Assert(err, check.NotNil)
			c.Assert(bytes, check.Equals, len(data)-idx)
		}

	}
}

func (lfs *LogFileSuit) TestSimpleCorruption(c *check.C) {
	lf := lfs.lf
	var payload = make([]byte, 100)
	// write one record
	recordLen, err := encodeRecord(lf.fd, payload)
	c.Assert(err, check.IsNil)

	// truncate file
	err = lf.fd.Truncate(int64(recordLen) / 2)
	c.Assert(err, check.IsNil)

	// write one record
	recordLen, err = encodeRecord(lf.fd, payload)
	c.Assert(err, check.IsNil)

	// should get the later one record write
	var recordGet *Record
	lf.scan(0, func(vp valuePointer, record *Record) error {
		if recordGet != nil {
			c.Fatal("get more than on record")
		}

		recordGet = record
		c.Assert(record.payload, check.BytesEquals, payload)

		return nil
	})

	c.Assert(recordGet, check.NotNil)
}

func (lfs *LogFileSuit) TestCorruption(c *check.C) {
	lf := lfs.lf
	fuzz := lfs.fuzz

	var err error

	var payloads = make([][]byte, 100)

	for i := 0; i < len(payloads); i++ {
		r := new(Record)
		fuzz.Fuzz(r)
		payloads[i] = r.payload
	}

	// var writeSuccessPayloads [][]byte
	var expectPayloads [][]byte
	var scanBackPayloads [][]byte

	// the size of the log file
	var size int
	for idx, payload := range payloads {
		n, err := encodeRecord(lf.fd, payload)
		c.Assert(err, check.IsNil)
		size += n

		// truncate half of records
		if idx%2 == 0 {
			var truncateBytes int
			truncateBytes = rand.Intn(n) + 1
			c.Log("truncate: ", truncateBytes)
			size -= truncateBytes
			err := lf.fd.Truncate(int64(size))
			c.Assert(err, check.IsNil)
		} else {
			expectPayloads = append(expectPayloads, payload)
		}
	}

	err = lf.scan(0, func(vp valuePointer, record *Record) error {
		scanBackPayloads = append(scanBackPayloads, record.payload)
		return nil
	})
	c.Assert(err, check.IsNil)

	rd := bufio.NewReader(io.NewSectionReader(lf.fd, 0, int64(size)))
	data, err := ioutil.ReadAll(rd)
	c.Log(data)

	c.Assert(scanBackPayloads, check.DeepEquals, expectPayloads, check.Commentf("payloads: %x", payloads))
}

func (lfs *LogFileSuit) TestSeekToNextRecordReturnEOF(c *check.C) {
	lf := lfs.lf

	for size := 1; size < 10; size++ {
		n, err := lf.fd.Write(make([]byte, size))
		c.Assert(err, check.IsNil)
		c.Assert(n, check.Equals, size)

		reader := bufio.NewReader(io.NewSectionReader(lf.fd, 0, int64(size)))
		_, err = seekToNextRecord(reader)
		c.Assert(errors.Cause(err), check.Equals, io.EOF)
	}
}

func (lfs *LogFileSuit) TestLogFile(c *check.C) {
	lf := lfs.lf

	// write some random records
	var records []*Record
	lfs.fuzz.Fuzz(&records)
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
	var err error
	for i := range records {
		readRecords[i], err = lf.readRecord(offset)
		c.Assert(err, check.IsNil)

		recordOffsets[i] = offset

		c.Assert(readRecords[i].payload, check.DeepEquals, records[i].payload)
		offset += readRecords[i].recordLength()
	}

	// read by scan and check if if's equal
	idx := 0
	err = lf.scan(0, func(vp valuePointer, r *Record) error {
		c.Assert(r.payload, check.DeepEquals, records[idx].payload)
		c.Assert(recordOffsets[idx], check.Equals, vp.Offset)
		idx++

		return nil
	})
	c.Assert(err, check.IsNil)

	c.Assert(idx, check.Equals, len(records))

	// test recover lf.maxTS
	c.Assert(int(lf.maxTS), check.Equals, 0, check.Commentf("lf.maxTS should be 0, we didn't update it"))

	lf.close()
	lf, err = newLogFile(lf.fid, lf.path)
	c.Assert(err, check.IsNil)

	c.Assert(lf.maxTS, check.Equals, lfs.maxTS)
}
