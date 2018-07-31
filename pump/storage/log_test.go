package storage

import (
	"bytes"
	"hash/crc32"
	"os"
	"path"
	"strconv"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

func init() {
	log.SetLevel(log.LOG_LEVEL_ERROR)
}

func TestLogFile(t *testing.T) {
	// max ts binlog save in file
	var maxTS int64
	fuzz := fuzz.New().NumElements(200, 200).Funcs(
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
	if err != nil {
		t.Fatal(err)
	}

	// wirte some random records
	var records []*Record
	fuzz.Fuzz(&records)
	t.Logf("fuzz %d records", len(records))

	for _, r := range records {
		_, err := encodeRecord(lf.fd, r.payload)
		if err != nil {
			t.Fatal(err)
		}
	}

	var readRecords = make([]*Record, len(records))

	// read it back by specify offset and check if it's equal
	var offset int64
	for i := range records {
		readRecords[i], err = lf.readRecord(offset)
		if err != nil {
			t.Fatal()
		}
		if bytes.Equal(readRecords[i].payload, records[i].payload) == false {
			t.Fatalf("read back record not equal, get: %v, want: %v", readRecords[i].payload, records[i].payload)
		}
		offset += readRecords[i].recordLength()
	}

	// read by scan and check if if's equal
	idx := 0
	lf.scan(0, func(vp valuePointer, r *Record) error {
		if bytes.Equal(r.payload, records[idx].payload) == false {
			t.Fatalf("read back record not equal, get: %v, want: %v", r.payload, records[idx].payload)
		}
		idx++

		return nil
	})

	if idx != len(records) {
		t.Fatalf("should have %d records but get: %d", len(records), idx)
	}

	// test recover
	if lf.maxTS != 0 {
		t.Fatal("lf.maxTS should be 0, we did't update it")
	}

	lf.close()
	lf, err = newLogFile(lf.fid, lf.path)
	if err != nil {
		t.Fatal(err)
	}

	if lf.maxTS != maxTS {
		t.Fatalf("get: %d, want: %d", lf.maxTS, maxTS)
	}
}
