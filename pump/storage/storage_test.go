package storage

import (
	"context"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
	"github.com/ngaut/log"
	"github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/syndtr/goleveldb/leveldb"
)

func init() {
	log.SetLevel(log.LOG_LEVEL_DEBUG)
}

type Log interface {
	Fatal(args ...interface{})
	Log(args ...interface{})
}

var _ Log = &testing.B{}
var _ Log = &testing.T{}
var _ Log = &check.C{}

type AppendSuit struct{}

var _ = check.Suite(&AppendSuit{})

func newAppend(t Log) *Append {
	return newAppendWithOptions(t, nil)
}

func newAppendWithOptions(t Log, options *Options) *Append {
	dir := path.Join(os.TempDir(), strconv.Itoa(rand.Int()))
	// t.Log("use dir: ", dir)
	err := os.Mkdir(dir, 0777)
	if err != nil {
		t.Fatal(err)
	}

	append, err := NewAppend(dir, options)
	if err != nil {
		t.Fatal(err)
	}

	return append
}

func (as *AppendSuit) TestNewAppend(c *check.C) {
	append := newAppend(c)
	defer os.RemoveAll(append.dir)

	append.Close()
}

func (as *AppendSuit) TestCloseAndOpenAgain(c *check.C) {
	append := newAppend(c)
	defer os.RemoveAll(append.dir)

	err := append.Close()
	c.Assert(err, check.IsNil)

	append, err = NewAppend(append.dir, append.options)
	c.Assert(err, check.IsNil)
}

func (as *AppendSuit) TestWriteBinlogAndPullBack(c *check.C) {
	as.testWriteBinlogAndPullBack(c, 128, 1)

	as.testWriteBinlogAndPullBack(c, 128, 1024)

	as.testWriteBinlogAndPullBack(c, 1<<20, 1024)
}

func (as *AppendSuit) testWriteBinlogAndPullBack(c *check.C, prewriteValueSize int, binlogNum int) {
	appendStorage := newAppend(c)
	defer os.RemoveAll(appendStorage.dir)

	populateBinlog(c, appendStorage, prewriteValueSize, binlogNum)

	ctx, cancel := context.WithCancel(context.Background())
	values := appendStorage.PullCommitBinlog(ctx, 0)

	// pull the binlogs back and check sorted
	var binlogs []*pb.Binlog
PullLoop:
	for {
		select {
		case value := <-values:
			getBinlog := new(pb.Binlog)
			err := getBinlog.Unmarshal(value)
			c.Assert(err, check.IsNil)
			binlogs = append(binlogs, getBinlog)
			if len(binlogs) == binlogNum {
				break PullLoop
			}
		case <-time.After(time.Second * 5):
			c.Fatal("get value timeout")
		}
	}

	// check commitTS increasing
	for i := 1; i < len(binlogs); i++ {
		c.Assert(binlogs[i].CommitTs, check.Greater, binlogs[i-1].CommitTs)
	}
	cancel()
}

func (as *AppendSuit) TestDoGCTS(c *check.C) {
	var value = make([]byte, 10)
	append := newAppend(c)
	defer os.RemoveAll(append.dir)

	var i int64
	var n int64 = 1 << 11
	for i = 1; i < n; i++ {
		err := append.metadata.Put(encodeTSKey(i), value, nil)
		c.Assert(err, check.IsNil)
	}

	var gcTS int64 = 10
	append.doGCTS(gcTS)

	for i = 1; i < n; i++ {
		_, err := append.metadata.Get(encodeTSKey(i), nil)
		if i <= gcTS {
			c.Assert(err, check.Equals, leveldb.ErrNotFound, check.Commentf("after gc still found ts: %v", i))
		} else {
			c.Assert(err, check.IsNil, check.Commentf("can't found ts: %v", i))
		}
	}
}

func (as *AppendSuit) TestReadWritePointer(c *check.C) {
	append := newAppend(c)
	defer os.RemoveAll(append.dir)

	// check return zero valuePointer when the key not exist
	var readVP valuePointer
	var err error
	readVP, err = append.readPointer([]byte("no_exist_key"))
	c.Assert(err, check.IsNil)
	c.Assert(readVP, check.Equals, valuePointer{})

	// test with random key and valuePointer value
	fuzz := fuzz.New().NilChance(0)
	for i := 0; i < 100; i++ {
		var vp valuePointer
		var key []byte
		fuzz.Fuzz(&vp)
		// offset should >= 0, so just take abs(vp.Offset), when the random value is negative
		if vp.Offset < 0 {
			vp.Offset = -vp.Offset
		}
		fuzz.Fuzz(&key)

		err = append.savePointer(key, vp)
		c.Assert(err, check.IsNil)

		readVP, err = append.readPointer(key)
		c.Assert(err, check.IsNil)
		c.Assert(readVP, check.Equals, vp)
	}
}

func (as *AppendSuit) TestReadWriteGCTS(c *check.C) {
	append := newAppend(c)
	defer os.RemoveAll(append.dir)

	ts, err := append.readGCTSFromDB()
	c.Assert(err, check.IsNil)
	c.Assert(ts, check.Equals, int64(0))

	err = append.saveGCTSToDB(100)
	c.Assert(err, check.IsNil)

	ts, err = append.readGCTSFromDB()
	c.Assert(err, check.IsNil)
	c.Assert(ts, check.Equals, int64(100))

	// close and open read again
	append.Close()
	append, err = NewAppend(append.dir, append.options)
	c.Assert(err, check.IsNil)

	c.Assert(append.gcTS, check.Equals, int64(100))
}

// test helper to write binlogNum binlog to append
func populateBinlog(b Log, append *Append, prewriteValueSize int, binlogNum int) {
	prewriteValue := make([]byte, prewriteValueSize)
	var ts int64
	getTS := func() int64 {
		return atomic.AddInt64(&ts, 1)
	}

	var wg sync.WaitGroup
	for i := 0; i < binlogNum; i++ {
		wg.Add(1)
		func() {
			defer wg.Done()
			// write P binlog
			binlog := new(pb.Binlog)
			binlog.Tp = pb.BinlogType_Prewrite
			startTS := getTS()
			binlog.StartTs = startTS
			binlog.PrewriteValue = prewriteValue

			err := append.WriteBinlog(binlog)
			if err != nil {
				b.Fatal(err)
			}

			// write C binlog
			binlog = new(pb.Binlog)
			binlog.Tp = pb.BinlogType_Commit
			binlog.StartTs = startTS
			binlog.CommitTs = getTS()
			err = append.WriteBinlog(binlog)
			if err != nil {
				b.Fatal(err)
			}
		}()
	}

	// wait finish populate data
	wg.Wait()
}

func (as *AppendSuit) TestNoSpace(c *check.C) {
	// TODO test when no space left
}

func (as *AppendSuit) TestResolve(c *check.C) {
	// TODO test the case we query tikv to know weather a txn a commit
	// is there a fake or mock kv.Storage and tikv.LockResolver to easy the test?
}
