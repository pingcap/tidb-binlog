package storage

import (
	"context"
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

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

func (as *AppendSuit) TestWriteBinlog(c *check.C) {
	append := newAppend(c)
	defer os.RemoveAll(append.dir)

	binlog := new(pb.Binlog)
	binlog.Tp = pb.BinlogType_Prewrite
	binlog.StartTs = 10

	// write P binlog
	err := append.WriteBinlog(binlog)
	c.Assert(err, check.IsNil)

	// write C binlog
	binlog = new(pb.Binlog)
	binlog.Tp = pb.BinlogType_Commit
	binlog.StartTs = 10
	binlog.CommitTs = 11
	err = append.WriteBinlog(binlog)
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	values := append.PullCommitBinlog(ctx, 0)

	var value []byte
	select {
	case value = <-values:
	case <-time.After(time.Second * 5):
		c.Fatal("get value timeout")
	}

	getBinlog := new(pb.Binlog)
	err = getBinlog.Unmarshal(value)
	c.Assert(err, check.IsNil)

	c.Assert(getBinlog.Tp, check.Equals, binlog.Tp)
	c.Assert(getBinlog.StartTs, check.Equals, binlog.StartTs)

	cancel()
}

func (as *AppendSuit) TestDoGCTS(c *check.C) {
	var value = make([]byte, 10)
	append := newAppend(c)
	defer os.RemoveAll(append.dir)

	var i int64
	var n int64 = 1 << 11
	for i = 1; i < n; i++ {
		err := append.db.Put(encodeTSKey(i), value, nil)
		c.Assert(err, check.IsNil)
	}

	var gcTS int64 = 10
	append.doGCTS(gcTS)

	for i = 1; i < n; i++ {
		_, err := append.db.Get(encodeTSKey(i), nil)
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

	var vp valuePointer
	var key = []byte("testK")

	var readVP valuePointer
	var err error
	readVP, err = append.readPointer(key)
	c.Assert(err, check.IsNil)
	c.Assert(readVP, check.Equals, vp)

	vp.Fid = 10
	vp.Offset = 100

	err = append.savePointer(key, vp)
	c.Assert(err, check.IsNil)

	readVP, err = append.readPointer(key)
	c.Assert(err, check.IsNil)
	c.Assert(readVP, check.Equals, vp)
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

func (as *AppendSuit) TestNoSpace(c *check.C) {
	// TODO test when no space left
}

func (as *AppendSuit) TestResolve(c *check.C) {
	// TODO test the case we query tikv to know weather a txn a commit
	// is there a fake or mock kv.Storage and tikv.LockResolver to easy the test?
}
