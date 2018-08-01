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

func TestNewAppend(t *testing.T) {
	append := newAppend(t)
	defer os.RemoveAll(append.dir)

	append.Close()
}

func TestCloseAndOpenAgain(t *testing.T) {
	append := newAppend(t)
	defer os.RemoveAll(append.dir)

	err := append.Close()
	if err != nil {
		t.Fatal(err)
	}

	append, err = NewAppend(append.dir, append.options)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteBinlog(t *testing.T) {
	append := newAppend(t)
	defer os.RemoveAll(append.dir)

	binlog := new(pb.Binlog)
	binlog.Tp = pb.BinlogType_Prewrite
	binlog.StartTs = 10

	// write P binlog
	err := append.WriteBinlog(binlog)
	if err != nil {
		t.Fatal(err)
	}

	// write C binlog
	binlog = new(pb.Binlog)
	binlog.Tp = pb.BinlogType_Commit
	binlog.StartTs = 10
	binlog.CommitTs = 11
	err = append.WriteBinlog(binlog)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	values := append.PullCommitBinlog(ctx, 0)

	var value []byte
	select {
	case value = <-values:
	case <-time.After(time.Second * 5):
		t.Fatal("get value timeout")
	}

	getBinlog := new(pb.Binlog)
	err = getBinlog.Unmarshal(value)
	if err != nil {
		t.Fatal(err)
	}

	if getBinlog.Tp != binlog.Tp || getBinlog.StartTs != binlog.StartTs {
		t.Fatalf("read back fail before: %v, after: %v", binlog, getBinlog)
	}

	cancel()
}

func TestDoGCTS(t *testing.T) {
	var value = make([]byte, 10)
	append := newAppend(t)
	defer os.RemoveAll(append.dir)

	var i int64
	var n int64 = 1 << 11
	for i = 1; i < n; i++ {
		err := append.db.Put(encodeTSKey(i), value, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	var gcTS int64 = 10
	append.doGCTS(gcTS)

	for i = 1; i < n; i++ {
		_, err := append.db.Get(encodeTSKey(i), nil)
		if i <= gcTS {
			if err != leveldb.ErrNotFound {
				t.Errorf("after gc still found ts: %v", i)
			}
		} else {
			if err != nil {
				t.Errorf("can't found ts: %v", i)
			}
		}
	}
}

func TestReadWritePointer(t *testing.T) {
	append := newAppend(t)
	defer os.RemoveAll(append.dir)

	var vp valuePointer
	var key = []byte("testK")

	var readVP valuePointer
	readVP, _ = append.readPointer(key)
	if readVP != vp {
		t.Fatal("wrong vp: ", readVP)
	}

	vp.Fid = 10
	vp.Offset = 100

	err := append.savePointer(key, vp)
	if err != nil {
		t.Fatal(err)
	}

	readVP, _ = append.readPointer(key)
	if readVP != vp {
		t.Fatal("wrong vp: ", readVP)
	}
}

func TestReadWriteGCTS(t *testing.T) {
	append := newAppend(t)
	defer os.RemoveAll(append.dir)

	ts, _ := append.readGCTSFromDB()
	if ts != 0 {
		t.Fatalf("get: %v, want: %v", ts, 0)
	}

	err := append.saveGCTSToDB(100)
	if err != nil {
		t.Fatal(err)
	}

	ts, _ = append.readGCTSFromDB()
	if ts != 100 {
		t.Fatalf("get: %v, want: %v", ts, 0)
	}

	// close and open read again
	append.Close()
	append, err = NewAppend(append.dir, append.options)
	if err != nil {
		t.Fatal(err)
	}

	if append.gcTS != 100 {
		t.Errorf("get: %v, want: %v", append.gcTS, 0)
	}
}

func TestNoSpace(t *testing.T) {
	// TODO test when no space left
}

func TestResolve(t *testing.T) {
	// TODO test the case we query tikv to know weather a txn a commit
	// is there a fake or mock kv.Storage and tikv.LockResolver to easy the test?
}
