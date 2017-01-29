package cistern_test

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/cistern"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testGCSuite{})

type testGCSuite struct{}

func (suite *testGCSuite) TestGColdBinLog(c *C) {
	binlogNamespace := []byte("binlog")
	segmentNamespace := []byte("meta")
	dir := "./test"
	os.MkdirAll(dir, 07777)
	s, err := store.NewBoltStore(path.Join(dir, "test"), [][]byte{binlogNamespace, segmentNamespace}, false)
	c.Assert(err, IsNil)
	defer func() {
		s.Close()
		os.RemoveAll(dir)
	}()

	err = cistern.InitTest(binlogNamespace, segmentNamespace, s, dir, 20, false)
	c.Assert(err, IsNil)

	keys := []int64{
		getBinlogTS("2011-11-11 11:11:11"),
		getBinlogTS("2015-11-11 11:11:11"),
		getBinlogTS("2016-10-01 11:11:11"),
		getBinlogTS("2017-01-29 12:18:18"),
		getBinlogTS("2017-01-29 12:18:18"),
		getBinlogTS("2017-01-29 12:12:42"),
	}

	batch := cistern.DS.NewBatch()
	for _, key := range keys {
		batch.Put(key, []byte("hello"))
	}
	err = cistern.DS.Commit(batch)
	c.Check(err, IsNil)

	// remove binlog older than 7 days
	err = cistern.GCHistoryBinlog(2 * time.Hour * 24)
	c.Check(err, IsNil)

	// check
	_, err = cistern.DS.Get(keys[0])
	c.Check(errors.IsNotFound(err), IsTrue)
	_, err = cistern.DS.Get(keys[1])
	c.Check(errors.IsNotFound(err), IsTrue)
	_, err = cistern.DS.Get(keys[2])
	c.Check(errors.IsNotFound(err), IsTrue)
	v, err := cistern.DS.Get(keys[3])
	c.Check(err, IsNil)
	c.Check(v, NotNil)
	cistern.DS.Close()
}

func getBinlogTS(date string) int64 {
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, date)
	if err != nil {
		panic(err)
	}
	physical := oracle.GetPhysical(t)
	ts := oracle.ComposeTS(physical, 0)
	return int64(ts)
}
