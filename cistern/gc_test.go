package cistern_test

import (
	"os"
	"testing"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/cistern"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/codec"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testGCSuite{})

type testGCSuite struct{}

func (suite *testGCSuite) TestGColdBinLog(c *C) {
	binlogNamespace := []byte("binlog")
	s, err := store.NewBoltStore("./test", [][]byte{binlogNamespace})
	c.Assert(err, IsNil)
	defer func() {
		s.Close()
		os.Remove("./test")
	}()

	keys := [][]byte{
		getBinlogKey("2011-11-11 11:11:11"),
		getBinlogKey("2015-11-11 11:11:11"),
		getBinlogKey("2016-10-01 11:11:11"),
		getBinlogKey("2016-10-27 18:18:18"),
		getBinlogKey("2016-10-28 18:18:18"),
		getBinlogKey("2016-10-29 17:12:42"),
	}

	batch := s.NewBatch()
	for _, key := range keys {
		batch.Put(key, []byte("hello"))
	}
	err = s.Commit(binlogNamespace, batch)
	c.Check(err, IsNil)

	// remove binlog older than 7 days
	err = cistern.GCHistoryBinlog(s, binlogNamespace, 7*time.Hour*24)
	c.Check(err, IsNil)

	// check
	v, err := s.Get(binlogNamespace, keys[0])
	c.Check(errors.IsNotFound(err), IsTrue)
	v, err = s.Get(binlogNamespace, keys[1])
	c.Check(errors.IsNotFound(err), IsTrue)
	v, err = s.Get(binlogNamespace, keys[2])
	c.Check(errors.IsNotFound(err), IsTrue)
	v, err = s.Get(binlogNamespace, keys[3])
	c.Check(err, IsNil)
	c.Check(v, NotNil)

	// remove neally all data
	err = cistern.GCHistoryBinlog(s, binlogNamespace, 5*time.Hour)
	c.Check(err, IsNil)
	v, err = s.Get(binlogNamespace, keys[4])
	c.Check(errors.IsNotFound(err), IsTrue)
}

func getBinlogKey(date string) []byte {
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, date)
	if err != nil {
		panic(err)
	}
	physical := oracle.GetPhysical(t)
	ts := oracle.ComposeTS(physical, 0)
	key := codec.EncodeInt(nil, int64(ts))
	return key
}
