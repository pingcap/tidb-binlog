package cistern

import (
	"os"
	"path"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/codec"
)

var testDS *BinlogStorage

func Test(t *testing.T) {
	binlogNamespace := []byte("binlog")
	segmentNamespace := []byte("meta")
	dir := "./test"
	os.MkdirAll(dir, 07777)
	s, err := store.NewBoltStore(path.Join(dir, "test"), [][]byte{binlogNamespace, segmentNamespace}, false)
	if err != nil {
		t.Fatalf("create meta store", err)
	}
	defer func() {
		s.Close()
		os.RemoveAll(dir)
	}()

	testDS, err = NewBinlogStorage(s, dir, 29, false)
	if err != nil {
		t.Fatalf("init binlogStorage", err)
	}
	TestingT(t)
}

var _ = Suite(&testCisternSuite{})

type testCisternSuite struct{}

func (t *testCisternSuite) TestBinlogStorage(c *C) {
	keys := []int64{
		// 189131724161024000 => 352285288
		getBinlogTS("1992-11-11 11:11:11"),
		// 346294811623424000 => 645024351
		getBinlogTS("2011-11-11 11:11:11"),
		// 379385353601024000 => 706660288
		getBinlogTS("2015-11-11 11:11:11"),
		// 386746357121024000 => 720371226
		getBinlogTS("2016-10-01 11:11:11"),
		// 389284039753728000 => 725098028
		getBinlogTS("2017-01-21 12:12:42"),
		// 389464289968128000 => 725433770
		getBinlogTS("2017-01-29 11:12:42"),
		// 389465321766912000 => 725435692
		getBinlogTS("2017-01-29 12:18:18"),
		// 389465322029056000 => 725435692
		getBinlogTS("2017-01-29 12:18:19"),
		// 397732294950912000 => 740834129
		getBinlogTS("2018-01-29 12:18:18"),
	}

	testBatchAndCommit(c, keys[0:6])
	mustCheckSegments(c, 6)
	testPut(c, keys[6:])
	mustCheckSegments(c, 8)
	testGet(c, keys)
	testScan(c, keys, 0)
	testScan(c, keys, 6)
	testEndKeys(c, keys[8])
	testGColdBinLog(c, keys)
	testDS.Close()
}

func testBatchAndCommit(c *C, keys []int64) {
	batch := testDS.NewBatch()
	for _, key := range keys {
		batch.Put(key, []byte("hello"))
	}
	err := testDS.Commit(batch)
	c.Check(err, IsNil)
}

func testGet(c *C, keys []int64) {
	for _, ts := range keys {
		val, err := testDS.Get(ts)
		c.Assert(err, IsNil)
		c.Assert(val, DeepEquals, []byte("hello"))
	}
}

func testPut(c *C, keys []int64) {
	for _, key := range keys {
		err := testDS.Put(key, []byte("hello"))
		c.Assert(err, IsNil)
	}
}

func testScan(c *C, keys []int64, index int) {
	err := testDS.Scan(keys[index], func(key, val []byte) (bool, error) {
		exceptedKey := codec.EncodeInt([]byte{}, keys[index])
		c.Assert(exceptedKey, HasLen, len(key))
		for i := range exceptedKey {
			c.Assert(exceptedKey[i], Equals, key[i])
		}
		c.Assert(val, DeepEquals, []byte("hello"))
		index++
		return true, nil
	})
	c.Assert(index, Equals, len(keys))
	c.Assert(err, IsNil)
}

func testEndKeys(c *C, ts int64) {
	key, err := testDS.EndKey()
	c.Assert(err, IsNil)
	exceptedKey := codec.EncodeInt([]byte{}, ts)
	c.Assert(exceptedKey, HasLen, len(key))
	for i := range exceptedKey {
		c.Assert(exceptedKey[i], Equals, key[i])
	}
}

func mustCheckSegments(c *C, count int) {
	c.Assert(len(testDS.mu.segments), Equals, count)
	c.Assert(len(testDS.mu.segmentTSs), Equals, count)
	nums := 0
	err := testDS.metaStore.Scan(segmentNamespace, nil, func(key []byte, val []byte) (bool, error) {
		_, segmentTS, err := codec.DecodeInt(key)
		c.Assert(err, IsNil)
		c.Assert(segmentTS, Equals, testDS.mu.segmentTSs[nums])
		_, ok := testDS.mu.segments[segmentTS]
		c.Assert(ok, IsTrue)
		nums++
		return true, nil
	})
	c.Assert(err, IsNil)
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
