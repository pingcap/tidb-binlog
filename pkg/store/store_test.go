package store

import (
	//"fmt"
	"os"
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/codec"
)

var keys [][]byte
var batchValues [][]byte
var values [][]byte
var testData = []int64{100, 101, 200, 1000}

func init() {
	batchData := []byte("test")
	for i := 0; i < len(testData); i++ {
		keys = append(keys, codec.EncodeInt([]byte{}, testData[i]))
		values = append(values, codec.EncodeInt([]byte{}, testData[i]))
		batchValues = append(batchValues, batchData)
	}
}

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStoreSuite{})

type testStoreSuite struct{}

func (s *testStoreSuite) TestBlot(c *C) {
	store, err := NewBoltStore("./test", [][]byte{windowNamespace, binlogNamespace})
	c.Assert(err, IsNil)

	defer func() {
		store.Close()
		os.Remove("./test")
	}()

	testBuckets(c, store)
	testPut(c, store)
	testGet(c, store)
	testScan(c, store)
	testBatch(c, store)
}

func testBuckets(c *C, store *BoltStore) {
	err := store.Put(windowNamespace, codec.EncodeInt([]byte{}, 1), []byte("test"))
	c.Assert(err, IsNil)

	err = store.Put([]byte("test"), codec.EncodeInt([]byte{}, 1), []byte("test"))
	if err == nil {
		c.Fatal("want bucket test not found, but it exits")
	}
}

func testPut(c *C, store *BoltStore) {
	for i := range keys {
		err := store.Put(binlogNamespace, keys[i], values[i])
		c.Assert(err, IsNil)
	}
}

func testGet(c *C, store *BoltStore) {
	for i := range keys {
		val, err := store.Get(binlogNamespace, keys[i])
		c.Assert(err, IsNil)
		c.Assert(val, DeepEquals, values[i])
	}
}

func testScan(c *C, store *BoltStore) {
	index := 1
	err := store.Scan(binlogNamespace, keys[1], func(key []byte, val []byte) (bool, error) {
		if index == 3 {
			return false, nil
		}

		c.Assert(key, DeepEquals, keys[index])
		c.Assert(val, DeepEquals, values[index])
		index++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(index, Equals, 3)

	index = 1
	err = store.Scan(binlogNamespace, keys[1], func(key []byte, val []byte) (bool, error) {

		c.Assert(key, DeepEquals, keys[index])
		c.Assert(val, DeepEquals, values[index])
		index++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(index, Equals, 4)

	err = store.Scan(binlogNamespace, keys[1], func(key []byte, val []byte) (bool, error) {
		return false, errors.NotFoundf("test err %s", "test")
	})
	if !errors.IsNotFound(err) {
		c.Fatalf("err should be not found err %v", err)
	}
}

func testBatch(c *C, store *BoltStore) {
	b := NewBatch()
	for i := 0; i < len(keys); i++ {
		b.Put(keys[i], batchValues[i])
	}
	c.Assert(b.Len(), Equals, len(keys))
	err := store.Commit(binlogNamespace, b)
	c.Assert(err, IsNil)

	batchData := []byte("test")
	for i := range keys {
		val, err := store.Get(binlogNamespace, keys[i])
		c.Assert(err, IsNil)
		c.Assert(val, DeepEquals, batchData)
	}
}
