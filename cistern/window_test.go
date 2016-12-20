package cistern

import (
	"os"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/util/codec"
)

func (t *testCisternSuite) TestWindow(c *C) {
	// test window
	s, err := store.NewBoltStore("./window.test", [][]byte{windowNamespace})
	c.Assert(err, IsNil)
	defer func() {
		s.Close()
		os.Remove("./window.test")
	}()

	w, err := NewDepositWindow(s)
	c.Assert(err, IsNil)
	c.Assert(w.LoadLower(), Equals, int64(0))
	c.Assert(w.LoadUpper(), Equals, int64(0))
	w.SaveLower(3)
	w.SaveUpper(3)
	c.Assert(w.LoadLower(), Equals, int64(3))
	c.Assert(w.LoadLower(), Equals, int64(3))
	// test persistlower
	err = w.PersistLower(4)
	c.Assert(err, IsNil)
	c.Assert(w.LoadLower(), Equals, int64(4))
	// test loadMark
	_, _, err = loadMark(s)
	c.Assert(err, NotNil)
	// test endKey = 0
	s.Close()
	s, err = store.NewBoltStore("./window.test", [][]byte{windowNamespace, binlogNamespace})
	l, u, err := loadMark(s)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(4))
	c.Assert(u, Equals, int64(0))
	// test upper
	data := codec.EncodeInt([]byte{}, 5)
	err = s.Put(binlogNamespace, data, []byte("xixi"))
	c.Assert(err, IsNil)
	_, u, err = loadMark(s)
	c.Assert(err, IsNil)
	c.Assert(u, Equals, int64(5))
}
