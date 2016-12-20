package cistern

import (
	"os"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/store"
	"github.com/pingcap/tidb/util/codec"
	"golang.org/x/net/context"
)

func (t *testCisternSuite) TestPublish(c *C) {
	// test window
	s, err := store.NewBoltStore("./publish.test", [][]byte{windowNamespace, binlogNamespace})
	c.Assert(err, IsNil)
	defer func() {
		s.Close()
		os.Remove("./publish.test")
	}()

	w, err := NewDepositWindow(s)
	c.Assert(err, IsNil)

	cfg := &Config{
		DepositWindowPeriod: 0,
	}
	p := NewPublisher(cfg, s, w)
	p.interval = 0
	ctx, cancle := context.WithCancel(context.Background())
	go p.Start(ctx)
	key := codec.EncodeInt([]byte{}, 5)
	data, err := encodePayload([]byte("ixix"))
	c.Assert(err, IsNil)
	err = s.Put(binlogNamespace, key, data)
	c.Assert(err, IsNil)
	cancle()
	time.Sleep(1 * time.Second)
	c.Assert(w.LoadLower(), Equals, int64(5))
}
