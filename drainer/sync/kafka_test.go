package sync

import (
	"github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

var _ = check.Suite(&kafkaSuite{})

type kafkaSuite struct {
}

func newMockItem(ts int64) *Item {
	return &Item{Binlog: &pb.Binlog{CommitTs: ts}}
}

func (s kafkaSuite) TestAckWindow(c *check.C) {
	win := newAckWindow()
	_, ok := win.getReadyItem()
	c.Assert(ok, check.IsFalse)

	win.appendTS(1, 101)
	win.appendTS(3, 102)
	win.appendTS(7, 103)
	_, ok = win.getReadyItem()
	c.Assert(ok, check.IsFalse)
	c.Assert(win.unackedCount, check.Equals, 3)
	c.Assert(win.unackedSize, check.Equals, 306)

	win.handleSuccess(newMockItem(3))
	_, ok = win.getReadyItem()
	c.Assert(ok, check.IsFalse)
	c.Assert(win.unackedCount, check.Equals, 2)
	c.Assert(win.unackedSize, check.Equals, 204)

	win.handleSuccess(newMockItem(7))
	_, ok = win.getReadyItem()
	c.Assert(ok, check.IsFalse)
	c.Assert(win.unackedCount, check.Equals, 1)
	c.Assert(win.unackedSize, check.Equals, 101)

	win.handleSuccess(newMockItem(1))
	c.Assert(win.unackedCount, check.Equals, 0)
	c.Assert(win.unackedSize, check.Equals, 0)
	item, ok := win.getReadyItem()
	c.Assert(ok, check.IsTrue)
	c.Assert(item.Binlog.GetCommitTs(), check.Equals, int64(1))
	item, ok = win.getReadyItem()
	c.Assert(ok, check.IsTrue)
	c.Assert(item.Binlog.GetCommitTs(), check.Equals, int64(3))
	item, ok = win.getReadyItem()
	c.Assert(ok, check.IsTrue)
	c.Assert(item.Binlog.GetCommitTs(), check.Equals, int64(7))
}
