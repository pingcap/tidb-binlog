package flash_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/flash"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testFlashSuite{})

type testFlashSuite struct {
	cp *flash.MetaCheckpoint
}

func (s *testFlashSuite) SetUpTest(c *C) {
	s.cp = new(flash.MetaCheckpoint)
	s.cp.PushPendingCP(10)
	s.cp.PushPendingCP(20)
	s.cp.PushPendingCP(30)
	s.cp.PushPendingCP(40)
	s.cp.PushPendingCP(50)
}

func (s *testFlashSuite) TestPopNoFlush(c *C) {
	// Not calling flash = nothing is safe.
	forceSave, ok, _ := s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsFalse)
}

func (s *testFlashSuite) TestFlushNoSave(c *C) {
	// everything before ts=25 are safe.
	s.cp.Flush(25, false)

	forceSave, ok, safeTS := s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsTrue)
	c.Assert(safeTS, Equals, int64(20))

	// save CP has been cleared above.
	forceSave, ok, safeTS = s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsFalse)

	// increase flush TS
	s.cp.Flush(55, false)

	forceSave, ok, safeTS = s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsTrue)
	c.Assert(safeTS, Equals, int64(50))

	// no more checkpoints beyond ts=50
	s.cp.Flush(65, false)
	forceSave, ok, safeTS = s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsFalse)

	// add a few more and see
	s.cp.PushPendingCP(70)
	s.cp.PushPendingCP(80)

	s.cp.Flush(75, false)
	forceSave, ok, safeTS = s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsTrue)
	c.Assert(safeTS, Equals, int64(70))

	s.cp.Flush(76, false)
	forceSave, ok, safeTS = s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsFalse)

	s.cp.PushPendingCP(90)

	s.cp.Flush(95, false)
	forceSave, ok, safeTS = s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsTrue)
	c.Assert(safeTS, Equals, int64(90))
}

func (s *testFlashSuite) TestFlushForceSave(c *C) {
	// flush with force-save will make PopSafeCP return the force-save flag without safeTS
	s.cp.Flush(25, true)

	forceSave, ok, _ := s.cp.PopSafeCP()
	c.Assert(forceSave, IsTrue)
	c.Assert(ok, IsFalse)

	// the behavior after a single pop is the same as non-force-save
	forceSave, ok, _ = s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsFalse)

	// force-save will clear all pending TS
	s.cp.Flush(26, false)

	forceSave, ok, _ = s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsFalse)

	// After a force-save, all future flush before pop is no-op.
	s.cp.Flush(35, true)
	s.cp.Flush(36, true)
	s.cp.Flush(37, false)

	forceSave, ok, _ = s.cp.PopSafeCP()
	c.Assert(forceSave, IsTrue)
	c.Assert(ok, IsFalse)

	forceSave, ok, _ = s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsFalse)
}

func (s *testFlashSuite) TestPushAfterFlush(c *C) {
	s.cp.Flush(25, false)
	s.cp.PushPendingCP(24)

	forceSave, ok, safeTS := s.cp.PopSafeCP()
	c.Assert(forceSave, IsFalse)
	c.Assert(ok, IsTrue)
	c.Assert(safeTS, Equals, int64(20))
}

func (s *testFlashSuite) TestGetInstanceIsSingleton(c *C) {
	inst1 := flash.GetInstance()
	inst2 := flash.GetInstance()

	c.Assert(inst1, Equals, inst2)
	c.Assert(inst1, Not(Equals), &s.cp)
}
