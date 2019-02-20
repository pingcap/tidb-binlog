package bitmap

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBitmap{})

type testBitmap struct {
}

func (t *testBitmap) TestBitmap(c *C) {
	bm := NewBitmap(0)
	c.Assert(bm, IsNil)

	bm = NewBitmap(1)
	c.Assert(bm.Value, HasLen, 1)
	c.Assert(bm.Value, DeepEquals, []uint8{0xFE})
	testSetBitMap(c, bm, 1)

	bm = NewBitmap(8)
	c.Assert(bm.Value, HasLen, 1)
	c.Assert(bm.Value, DeepEquals, []uint8{0x0})
	testSetBitMap(c, bm, 8)

	bm = NewBitmap(9)
	c.Assert(bm.Value, HasLen, 2)
	c.Assert(bm.Value, DeepEquals, []uint8{0x0, 0xFE})
	testSetBitMap(c, bm, 9)

	bm = NewBitmap(10)
	c.Assert(bm.Value, HasLen, 2)
	c.Assert(bm.Value, DeepEquals, []uint8{0x0, 0xFC})
	testSetBitMap(c, bm, 10)

	bm = NewBitmap(16)
	c.Assert(bm.Value, HasLen, 2)
	c.Assert(bm.Value, DeepEquals, []uint8{0x0, 0x00})
	testSetBitMap(c, bm, 16)
}

func testSetBitMap(c *C, bm *Bitmap, total int) {
	for i := 0; i < total; i++ {
		c.Assert(bm.Completed(), IsFalse)
		c.Assert(bm.Set(i), IsTrue)
		c.Assert(bm.Current, Equals, i+1)
	}

	c.Assert(bm.Completed(), IsTrue)

	for i := 0; i < total; i++ {
		c.Assert(bm.Set(i), IsFalse)
		c.Assert(bm.Completed(), IsTrue)
	}

}
