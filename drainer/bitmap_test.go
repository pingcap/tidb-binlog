package drainer

import (
	. "github.com/pingcap/check"
)

func (t *testDrainerSuite) TestBitmap(c *C) {
	bm := newBitmap(0)
	c.Assert(bm, IsNil)

	bm = newBitmap(1)
	c.Assert(bm.value, HasLen, 1)
	c.Assert(bm.value, DeepEquals, []uint8{0xFE})
	testSetBitMap(c, bm, 1)

	bm = newBitmap(8)
	c.Assert(bm.value, HasLen, 1)
	c.Assert(bm.value, DeepEquals, []uint8{0x0})
	testSetBitMap(c, bm, 8)

	bm = newBitmap(9)
	c.Assert(bm.value, HasLen, 2)
	c.Assert(bm.value, DeepEquals, []uint8{0x0, 0xFE})
	testSetBitMap(c, bm, 9)

	bm = newBitmap(10)
	c.Assert(bm.value, HasLen, 2)
	c.Assert(bm.value, DeepEquals, []uint8{0x0, 0xFC})
	testSetBitMap(c, bm, 10)

	bm = newBitmap(16)
	c.Assert(bm.value, HasLen, 2)
	c.Assert(bm.value, DeepEquals, []uint8{0x0, 0x00})
	testSetBitMap(c, bm, 16)
}

func testSetBitMap(c *C, bm *bitmap, total int) {
	for i := 0; i < total; i++ {
		c.Assert(bm.completed(), IsFalse)
		bm.set(i)
		c.Assert(bm.current, Equals, i+1)
	}

	c.Assert(bm.completed(), IsTrue)

	for i := 0; i < total; i++ {
		bm.set(i)
		c.Assert(bm.completed(), IsTrue)
	}

}
