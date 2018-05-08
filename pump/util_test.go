package pump

import (
	"io"
	"io/ioutil"
	"os"

	. "github.com/pingcap/check"
)

func (t *testPumpServerSuite) TestSeekOffset(c *C) {
	f, err := ioutil.TempFile(os.TempDir(), "testOffset")
	c.Assert(err, IsNil)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	content := "test offset"
	_, err = f.Write([]byte(content))
	c.Assert(err, IsNil)

	offsets := []int64{0, 5, 10}
	for _, offset := range offsets {
		err = seekOffset(f, offset)
		c.Assert(err, IsNil)

		b := make([]byte, 1)
		_, err = io.ReadFull(f, b)
		c.Assert(err, IsNil)
		c.Assert(b[0], Equals, content[offset])
	}
}
