package file

import (
	"io/ioutil"
	"os"
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testLockSuite{})

type testLockSuite struct{}

func (s *testLockSuite) TestLockAndUnlock(c *C) {
	f, err := ioutil.TempFile("", "lock")
	c.Assert(err, IsNil)
	f.Close()
	defer func() {
		err = os.Remove(f.Name())
		c.Assert(err, IsNil)
	}()

	// lock the file
	l, err := LockFile(f.Name(), os.O_WRONLY, PrivateFileMode)
	c.Assert(err, IsNil)

	// try lock a locked file
	if _, err = TryLockFile(f.Name(), os.O_WRONLY, PrivateFileMode); err != ErrLocked {
		c.Fatal(err)
	}

	// unlock the file
	err = l.Close()
	c.Assert(err, IsNil)

	// try lock the unlocked file
	dupl, err := TryLockFile(f.Name(), os.O_WRONLY, PrivateFileMode)
	c.Assert(err, IsNil)

	// blocking on locked file
	locked := make(chan struct{}, 1)
	go func() {
		bl, blerr := LockFile(f.Name(), os.O_WRONLY, PrivateFileMode)
		c.Assert(blerr, IsNil)

		locked <- struct{}{}
		blerr = bl.Close()
		c.Assert(blerr, IsNil)
	}()

	select {
	case <-locked:
		c.Error("unexpected unblocking")
	case <-time.After(100 * time.Millisecond):
	}

	// unlock
	err = dupl.Close()
	c.Assert(err, IsNil)

	// the previously blocked routine should be unblocked
	select {
	case <-locked:
	case <-time.After(1 * time.Second):
		c.Error("unexpected blocking")
	}
}
